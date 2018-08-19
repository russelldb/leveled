%% -------------------------------------------------------------------
%%
%% leveld_eqc: basic statem for doing things to leveled
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(leveled_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("../include/leveled.hrl").

-compile(nowarn_export_all).
-compile(export_all).

-define(DATA_DIR, "./leveled_eqc").
                 
-record(state, {leveled = undefined ::  undefined | pid(),
                model :: orddict:orddict(),  %% The DB state on disk
                previous_keys = [] :: list(binary()),   %% Used to increase probability to pick same key
                deleted_keys = [] :: list(binary()),
                start_opts = [] :: [tuple()],
                folders = [] :: [map()],
                used_folders = [] :: [map()],
                stop_folders = [] :: [map()],
                old_folders = [],
                counter = 0 :: integer()
               }).

-define(NUMTESTS, 1000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

-type state() :: #state{}.

eqc_test_() ->
    Timeout = 50,
    {timeout, 2 * Timeout, ?_assertEqual(true, eqc:quickcheck(eqc:testing_time(Timeout, ?QC_OUT(prop_db()))))}.

run() ->
    run(?NUMTESTS).

run(Count) ->
    eqc:quickcheck(eqc:numtests(Count, prop_db())).

check() ->
    eqc:check(prop_db()).

iff(B1, B2) -> B1 == B2.
implies(B1, B2) -> (not B1 orelse B2).

initial_state() ->
    #state{model = orddict:new()}.

%% --- Operation: init_backend ---
%% @doc init_backend_pre/1 - Precondition for generation
-spec init_backend_pre(S :: eqc_statem:symbolic_state()) -> boolean().
init_backend_pre(S) ->
    not is_leveled_open(S).

%% @doc init_backend_args - Argument generator
-spec init_backend_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
init_backend_args(_S) ->
    [gen_opts()].

%% @doc init_backend - The actual operation
%% Start the database and read data from disk
init_backend(Options) ->
    case leveled_bookie:book_start(Options) of
        {ok, Bookie} when is_pid(Bookie) ->
            unlink(Bookie),
            erlang:register(sut, Bookie),
            Bookie;
        Error -> Error
    end.

%% @doc init_backend_next - Next state function
-spec init_backend_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
init_backend_next(S, LevelEdPid, [Options]) ->
    S#state{leveled=LevelEdPid, start_opts = Options}.

%% @doc init_backend_post - Postcondition for init_backend
-spec init_backend_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
init_backend_post(_S, [_Options], LevelEdPid) ->
    is_pid(LevelEdPid).

%% --- Operation: stop ---
%% @doc stop_pre/1 - Precondition for generation
-spec stop_pre(S :: eqc_statem:symbolic_state()) -> boolean().
stop_pre(S) ->
    is_leveled_open(S).

%% @doc stop_args - Argument generator
-spec stop_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
stop_args(#state{leveled=Pid}) ->
    [Pid].

stop_pre(S, [Pid]) ->
    Pid == S#state.leveled.

stop_adapt(S, [_]) ->
    [S#state.leveled].

%% @doc stop - The actual operation
%% Stop the server, but the values are still on disk
stop(Pid) ->
    ok = leveled_bookie:book_close(Pid).

%% @doc stop_next - Next state function
-spec stop_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
stop_next(S, _Value, [_Pid]) ->
    S#state{leveled = undefined,
            folders = [],
            stop_folders = S#state.folders,
            old_folders = S#state.old_folders ++ S#state.stop_folders}.  

%% @doc stop_post - Postcondition for stop
-spec stop_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
stop_post(_S, [Pid], _Res) ->
    Mon = erlang:monitor(process, Pid),
    receive
        {'DOWN', Mon, _Type, Pid, _Info} ->
            true
    after 5000 ->
            {still_a_pid, Pid}
    end.


%% --- Operation: put ---
%% @doc put_pre/1 - Precondition for generation
-spec put_pre(S :: eqc_statem:symbolic_state()) -> boolean().
put_pre(S) ->
    is_leveled_open(S).

%% @doc put_args - Argument generator
-spec put_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
put_args(#state{leveled=Pid, previous_keys=PK}) ->
    [Pid, gen_key(PK), gen_val()].

%% @doc put - The actual operation
put(Pid, Key, Value) ->
    ok = leveled_bookie:book_put(Pid, Key, Key, Value, []).

put_pre(S, [Pid, _Key, _Value]) ->
    Pid == S#state.leveled.

put_adapt(S, [_, Key, Value]) ->
    [ S#state.leveled, Key, Value ].

%% @doc put_next - Next state function
-spec put_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
put_next(S, _Value, [_Pid, Key, Value]) ->
    #state{model=Model, previous_keys=PK} = S,
    Model2 = orddict:store(Key, Value, Model),
    S#state{model=Model2, previous_keys=[Key | PK]}.

put_post(_, [_, _, _], Res) ->
    eq(Res, ok).

%% @doc put_features - Collects a list of features of this call with these arguments.
-spec put_features(S, Args, Res) -> list(any())
    when S    :: eqc_statem:dynmic_state(),
         Args :: [term()],
         Res  :: term().
put_features(#state{previous_keys=PK}, [_Pid, Key, _Value], _Res) ->
    case lists:member(Key, PK) of
        true ->
            [{put, update}];
        false ->
            [{put, insert}]
    end.

%% --- Operation: get ---
%% @doc get_pre/1 - Precondition for generation
-spec get_pre(S :: eqc_statem:symbolic_state()) -> boolean().
get_pre(S) ->
    is_leveled_open(S).

%% @doc get_args - Argument generator
-spec get_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
get_args(#state{leveled=Pid, previous_keys=PK}) ->
    [Pid, gen_key(PK)].

%% @doc get - The actual operation
get(Pid, Key) ->
    leveled_bookie:book_get(Pid, Key, Key).

get_pre(S, [Pid, _Key]) ->
    Pid == S#state.leveled.

get_adapt(S, [_, Key]) ->
    [S#state.leveled, Key].

%% @doc get_post - Postcondition for get
-spec get_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
get_post(S, [_Pid, Key], Res) ->
    #state{model=Model} = S,
    case orddict:find(Key, Model) of
        {ok, V} ->
            Res == {ok, V};
        error ->
            Res == not_found
    end.

%% @doc get_features - Collects a list of features of this call with these arguments.
-spec get_features(S, Args, Res) -> list(any())
    when S    :: eqc_statem:dynmic_state(),
         Args :: [term()],
         Res  :: term().
get_features(S, [_Pid, Key], Res) ->
    case Res of
        not_found ->
            [{get, not_found, deleted} || lists:member(Key, S#state.deleted_keys)] ++ 
          [{get, not_found, not_inserted} || not lists:member(Key, S#state.previous_keys)];
        {ok, B} when is_binary(B) ->
            [{get, found}]
    end.

%% --- Operation: delete ---
%% @doc delete_pre/1 - Precondition for generation
-spec delete_pre(S :: eqc_statem:symbolic_state()) -> boolean().
delete_pre(S) ->
    is_leveled_open(S).

%% @doc delete_args - Argument generator
-spec delete_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
delete_args(#state{leveled=Pid, previous_keys=PK}) ->
    [Pid, gen_key(PK)].

delete_pre(S, [Pid, _Key]) ->
    Pid == S#state.leveled.

delete_adapt(S, [_, Key]) ->
    [ S#state.leveled, Key ].

%% @doc delete - The actual operation
delete(Pid, Key) ->
    ok = leveled_bookie:book_delete(Pid, Key, Key, []).

%% @doc delete_next - Next state function
-spec delete_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
delete_next(S, _Value, [_Pid, Key]) ->
    #state{model=Model, deleted_keys=DK} = S,
    Model2 = orddict:erase(Key, Model),
    S#state{model=Model2, deleted_keys = case orddict:is_key(Key, Model) of
                                             true -> [Key | DK];
                                             false -> DK
                                         end}.

%% @doc delete_features - Collects a list of features of this call with these arguments.
-spec delete_features(S, Args, Res) -> list(any())
    when S    :: eqc_statem:dynmic_state(),
         Args :: [term()],
         Res  :: term().
delete_features(#state{previous_keys=PK}, [_Pid, Key], _Res) ->
    case lists:member(Key, PK) of
        true ->
            [{delete, written}];
        false ->
            [{delete, not_written}]
    end.

%% --- Operation: is_empty ---
%% @doc is_empty_pre/1 - Precondition for generation
-spec is_empty_pre(S :: eqc_statem:symbolic_state()) -> boolean().
is_empty_pre(S) ->
    is_leveled_open(S).

%% @doc is_empty_args - Argument generator
-spec is_empty_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
is_empty_args(#state{leveled=Pid}) ->
    [Pid].

%% @doc is_empty - The actual operation
is_empty(Pid) ->
    FoldBucketsFun = fun(B, Acc) -> sets:add_element(B, Acc) end,
    ListBucketQ = {binary_bucketlist,
                    ?STD_TAG,
                    {FoldBucketsFun, sets:new()}},
    {async, Folder} = leveled_bookie:book_returnfolder(Pid, ListBucketQ),
    BSet = Folder(),
    sets:size(BSet) == 0.

is_empty_pre(S, [Pid]) ->
    Pid == S#state.leveled.

is_empty_adapt(S, [_]) ->
    [S#state.leveled].

%% @doc is_empty_post - Postcondition for is_empty
-spec is_empty_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
is_empty_post(#state{model=Model}, [_Pid], Res) ->
    Size = orddict:size(Model),
    case Res of
      true -> eq(0, Size);
      false when Size == 0 -> expected_empty;
      false when Size > 0  -> true
    end.

%% @doc is_empty_features - Collects a list of features of this call with these arguments.
-spec is_empty_features(S, Args, Res) -> list(any())
    when S    :: eqc_statem:dynmic_state(),
         Args :: [term()],
         Res  :: term().
is_empty_features(_S, [_Pid], Res) ->
    [{empty, Res}].

%% --- Operation: drop ---
%% @doc drop_pre/1 - Precondition for generation
-spec drop_pre(S :: eqc_statem:symbolic_state()) -> boolean().
drop_pre(S) ->
    is_leveled_open(S).

%% @doc drop_args - Argument generator
-spec drop_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
drop_args(#state{leveled=Pid}) ->
    [Pid, gen_opts()].

drop_pre(S, [Pid, _Opts]) ->
    Pid == S#state.leveled.

drop_adapt(S, [_Pid, _]) ->
    [S#state.leveled, S#state.start_opts].
    
%% @doc drop - The actual operation
%% Remove fles from disk (directory structure may remain) and start a new clean database
drop(Pid, Opts) ->
    Mon = erlang:monitor(process, Pid),
    ok = leveled_bookie:book_destroy(Pid),
    receive
        {'DOWN', Mon, _Type, Pid, _Info} ->
            init_backend(Opts)
    after 5000 ->
            {still_a_pid, Pid}
    end.

%% @doc drop_next - Next state function
-spec drop_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
drop_next(S, Value, [Pid, Opts]) ->
    S1 = stop_next(S, Value, [Pid]),
    init_backend_next(S1#state{model = orddict:new()}, 
                      Value, [Opts]).

%% @doc drop_post - Postcondition for drop
-spec drop_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
drop_post(_S, [_Pid, _Opts], NewPid) ->
    case is_pid(NewPid) of
        true  -> true;
        false -> NewPid
    end.

drop_features(S, [_Pid, _Opts], _Res) ->
    Size = orddict:size(S#state.model),
    [{drop, empty} || Size == 0 ] ++ [{drop, Size div 10} || Size > 0 ].



%% --- Operation: kill ---
kill_pre(S) ->
    is_leveled_open(S).

kill_args(S) ->
    [S#state.leveled].

kill_pre(S, [Pid]) ->
    Pid == S#state.leveled.

kill_adapt(S, [_]) ->
    [ S#state.leveled ].

kill(Pid) ->
    exit(Pid, kill),
    timer:sleep(1).

kill_next(S, Value, [Pid]) ->
    stop_next(S, Value, [Pid]).

    


%% Testing fold:
%% Note async and sync mode!
%% see https://github.com/martinsumner/riak_kv/blob/mas-2.2.5-tictactaae/src/riak_kv_leveled_backend.erl#L238-L419

%% --- Operation: create folding ---
fold_create_pre(S) ->
    is_leveled_open(S).

fold_create_args(#state{leveled=Pid, start_opts = Opts} = S) ->
    ?LET(FoldFun, oneof([fold_collect, fold_count, fold_keys]),
         [Pid, {keylist, tag_gen(Opts), {call, ?MODULE, FoldFun, []}}, FoldFun, 
          S#state.counter  %% add a unique counter
         ]).

fold_create_pre(S, [Pid, {FoldType, _Tag, _}, FoldFun, _Counter]) ->
    %% Make sure we operate on an existing Pid when shrinking
    %% Check start options validity as well?
    Pid == S#state.leveled
      andalso implies(FoldType == keylist, FoldFun =/= fold_collect).  %% fold_collect results in {Key, Key}
    
fold_create_adapt(S, [_, FoldType, FoldFun, Counter]) ->
    %% Keep the counter!
    [S#state.leveled, FoldType, FoldFun, Counter].

fold_create(Pid, FoldType, _, _) ->
    {async, Folder} = leveled_bookie:book_returnfolder(Pid, FoldType),
    Folder.

fold_create_next(S, SymFolder, [_, FoldType, FoldFun, Counter]) ->
    S#state{folders = S#state.folders ++ 
                [#{counter => Counter, 
                   folder => SymFolder, 
                   folder_type => FoldType, 
                   foldfun => FoldFun, 
                   snapshot => S#state.model}],
           counter = S#state.counter + 1}.

fold_create_post(_S, [_, _, _, _], Res) ->
    is_function(Res).


%% --- Operation: fold_run ---
fold_run_pre(S) ->
    S#state.folders /= [].

fold_run_args(#state{folders = Folders}) ->
    ?LET(#{counter := Counter, folder := Folder}, elements(Folders),
         [Counter, Folder]).

fold_run_pre(S, [Counter, _Folder]) ->
    %% Ensure membership even under shrinking
    %% Counter is fixed at first generation and does not shrink!
    get_foldobj(S#state.folders, Counter) =/= undefined.

fold_run(_, Folder) ->
    catch Folder().

fold_run_next(S, _Value, [Counter, _Folder]) ->
    %% leveled_runner comment: "Iterators should de-register themselves from the Penciller on completion."
    FoldObj = get_foldobj(S#state.folders, Counter),
    S#state{folders = S#state.folders -- [FoldObj],
            used_folders = S#state.used_folders ++ [FoldObj]}.
    
fold_run_post(S, [Count, _], Res) ->
    #{snapshot := Snapshot, foldfun := FoldFun} = get_foldobj(S#state.folders, Count),
    {FF, Acc} = apply(?MODULE, FoldFun, []),
    case S#state.leveled of 
        undefined ->
            is_exit(Res);
        _ ->
            eq(Res, orddict:fold(FF, Acc, Snapshot))
    end.

fold_run_features(S, [Counter, _Folder], _) ->
    #{snapshot := Snapshot, foldfun := FoldFun} = get_foldobj(S#state.folders, Counter),
    [ case orddict:size(Snapshot) of
          0 -> {fold, FoldFun, empty};
          _ -> {fold, FoldFun, non_empty}
      end ].
               
%% --- Operation: fold_run on already used folder ---
%% A fold that has already ran to completion should results in an exception when re-used.
noreuse_fold_pre(S) ->
    S#state.used_folders /= [].

noreuse_fold_args(#state{used_folders = Folders}) ->
    ?LET(#{counter := Counter, folder := Folder}, elements(Folders),
         [Counter, Folder]).

noreuse_fold_pre(S, [Counter, _Folder]) ->
    %% Ensure membership even under shrinking
    %% Counter is fixed at first generation and does not shrink!
    get_foldobj(S#state.used_folders, Counter) =/= undefined.

noreuse_fold(_, Folder) ->
    catch Folder().

noreuse_fold_post(_S, [_, _], Res) ->
    is_exit(Res).

noreuse_fold_features(S, [_, _], _) ->
    [ case S#state.leveled of
          undefined -> 
              reuse_fold_when_closed;
          _ ->
              reuse_fold_when_open
      end ].


%% --- Operation: fold_run on folder that survived a crash ---
%% A fold that has already ran to completion should results in an exception when re-used.
stop_fold_pre(S) ->
    S#state.stop_folders /= [].

stop_fold_args(#state{stop_folders = Folders}) ->
    ?LET(#{counter := Counter, folder := Folder}, elements(Folders),
         [Counter, Folder]).

stop_fold_pre(S, [Counter, _Folder]) ->
    %% Ensure membership even under shrinking
    %% Counter is fixed at first generation and does not shrink!
    get_foldobj(S#state.stop_folders, Counter) =/= undefined.

stop_fold(_, Folder) ->
    catch Folder().

stop_fold_post(_S, [_Counter, _], Res) ->
    is_exit(Res).

stop_fold_next(S, _Value, [Counter, _]) ->
    %% leveled_runner comment: "Iterators should de-register themselves from the Penciller on completion."
    FoldObj = get_foldobj(S#state.stop_folders, Counter),
    S#state{stop_folders = S#state.stop_folders -- [FoldObj],
            used_folders = S#state.used_folders ++ [FoldObj]}.

stop_fold_features(S, [_, _], _) ->
    [ case S#state.leveled of
          undefined -> 
              stop_fold_when_closed;
          _ ->
              stop_fold_when_open
      end ].


weight(#state{previous_keys=[]}, Command) when Command == get;
                                               Command == delete ->
    1;
weight(_S, C) when C == get;
                   C == put ->
    10;
weight(_S, stop) ->
    1;
weight(_, _) ->
    1.


%% @doc check that the implementation of leveled is equivalent to a
%% sorted dict at least
-spec prop_db() -> eqc:property().
prop_db() ->
    Dir = "./leveled_data",
    ?FORALL({Kind, Cmds}, more_commands(20, oneof([{seq, commands(?MODULE)}, 
                                                   {par, parallel_commands(?MODULE)}])),
    begin
        delete_level_data(Dir),
        ?IMPLIES(empty_dir(Dir),
        begin
            Procs = erlang:processes(),
            StartTime = erlang:system_time(millisecond),

            RunResult = execute(Kind, Cmds),
            %% Do not extract the 'state' from this tuple, since parallel commands
            %% miss the notion of final state.
            CallFeatures = call_features(history(RunResult)),

            case whereis(sut) of
                undefined -> delete_level_data(Dir);
                Pid when is_pid(Pid) ->
                    leveled_bookie:book_destroy(Pid)
            end,

            Wait = wait_for_procs(Procs, 500),
            RunTime = erlang:system_time(millisecond) - StartTime,

            pretty_commands(?MODULE, Cmds, RunResult,
            measure(time_per_test, RunTime,
            aggregate(command_names(Cmds),
            collect(Kind,
            aggregate(with_title('Features'), CallFeatures,
                      features(CallFeatures,
                               conjunction([{result, result(RunResult) == ok},
                                            {data_cleanup, 
                                             ?WHENFAIL(eqc:format("~s\n", [os:cmd("ls -Rl ./leveled_data")]),
                                                       empty_dir(Dir))},
                                            {pid_cleanup, equals(Wait, [])}])))))))

        end)
    end).

history({H, _, _}) -> H.
result({_, _, Res}) -> Res.

execute(seq, Cmds) ->
    run_commands(Cmds);
execute(par, Cmds) ->
    run_parallel_commands(Cmds).

is_exit({'EXIT', _}) ->
    true;
is_exit(Other) ->
    {expected_exit, Other}.


gen_opts() ->
    [{root_path, "./leveled_data"}].

gen_key() ->
    binary(16).

gen_val() ->
    noshrink(binary(32)).

gen_key([]) ->
    gen_key();
gen_key(Previous) ->
    frequency([{1, gen_key()},
               {2, elements(Previous)}]).

tag_gen(_StartOptions) ->
  oneof([?STD_TAG]). %%, ?IDX_TAG, ?HEAD_TAG]).


fold_collect() ->
  {fun(X, Y, Z) -> [{X, Y} | Z] end, []}.

%% This makes system fall over
fold_collect_no_acc() ->
  fun(X, Y, Z) -> [{X, Y} | Z] end.

fold_count() ->
  {fun(_X, _Y, Z) -> Z + 1 end, 0}.

fold_keys() ->
  {fun(X, _Y, Z) -> [X | Z] end, []}.


empty_dir(Dir) ->
    case file:list_dir(Dir) of
        {error, enoent} -> true;
        {ok, Ds} ->
            lists:all(fun(D) -> empty_dir(filename:join(Dir, D)) end, Ds);
        _ ->
            false
    end.

get_foldobj([], _Counter) ->
    undefined;
get_foldobj([#{counter := Counter} = Map | _Rest], Counter) ->
    Map;
get_foldobj([_ | Rest], Counter) ->
    get_foldobj(Rest, Counter).
                

%% Helper for all those preconditions that just check that leveled Pid
%% is populated in state.
-spec is_leveled_open(state()) -> boolean().
is_leveled_open(#state{leveled=undefined}) ->
    false;
is_leveled_open(_) ->
    true.

wait_for_procs(Known, Timeout) ->
    case erlang:processes() -- Known of
        [] -> [];
        Running ->
            case Timeout > 0 of
                true ->
                    timer:sleep(100),
                    wait_for_procs(Known, Timeout - 100);
                false ->
                    Running
            end
    end.

delete_level_data(Dir) ->
    os:cmd("rm -rf " ++ Dir).
