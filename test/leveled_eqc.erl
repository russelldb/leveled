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

-compile([export_all, nowarn_export_all]).

-define(NUMTESTS, 1000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

-define(CMD_VALID(State, Cmd, True, False),
        case is_valid_cmd(State, Cmd) of
            true -> True;
            false -> False
        end).
                       

-type state() :: map().

eqc_test_() ->
    Timeout = 50,
    {timeout, max(2 * Timeout, Timeout + 10),
     ?_assertEqual(true, eqc:quickcheck(eqc:testing_time(Timeout, ?QC_OUT(prop_db()))))}.

run() ->
    run(?NUMTESTS).

run(Count) ->
    eqc:quickcheck(eqc:numtests(Count, prop_db())).

check() ->
    eqc:check(prop_db()).

iff(B1, B2) -> B1 == B2.
implies(B1, B2) -> (not B1 orelse B2).

%% start_opts should not be added to this map, it is added only when the system is started the first time.
initial_state() ->
    #{dir => {var, dir},
      leveled => undefined,   %% to make adapt happy after failing pre/1
      counter => 0,
      model => orddict:new(),
      previous_keys => [],
      deleted_keys => [],
      folders => []
     }.

%% --- Operation: init_backend ---
%% @doc init_backend_pre/1 - Precondition for generation
-spec init_backend_pre(S :: eqc_statem:symbolic_state()) -> boolean().
init_backend_pre(S) ->
    not is_leveled_open(S).

%% @doc init_backend_args - Argument generator
-spec init_backend_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
init_backend_args(#{dir := Dir} = S) ->
    case maps:get(start_opts, S, undefined) of
        undefined ->
            [[{root_path, Dir} | gen_opts()]];
        Opts ->
            %% root_path is part of existing options
            [Opts]
    end.

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
    S#{leveled => LevelEdPid, start_opts => Options}.

%% @doc init_backend_post - Postcondition for init_backend
-spec init_backend_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
init_backend_post(_S, [_Options], LevelEdPid) ->
    is_pid(LevelEdPid).

init_backend_features(_S, [Options], _Res) ->
    Options.


%% --- Operation: stop ---
%% @doc stop_pre/1 - Precondition for generation
-spec stop_pre(S :: eqc_statem:symbolic_state()) -> boolean().
stop_pre(S) ->
    is_leveled_open(S).

%% @doc stop_args - Argument generator
-spec stop_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
stop_args(#{leveled := Pid}) ->
    [Pid].

stop_pre(#{leveled := Leveled}, [Pid]) ->
    %% check during shrinking
    Pid == Leveled.

stop_adapt(#{leveled := Leveled}, [_]) ->
    [Leveled].

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
    S#{leveled => undefined,
       folders => [],
       stop_folders => maps:get(folders, S, [])}.  

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
put_args(#{leveled := Pid, previous_keys := PK}) ->
    ?LET({Key, Bucket}, gen_key_in_bucket(PK),
         [Pid, Bucket, Key, gen_val()]).

put_pre(#{leveled := Leveled}, [Pid, _Bucket, _Key, _Value]) ->
    Pid == Leveled.

put_adapt(#{leveled := Leveled}, [_, Bucket, Key, Value]) ->
    [ Leveled, Bucket, Key, Value ].

%% @doc put - The actual operation
put(Pid, Bucket, Key, Value) ->
    leveled_bookie:book_put(Pid, Bucket, Key, Value, []).

%% @doc put_next - Next state function
-spec put_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
put_next(#{model := Model, previous_keys := PK} = S, _Value, [_Pid, Bucket, Key, Value]) ->
    ?CMD_VALID(S, put,
               S#{model => orddict:store({Bucket, Key}, Value, Model),
                  previous_keys => PK ++ [{Key, Bucket}]},
               S).

put_post(S, [_, _, _, _], Res) ->
    ?CMD_VALID(S, put, eq(Res, ok), eq(Res, {unsupported_message, put})).

%% @doc put_features - Collects a list of features of this call with these arguments.
-spec put_features(S, Args, Res) -> list(any())
    when S    :: eqc_statem:dynmic_state(),
         Args :: [term()],
         Res  :: term().
put_features(#{previous_keys := PK} = S, [_Pid, Bucket, Key, _Value], _Res) ->
    ?CMD_VALID(S, put,
               case 
                   lists:member({Key, Bucket}, PK) of
                   true ->
                       [{put, update}];
                   false ->
                       [{put, insert}]
               end,
               [{put, unsupported}]).

%% --- Operation: get ---
%% @doc get_pre/1 - Precondition for generation
-spec get_pre(S :: eqc_statem:symbolic_state()) -> boolean().
get_pre(S) ->
    is_leveled_open(S).

%% @doc get_args - Argument generator
-spec get_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
get_args(#{leveled := Pid, previous_keys := PK, start_opts := Opts}) ->
    ?LET({Key, Bucket}, gen_key_in_bucket(PK),
         [Pid, Bucket, Key, default(none, gen_tag(Opts))]).

%% @doc get - The actual operation
get(Pid, Bucket, Key, none) ->
    leveled_bookie:book_get(Pid, Bucket, Key);
get(Pid, Bucket, Key, Tag) ->
    leveled_bookie:book_get(Pid, Bucket, Key, Tag).

get_pre(#{leveled := Leveled}, [Pid, _Bucket, _Key, _Tag]) ->
    Pid == Leveled.

get_adapt(#{leveled := Leveled}, [_, Bucket, Key, Tag]) ->    
    [Leveled, Bucket, Key, Tag].

%% @doc get_post - Postcondition for get
-spec get_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
get_post(#{model := Model} = S, [_Pid, Bucket, Key, Tag], Res) ->
    ?CMD_VALID(S, get,
               case Res of
                   {ok, _} ->
                       eq(Res, orddict:find({Bucket, Key}, Model));
                   not_found ->
                       %% Weird to be able to supply a tag, but must be STD_TAG...
                       Tag =/= ?STD_TAG orelse orddict:find({Bucket, Key}, Model) == error
               end,
               eq(Res, {unsupported_message, get})).

%% @doc get_features - Collects a list of features of this call with these arguments.
-spec get_features(S, Args, Res) -> list(any())
    when S    :: eqc_statem:dynmic_state(),
         Args :: [term()],
         Res  :: term().
get_features(#{deleted_keys := DK, previous_keys := PK}, [_Pid, Bucket, Key, _Tag], Res) ->
    case Res of
        not_found ->
            [{get, not_found, deleted} || lists:member({Key, Bucket}, DK)] ++ 
          [{get, not_found, not_inserted} || not lists:member({Key, Bucket}, PK)];
        {ok, B} when is_binary(B) ->
            [{get, found}];
        {unsupported_message, _} ->
            [{get, unsupported}]
    end.

%% --- Operation: mput ---
-spec mput_pre(S :: eqc_statem:symbolic_state()) -> boolean().
mput_pre(S) ->
    is_leveled_open(S).

%% @doc put_args - Argument generator
%% Specification says: duplicated should be removed
%% "%% The list should be de-duplicated before it is passed to the bookie."
%% Wether this means that keys should be unique or even Action and values is unclear.
%% Slack discussion:
%% `[{add, B1, K1, SK1}, {add, B1, K1, SK2}]` should be fine (same bucket and key, different subkey)
%%
%% Really weird to have to specify a value in case of a remove action
mput_args(#{leveled := Pid, previous_keys := PK}) ->
    ?LET(Objs, list({gen_key_in_bucket(PK), nat()}),
         [Pid, [ {weighted_default({5, add}, {1, remove}), Bucket, Key, SubKey, gen_val()} || {{Key, Bucket}, SubKey} <- Objs ]]).


mput_pre(#{leveled := Leveled}, [Pid, ObjSpecs]) ->
    Pid == Leveled andalso no_key_dups(ObjSpecs) == ObjSpecs.

mput_adapt(#{leveled := Leveled}, [_, ObjSpecs]) ->
    [ Leveled, no_key_dups(ObjSpecs) ].

%% @doc put - The actual operation
mput(Pid, ObjSpecs) ->
    leveled_bookie:book_mput(Pid, ObjSpecs).

%% @doc put_next - Next state function
mput_next(S, _, [_Pid, ObjSpecs]) ->
    ?CMD_VALID(S, mput,
               lists:foldl(fun({add, Bucket, Key, _SubKey, Value}, #{model := Model, previous_keys := PK} = Acc) ->
                                   Acc#{model => orddict:store({Bucket, Key}, Value, Model),
                                        previous_keys => PK ++ [{Key, Bucket}]};
                              ({remove, Bucket, Key, _SubKey, _Value}, #{model := Model} = Acc) ->
                                   Acc#{model => orddict:erase({Bucket, Key}, Model)}
                           end, S, ObjSpecs),
               S).

mput_post(S, [_, _], Res) ->
    ?CMD_VALID(S, mput, eq(Res, ok), eq(Res, {unsupported_message, mput})).

mput_features(S, [_Pid, ObjSpecs], _Res) ->
    ?CMD_VALID(S, mput,
               {mput, [ element(1, ObjSpec) || ObjSpec <- ObjSpecs ]},
               [{mput, unsupported}]).


%% --- Operation: delete ---
%% @doc delete_pre/1 - Precondition for generation
-spec delete_pre(S :: eqc_statem:symbolic_state()) -> boolean().
delete_pre(S) ->
    is_leveled_open(S).

%% @doc delete_args - Argument generator
-spec delete_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
delete_args(#{leveled := Pid, previous_keys := PK}) ->
    ?LET({Key, Bucket}, gen_key_in_bucket(PK),
         [Pid, Bucket, Key]).

delete_pre(#{leveled := Leveled}, [Pid, _, _Key]) ->
    Pid == Leveled.

delete_adapt(#{leveled := Leveled}, [_, Bucket, Key]) ->
    [ Leveled, Bucket, Key ].

%% @doc delete - The actual operation
delete(Pid, Bucket, Key) ->
    leveled_bookie:book_delete(Pid, Bucket, Key, []).

%% @doc delete_next - Next state function
-spec delete_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
delete_next(#{model := Model, deleted_keys := DK} = S, _Value, [_Pid, Bucket, Key]) ->
    ?CMD_VALID(S, delete,
               S#{model => orddict:erase({Bucket, Key}, Model), 
                  deleted_keys => DK ++ [{Key, Bucket} || orddict:is_key({Key, Bucket}, Model)]},
               S).

delete_post(S, [_Pid, _Bucket, _Key], Res) ->
    ?CMD_VALID(S, delete,
               eq(Res, ok),
               case Res of
                   {unsupported_message, _} -> true;
                   _ -> Res
               end).

%% @doc delete_features - Collects a list of features of this call with these arguments.
-spec delete_features(S, Args, Res) -> list(any())
    when S    :: eqc_statem:dynmic_state(),
         Args :: [term()],
         Res  :: term().
delete_features(#{previous_keys := PK} = S, [_Pid, Bucket, Key], _Res) ->
    ?CMD_VALID(S, delete,
               case lists:member({Key, Bucket}, PK) of
                   true ->
                       [{delete, existing}];
                   false ->
                       [{delete, none_existing}]
               end,
               [{delete, unsupported}]).

%% --- Operation: is_empty ---
%% @doc is_empty_pre/1 - Precondition for generation
-spec is_empty_pre(S :: eqc_statem:symbolic_state()) -> boolean().
%% is_empty does not work when started in head_only mode! But it should.
is_empty_pre(S) ->
    is_leveled_open(S) andalso not in_head_only_mode(S).

%% @doc is_empty_args - Argument generator
-spec is_empty_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
is_empty_args(#{leveled := Pid, start_opts := Opts}) ->
    [Pid, gen_tag(Opts)].


is_empty_pre(#{leveled := Leveled}, [Pid, _]) ->
    Pid == Leveled.

is_empty_adapt(#{leveled := Leveled}, [_, Tag]) ->
    [Leveled, Tag].

%% @doc is_empty - The actual operation
is_empty(Pid, Tag) ->
    leveled_bookie:book_isempty(Pid, Tag).

%% @doc is_empty_post - Postcondition for is_empty
-spec is_empty_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
is_empty_post(#{model := Model, start_opts := Opts}, [_Pid, Tag], Res) ->
    case valid_tag(Tag, Opts) of
        false -> true;   %% this is a bit weird
        true ->
            Size = orddict:size(Model),
            case Res of
                true -> eq(0, Size);
                false when Size == 0 -> expected_empty;
                false when Size > 0  -> true
            end
    end.

%% @doc is_empty_features - Collects a list of features of this call with these arguments.
-spec is_empty_features(S, Args, Res) -> list(any())
    when S    :: eqc_statem:dynmic_state(),
         Args :: [term()],
         Res  :: term().
is_empty_features(_S, [_Pid, _], Res) ->
    [{empty, Res}].

%% --- Operation: drop ---
%% @doc drop_pre/1 - Precondition for generation
-spec drop_pre(S :: eqc_statem:symbolic_state()) -> boolean().
drop_pre(S) ->
    is_leveled_open(S).

%% @doc drop_args - Argument generator
%% Generate start options used when restarting
-spec drop_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
drop_args(#{leveled := Pid, dir := Dir}) ->
    [Pid, [{root_path, Dir} | gen_opts()]].

drop_pre(#{leveled := Leveled}, [Pid, _Opts]) ->
    Pid == Leveled.

drop_adapt(#{leveled := Leveled}, [_Pid, Opts]) ->
    [Leveled, Opts].
    
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
    init_backend_next(S1#{model => orddict:new()}, 
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

drop_features(#{model := Model}, [_Pid, _Opts], _Res) ->
    Size = orddict:size(Model),
    [{drop, empty} || Size == 0 ] ++ [{drop, Size div 10} || Size > 0 ].



%% --- Operation: kill ---
kill_pre(S) ->
    is_leveled_open(S).

kill_args(#{leveled := Pid}) ->
    [Pid].

kill_pre(#{leveled := Leveled}, [Pid]) ->
    Pid == Leveled.

kill_adapt(#{leveled := Leveled}, [_]) ->
    [ Leveled ].

kill(Pid) ->
    exit(Pid, kill),
    timer:sleep(1).

kill_next(S, Value, [Pid]) ->
    stop_next(S, Value, [Pid]).

    


%% Testing fold:
%% Note async and sync mode!
%% see https://github.com/martinsumner/riak_kv/blob/mas-2.2.5-tictactaae/src/riak_kv_leveled_backend.erl#L238-L419

%% --- Operation: index folding ---
indexfold_pre(S) ->
    is_leveled_open(S).

indexfold_args(#{leveled := Pid, counter := Counter, previous_keys := PK}) ->
    ?LET({Key, Bucket}, gen_key_in_bucket(PK),
         [Pid, oneof([Bucket, {Bucket, Key}]), fold_collect, {range, 1, 10}, {bool(), undefined},
          Counter  %% add a unique counter
         ]).

indexfold_pre(#{leveled := Leveled}, [Pid, _Constraint, _FoldFun, _Range, _TermHandling, _Counter]) ->
    %% Make sure we operate on an existing Pid when shrinking
    %% Check start options validity as well?
    Pid == Leveled.
    
indexfold_adapt(#{leveled := Leveled}, [_, Constraint, FoldFun, Range, TermHandling, Counter]) ->
    %% Keep the counter!
    [Leveled, Constraint, FoldFun, Range, TermHandling, Counter].

indexfold(Pid, Constraint, FoldFun, Range, TermHandling, _Counter) ->
    FoldAccT = ?MODULE:FoldFun(),
    {async, Folder} = leveled_bookie:book_indexfold(Pid, Constraint, FoldAccT, Range, TermHandling),
    Folder.

indexfold_next(#{folders := Folders, model := _Model} = S, SymFolder, 
               [_, _Constraint, FoldFun, _Range, _TermHandling, Counter]) ->
    S#{folders => 
           Folders ++ 
           [#{counter => Counter, 
              folder => SymFolder, 
              foldfun => FoldFun, 
              result => []         %% fold over the snapshot
             }],
       counter =>  Counter + 1}.

indexfold_post(_S, _, Res) ->
    is_function(Res).

indexfold_features(_S, [_Pid, _Constraint, FoldFun, _Range, _TermHandling, _Counter], _Res) ->
    [{foldAccT, FoldFun}]. %% This will be extracted for printing later




%% --- Operation: keylist folding ---
%% slack discussion: "`book_keylist` only passes `Bucket` and `Key` into the accumulator, ignoring SubKey - 
%% so I don't think this can be used in head_only mode to return results that make sense"
keylistfold1_pre(S) ->
    is_leveled_open(S) andalso not in_head_only_mode(S).

keylistfold1_args(#{leveled := Pid, counter := Counter, start_opts := Opts}) ->
    [Pid, gen_tag(Opts), gen_foldacc(),
     Counter  %% add a unique counter
    ].

keylistfold1_pre(#{leveled := Leveled}, [Pid, _Tag, _FoldFun, _Counter]) ->
    %% Make sure we operate on an existing Pid when shrinking
    %% Check start options validity as well?
    Pid == Leveled.
    
keylistfold1_adapt(#{leveled := Leveled}, [_, Tag, FoldFun, Counter]) ->
    %% Keep the counter!
    [Leveled, Tag, FoldFun, Counter].

keylistfold1(Pid, Tag, FoldAccT, _Counter) ->
    %% FoldAccT = ?MODULE:FoldFun(),
    {async, Folder} = leveled_bookie:book_keylist(Pid, Tag, FoldAccT),
    Folder.

keylistfold1_next(#{folders := Folders, model := Model, start_opts := Opts} = S, SymFolder, 
               [_, Tag, FoldAccT, Counter]) ->
    {Fun, Acc} = FoldAccT, 
    S#{folders => 
           Folders ++ 
           [#{counter => Counter, 
              folder => SymFolder, 
              foldfun => FoldAccT, 
              result => case Model == orddict:new() orelse not valid_tag(Tag, Opts)  of
                            true -> Acc;
                            false -> 
                                orddict:fold(fun({B, K}, _V, A) -> Fun(B, K, A) end, Acc, Model)
                        end
             }],
       counter => Counter + 1}.

keylistfold1_post(_S, _, Res) ->
    is_function(Res).

keylistfold1_features(_S, [_Pid, _Tag, FoldAccT, _Counter], _Res) ->
    [{foldAccT, FoldAccT}]. %% This will be extracted for printing later


%% --- Operation: fold_run ---
fold_run_pre(S) ->
    maps:get(folders, S, []) =/= [].

fold_run_args(#{folders := Folders}) ->
    ?LET(#{counter := Counter, folder := Folder}, elements(Folders),
         [Counter, Folder]).

fold_run_pre(#{folders := Folders}, [Counter, _Folder]) ->
    %% Ensure membership even under shrinking
    %% Counter is fixed at first generation and does not shrink!
    get_foldobj(Folders, Counter) =/= undefined.

fold_run(_, Folder) ->
    catch Folder().

fold_run_next(#{folders := Folders} = S, _Value, [Counter, _Folder]) ->
    %% leveled_runner comment: "Iterators should de-register themselves from the Penciller on completion."
    FoldObj = get_foldobj(Folders, Counter),
    UsedFolders = maps:get(used_folders, S, []),
    S#{folders => Folders -- [FoldObj],
       used_folders => UsedFolders ++ [maps:with([counter, folder], FoldObj)]}.
    
fold_run_post(#{folders := Folders, leveled := Leveled}, [Count, _], Res) ->
    #{result := Result} = get_foldobj(Folders, Count),
    case Leveled of 
        undefined ->
            is_exit(Res);
        _ ->
            eq(Res, Result)
    end.

               
%% --- Operation: fold_run on already used folder ---
%% A fold that has already ran to completion should results in an exception when re-used.
%% leveled_runner comment: "Iterators should de-register themselves from the Penciller on completion."
noreuse_fold_pre(S) ->
    maps:get(used_folders, S, []) =/= [].

noreuse_fold_args(#{used_folders := Folders}) ->
    ?LET(#{counter := Counter, folder := Folder}, elements(Folders),
         [Counter, Folder]).

noreuse_fold_pre(S, [Counter, _Folder]) ->
    %% Ensure membership even under shrinking
    %% Counter is fixed at first generation and does not shrink!
    lists:member(Counter, 
                 [ maps:get(counter, Used) || Used <- maps:get(used_folders, S, []) ]).

noreuse_fold(_, Folder) ->
    catch Folder().

noreuse_fold_post(_S, [_, _], Res) ->
    Res == [] orelse  %% This seems weird!
        is_exit(Res).

noreuse_fold_features(#{leveled := Leveled}, [_, _], _) ->
    [ case Leveled of
          undefined -> 
              reuse_fold_when_closed;
          _ ->
              reuse_fold_when_open
      end ].


%% --- Operation: fold_run on folder that survived a crash ---
%% A fold that has already ran to completion should results in an exception when re-used.
stop_fold_pre(S) ->
    maps:get(stop_folders, S, []) =/= [].

stop_fold_args(#{stop_folders := Folders}) ->
    ?LET(#{counter := Counter, folder := Folder}, elements(Folders),
         [Counter, Folder]).

stop_fold_pre(S, [Counter, _Folder]) ->
    %% Ensure membership even under shrinking
    %% Counter is fixed at first generation and does not shrink!
    lists:member(Counter, 
                 [ maps:get(counter, Used) || Used <- maps:get(stop_folders, S, []) ]).

stop_fold(_, Folder) ->
    catch Folder().

stop_fold_post(_S, [_Counter, _], Res) ->
    is_exit(Res).

stop_fold_features(S, [_, _], _) ->
    [ case maps:get(leveled, S) of
          undefined -> 
              stop_fold_when_closed;
          _ ->
              stop_fold_when_open
      end ].


weight(#{previous_keys := []}, Command) when Command == get;
                                             Command == delete ->
    1;
weight(S, C) when C == get;
                  C == put ->
    ?CMD_VALID(S, put, 10, 1);
weight(_S, stop) ->
    1;
weight(_, _) ->
    1.


is_valid_cmd(S, put) ->
    not in_head_only_mode(S);
is_valid_cmd(S, delete) ->
    is_valid_cmd(S, put);
is_valid_cmd(S, get) ->
    not in_head_only_mode(S);
is_valid_cmd(S, mput) ->
    in_head_only_mode(S).



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

            RunResult = execute(Kind, Cmds, [{dir, Dir}]),
            %% Do not extract the 'state' from this tuple, since parallel commands
            %% miss the notion of final state.
            CallFeatures = [ Feature || Feature <- call_features(history(RunResult)), not is_foldaccT(Feature)],

            case whereis(sut) of
                undefined -> delete_level_data(Dir);
                Pid when is_pid(Pid) ->
                    leveled_bookie:book_destroy(Pid)
            end,

            Wait = wait_for_procs(Procs, 500),
            RunTime = erlang:system_time(millisecond) - StartTime,

            %% Since in parallel commands we don't have access to the state, we retrieve functions
            %% from the features
            FoldAccTs = [ FoldAccT || Entry <- history(RunResult),
                                      {foldAccT, FoldAccT} <- eqc_statem:history_features(Entry)],

            pretty_commands(?MODULE, Cmds, RunResult,
            measure(time_per_test, RunTime,
            aggregate(command_names(Cmds),
            collect(Kind,
            aggregate(with_title('Features'), CallFeatures,
            features(CallFeatures,
                      conjunction([{result, 
                                    ?WHENFAIL([ begin
                                                    eqc:format("~p with acc ~p:\n~s\n", [F, Acc,
                                                                                         show_function(F)])
                                                end || {F, Acc} <- FoldAccTs ],
                                              result(RunResult) == ok)},
                                   {data_cleanup, 
                                    ?WHENFAIL(eqc:format("~s\n", [os:cmd("ls -Rl " ++ Dir)]),
                                              empty_dir(Dir))},
                                   {pid_cleanup, equals(Wait, [])}])))))))

        end)
    end).

history({H, _, _}) -> H.
result({_, _, Res}) -> Res.

execute(seq, Cmds, Env) ->
    run_commands(Cmds, Env);
execute(par, Cmds, Env) ->
    run_parallel_commands(Cmds, Env).

is_exit({'EXIT', _}) ->
    true;
is_exit(Other) ->
    {expected_exit, Other}.

is_foldaccT({foldAccT, _}) ->
    true;
is_foldaccT(_) ->
    false.

show_function(F) ->
    case proplists:get_value(module, erlang:fun_info(F)) of
        eqc_fun ->
            eqc_fun:show_function(F);
        _ ->
            proplists:get_value(name, erlang:fun_info(F))
    end.


gen_opts() ->
    ?LET([HeadOnly, CompMethod, CompPoint], vector(3, bool()),
         [{head_only, elements([false, no_lookup, with_lookup])} || HeadOnly] ++
             [{compression_method, elements([native, lz4])} || CompMethod] ++
             [{compression_point, elements([on_compact, on_receipt])} || CompPoint]
        ).

gen_key() ->
    binary(16).

%% Cannot be atoms!
gen_bucket() -> 
    elements([<<"bucket1">>, <<"bucket2">>, <<"bucket3">>]).

gen_val() ->
    noshrink(binary(32)).

gen_key_in_bucket([]) ->
    {gen_key(), gen_bucket()};
gen_key_in_bucket(Previous) ->
    ?LET({K, B}, elements(Previous),
         frequency([{1, gen_key_in_bucket([])},
                    {1, {K, gen_bucket()}},
                    {2, {K, B}}])).

gen_tag(_StartOptions) ->
  oneof([?STD_TAG, ?IDX_TAG, ?HEAD_TAG]).

valid_tag(?STD_TAG, StartOptions) ->
    proplists:get_value(head_only, StartOptions, false) == false;
valid_tag(?HEAD_TAG, StartOptions) ->
    proplists:get_value(head_only, StartOptions, false) =/= false;
valid_tag(?IDX_TAG, _StartOptions) ->
    false.

gen_foldacc() ->
    ?SHRINK(oneof([{eqc_fun:function3(int()), int()},
                   {eqc_fun:function3(list(int())), list(int())}]),
            [fold_collect()]).


fold_buckets() ->
    {fun(B, _K, Acc) -> [B | Acc] end, []}.
             
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
%% is populated in state. (We cannot check with is_pid, since that's
%% symbolic in test case generation!).
-spec is_leveled_open(state()) -> boolean().
is_leveled_open(S) ->
    maps:get(leveled, S, undefined) =/= undefined.

in_head_only_mode(S) ->
    proplists:get_value(head_only, maps:get(start_opts, S, []), false) =/= false.

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

%% Slack discussion:
%% `[{add, B1, K1, SK1}, {add, B1, K1, SK2}]` should be fine (same bucket and key, different subkey)
no_key_dups([]) ->
    [];
no_key_dups([{_Action, Bucket, Key, SubKey, _Value} = E | Es]) ->
    [E | no_key_dups([ {A, B, K, SK, V} || {A, B, K, SK, V} <- Es,
                                           {B, K, SK} =/= {Bucket, Key, SubKey}])].
