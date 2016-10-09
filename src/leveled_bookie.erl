%% -------- Overview ---------
%%
%% The eleveleddb is based on the LSM-tree similar to leveldb, except that:
%% - Keys, Metadata and Values are not persisted together - the Keys and
%% Metadata are kept in a tree-based ledger, whereas the values are stored
%% only in a sequential Journal.
%% - Different file formats are used for Journal (based on constant
%% database), and the ledger (sft, based on sst)
%% - It is not intended to be general purpose, but be specifically suited for
%% use as a Riak backend in specific circumstances (relatively large values,
%% and frequent use of iterators)
%% - The Journal is an extended nursery log in leveldb terms.  It is keyed
%% on the sequence number of the write
%% - The ledger is a merge tree, where the key is the actaul object key, and
%% the value is the metadata of the object including the sequence number
%%
%%
%% -------- The actors ---------
%% 
%% The store is fronted by a Bookie, who takes support from different actors:
%% - An Inker who persists new data into the journal, and returns items from
%% the journal based on sequence number
%% - A Penciller who periodically redraws the ledger, that associates keys with
%% sequence numbers and other metadata, as well as secondary keys (for index
%% queries)
%% - One or more Clerks, who may be used by either the inker or the penciller
%% to fulfill background tasks
%%
%% Both the Inker and the Penciller maintain a manifest of the files which
%% represent the current state of the Journal and the Ledger repsectively.
%% For the Inker the manifest maps ranges of sequence numbers to cdb files.
%% For the Penciller the manifest maps key ranges to files at each level of
%% the Ledger.
%%
%% -------- PUT --------
%%
%% A PUT request consists of
%% - A Primary Key and a Value
%% - IndexSpecs - a set of secondary key changes associated with the
%% transaction
%%
%% The Bookie takes the place request and passes it first to the Inker to add
%% the request to the ledger.
%%
%% The inker will pass the PK/Value/IndexSpecs to the current (append only)
%% CDB journal file to persist the change.  The call should return either 'ok'
%% or 'roll'. -'roll' indicates that the CDB file has insufficient capacity for
%% this write.
%%
%% (Note that storing the IndexSpecs will create some duplication with the
%% Metadata wrapped up within the Object value.  This Value and the IndexSpecs
%% are compressed before storage, so this should provide some mitigation for
%% the duplication).
%%
%% In resonse to a 'roll', the inker should:
%% - start a new active journal file with an open_write_request, and then;
%% - call to PUT the object in this file;
%% - reply to the bookie, but then in the background
%% - close the previously active journal file (writing the hashtree), and move
%% it to the historic journal
%%
%% The inker will also return the SQN which the change has been made at, as
%% well as the object size on disk within the Journal.
%%
%% Once the object has been persisted to the Journal, the Ledger can be updated.
%% The Ledger is updated by the Bookie applying a function (extract_metadata/4)
%% to the Value to return the Object Metadata, a function to generate a hash
%% of the Value and also taking the Primary Key, the IndexSpecs, the Sequence
%% Number in the Journal and the Object Size (returned from the Inker).
%%
%% The Bookie should generate a series of ledger key changes from this
%% information, using a function passed in at startup.  For Riak this will be
%% of the form:
%% {{o, Bucket, Key},
%%      SQN,
%%      {Hash, Size, {Riak_Metadata}},
%%      {active, TS}|{tomb, TS}} or
%% {{i, Bucket, IndexTerm, IndexField, Key},
%%      SQN,
%%      null,
%%      {active, TS}|{tomb, TS}}
%%
%% Recent Ledger changes are retained initially in the Bookies' memory (in a
%% small generally balanced tree).  Periodically, the current table is pushed to
%% the Penciller for eventual persistence, and a new table is started.
%%
%% This completes the non-deferrable work associated with a PUT
%%
%% -------- Snapshots (Key & Metadata Only) --------
%%
%% If there is a snapshot request (e.g. to iterate over the keys) the Bookie
%% may request a clone of the Penciller, or the Penciller and the Inker.
%%
%% The clone is seeded with the manifest.  Teh clone should be registered with
%% the real Inker/Penciller, so that the real Inker/Penciller may prevent the
%% deletion of files still in use by a snapshot clone.
%%
%% Iterators should de-register themselves from the Penciller on completion.
%% Iterators should be automatically release after a timeout period.  A file
%% can only be deleted from the Ledger if it is no longer in the manifest, and
%% there are no registered iterators from before the point the file was
%% removed from the manifest.
%%
%% -------- Special Ops --------
%%
%% e.g. Get all for SegmentID/Partition
%%
%%
%%
%% -------- On Startup --------
%%
%% On startup the Bookie must restart both the Inker to load the Journal, and
%% the Penciller to load the Ledger.  Once the Penciller has started, the
%% Bookie should request the highest sequence number in the Ledger, and then
%% and try and rebuild any missing information from the Journal.
%%
%% To rebuild the Ledger it requests the Inker to scan over the files from
%% the sequence number and re-generate the Ledger changes - pushing the changes
%% directly back into the Ledger.



-module(leveled_bookie).

-behaviour(gen_server).

-include("../include/leveled.hrl").

-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3,
        book_start/1,
        book_riakput/3,
        book_riakget/3,
        book_riakhead/3,
        book_snapshotstore/3,
        book_snapshotledger/3,
        book_compactjournal/2,
        book_close/1,
        strip_to_keyonly/1,
        strip_to_keyseqonly/1,
        strip_to_seqonly/1,
        strip_to_statusonly/1,
        striphead_to_details/1]).

-include_lib("eunit/include/eunit.hrl").

-define(CACHE_SIZE, 1000).
-define(JOURNAL_FP, "journal").
-define(LEDGER_FP, "ledger").
-define(SHUTDOWN_WAITS, 60).
-define(SHUTDOWN_PAUSE, 10000).
-define(SNAPSHOT_TIMEOUT, 300000).

-record(state, {inker :: pid(),
                penciller :: pid(),
                cache_size :: integer(),
                back_pressure :: boolean(),
                ledger_cache :: gb_trees:tree(),
                is_snapshot :: boolean()}).



%%%============================================================================
%%% API
%%%============================================================================

book_start(Opts) ->
    gen_server:start(?MODULE, [Opts], []).

book_riakput(Pid, Object, IndexSpecs) ->
    PrimaryKey = {o, Object#r_object.bucket, Object#r_object.key},
    gen_server:call(Pid, {put, PrimaryKey, Object, IndexSpecs}, infinity).

book_riakget(Pid, Bucket, Key) ->
    PrimaryKey = {o, Bucket, Key},    
    gen_server:call(Pid, {get, PrimaryKey}, infinity).

book_riakhead(Pid, Bucket, Key) ->
    PrimaryKey = {o, Bucket, Key},
    gen_server:call(Pid, {head, PrimaryKey}, infinity).

book_snapshotstore(Pid, Requestor, Timeout) ->
    gen_server:call(Pid, {snapshot, Requestor, store, Timeout}, infinity).

book_snapshotledger(Pid, Requestor, Timeout) ->
    gen_server:call(Pid, {snapshot, Requestor, ledger, Timeout}, infinity).

book_compactjournal(Pid, Timeout) ->
    gen_server:call(Pid, {compact_journal, Timeout}, infinity).

book_close(Pid) ->
    gen_server:call(Pid, close, infinity).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([Opts]) ->
    case Opts#bookie_options.snapshot_bookie of
        undefined ->
            % Start from file not snapshot
            {InkerOpts, PencillerOpts} = set_options(Opts),
            {Inker, Penciller} = startup(InkerOpts, PencillerOpts),
            CacheSize = if
                            Opts#bookie_options.cache_size == undefined ->
                                ?CACHE_SIZE;
                            true ->
                                Opts#bookie_options.cache_size
                        end,
            io:format("Bookie starting with Pcl ~w Ink ~w~n",
                                                [Penciller, Inker]),
            {ok, #state{inker=Inker,
                        penciller=Penciller,
                        cache_size=CacheSize,
                        ledger_cache=gb_trees:empty(),
                        is_snapshot=false}};
        Bookie ->
            {ok,
                {Penciller, LedgerCache},
                Inker} = book_snapshotstore(Bookie, self(), ?SNAPSHOT_TIMEOUT),
            ok = leveled_penciller:pcl_loadsnapshot(Penciller, []),
            io:format("Snapshot starting with Pcl ~w Ink ~w~n",
                                                [Penciller, Inker]),
            {ok, #state{penciller=Penciller,
                        inker=Inker,
                        ledger_cache=LedgerCache,
                        is_snapshot=true}}
    end.


handle_call({put, PrimaryKey, Object, IndexSpecs}, From, State) ->
    {ok, SQN, ObjSize} = leveled_inker:ink_put(State#state.inker,
                                                PrimaryKey,
                                                Object,
                                                IndexSpecs),
    Changes = preparefor_ledgercache(PrimaryKey,
                                        SQN,
                                        Object,
                                        ObjSize,
                                        IndexSpecs),
    Cache0 = addto_ledgercache(Changes, State#state.ledger_cache),
    gen_server:reply(From, ok),
    case maybepush_ledgercache(State#state.cache_size,
                                            Cache0,
                                            State#state.penciller) of
        {ok, NewCache} ->
            {noreply, State#state{ledger_cache=NewCache, back_pressure=false}};
        {pause, NewCache} ->
            {noreply, State#state{ledger_cache=NewCache, back_pressure=true}}
    end;
handle_call({get, Key}, _From, State) ->
    case fetch_head(Key, State#state.penciller, State#state.ledger_cache) of
        not_present ->
            {reply, not_found, State};
        Head ->
            {Seqn, Status, _MD} = striphead_to_details(Head),
            case Status of
                {tomb, _} ->
                    {reply, not_found, State};
                {active, _} ->
                    case fetch_value(Key, Seqn, State#state.inker) of
                        not_present ->
                            {reply, not_found, State};
                        Object ->
                            {reply, {ok, Object}, State}
                    end
            end
    end;
handle_call({head, Key}, _From, State) ->
    case fetch_head(Key, State#state.penciller, State#state.ledger_cache) of
        not_present ->
            {reply, not_found, State};
        Head ->
            {_Seqn, Status, MD} = striphead_to_details(Head),
            case Status of
                {tomb, _} ->
                    {reply, not_found, State};
                {active, _} ->
                    OMD = build_metadata_object(Key, MD),
                    {reply, {ok, OMD}, State}
            end
    end;
handle_call({snapshot, _Requestor, SnapType, _Timeout}, _From, State) ->
    PCLopts = #penciller_options{start_snapshot=true,
                                    source_penciller=State#state.penciller},
    {ok, LedgerSnapshot} = leveled_penciller:pcl_start(PCLopts),
    case SnapType of
        store ->
            InkerOpts = #inker_options{start_snapshot=true,
                                        source_inker=State#state.inker},
            {ok, JournalSnapshot} = leveled_inker:ink_start(InkerOpts),
            {reply,
                {ok,
                    {LedgerSnapshot,
                        State#state.ledger_cache},
                    JournalSnapshot},
                State};
        ledger ->
            {reply,
                {ok,
                    {LedgerSnapshot,
                        State#state.ledger_cache},
                    null},
                State}
    end;
handle_call({compact_journal, Timeout}, _From, State) ->
    ok = leveled_inker:ink_compactjournal(State#state.inker,
                                            self(),
                                            Timeout),
    {reply, ok, State};
handle_call(close, _From, State) ->
    {stop, normal, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, State) ->
    io:format("Bookie closing for reason ~w~n", [Reason]),
    WaitList = lists:duplicate(?SHUTDOWN_WAITS, ?SHUTDOWN_PAUSE),
    ok = case shutdown_wait(WaitList, State#state.inker) of
            false ->
                io:format("Forcing close of inker following wait of "
                                ++ "~w milliseconds~n",
                            [lists:sum(WaitList)]),
                leveled_inker:ink_forceclose(State#state.inker);
            true ->
                ok
        end,
    ok = leveled_penciller:pcl_close(State#state.penciller).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% Internal functions
%%%============================================================================

shutdown_wait([], _Inker) ->
    false;
shutdown_wait([TopPause|Rest], Inker) ->
    case leveled_inker:ink_close(Inker) of
        ok ->
            true;
        pause ->
            io:format("Inker shutdown stil waiting process to complete~n"),
            ok = timer:sleep(TopPause),
            shutdown_wait(Rest, Inker)
    end.
    

set_options(Opts) ->
    MaxJournalSize = case Opts#bookie_options.max_journalsize of
                            undefined ->
                                30000;
                            MS ->
                                MS
                        end,
    {#inker_options{root_path = Opts#bookie_options.root_path ++
                                    "/" ++ ?JOURNAL_FP,
                        cdb_options = #cdb_options{max_size=MaxJournalSize,
                                                    binary_mode=true}},
        #penciller_options{root_path=Opts#bookie_options.root_path ++
                                    "/" ++ ?LEDGER_FP}}.

startup(InkerOpts, PencillerOpts) ->
    {ok, Inker} = leveled_inker:ink_start(InkerOpts),
    {ok, Penciller} = leveled_penciller:pcl_start(PencillerOpts),
    LedgerSQN = leveled_penciller:pcl_getstartupsequencenumber(Penciller),
    io:format("LedgerSQN=~w at startup~n", [LedgerSQN]),
    ok = leveled_inker:ink_loadpcl(Inker,
                                    LedgerSQN + 1,
                                    fun load_fun/5,
                                    Penciller),
    {Inker, Penciller}.


fetch_head(Key, Penciller, Cache) ->
    case gb_trees:lookup(Key, Cache) of
        {value, Head} ->
            Head;
        none ->
            case leveled_penciller:pcl_fetch(Penciller, Key) of
                {Key, Head} ->
                    Head;
                not_present ->
                    not_present
            end
    end.

fetch_value(Key, SQN, Inker) ->
    case leveled_inker:ink_fetch(Inker, Key, SQN) of
        {ok, Value} ->
            Value;
        not_present ->
            not_present
    end.

%% Format of a Key within the ledger is
%% {PrimaryKey, SQN, Metadata, Status} 

strip_to_keyonly({keyonly, K}) -> K;
strip_to_keyonly({K, _V}) -> K.

strip_to_keyseqonly({K, {SeqN, _, _}}) -> {K, SeqN}.

strip_to_statusonly({_, {_, St, _}}) -> St.

strip_to_seqonly({_, {SeqN, _, _}}) -> SeqN.

striphead_to_details({SeqN, St, MD}) -> {SeqN, St, MD}.

get_metadatas(#r_object{contents=Contents}) ->
    [Content#r_content.metadata || Content <- Contents].

set_vclock(Object=#r_object{}, VClock) -> Object#r_object{vclock=VClock}.

vclock(#r_object{vclock=VClock}) -> VClock.

to_binary(v0, Obj) ->
    term_to_binary(Obj).

hash(Obj=#r_object{}) ->
    Vclock = vclock(Obj),
    UpdObj = set_vclock(Obj, lists:sort(Vclock)),
    erlang:phash2(to_binary(v0, UpdObj)).

extract_metadata(Obj, Size) ->
    {get_metadatas(Obj), vclock(Obj), hash(Obj), Size}.

build_metadata_object(PrimaryKey, Head) ->
    {o, Bucket, Key} = PrimaryKey,
    {MD, VC, _, _} = Head,
    Contents = lists:foldl(fun(X, Acc) -> Acc ++ [#r_content{metadata=X}] end,
                            [],
                            MD),
    #r_object{contents=Contents, bucket=Bucket, key=Key, vclock=VC}.

convert_indexspecs(IndexSpecs, SQN, PrimaryKey) ->
    lists:map(fun({IndexOp, IndexField, IndexValue}) ->
                        Status = case IndexOp of
                                    add ->
                                        %% TODO: timestamp support
                                        {active, infinity};
                                    remove ->
                                        %% TODO: timestamps for delayed reaping 
                                        {tomb, infinity}
                                end,
                        {o, B, K} = PrimaryKey,
                        {{i, B, IndexField, IndexValue, K},
                            {SQN, Status, null}}
                    end,
                IndexSpecs).


preparefor_ledgercache(PK, SQN, Obj, Size, IndexSpecs) ->
    PrimaryChange = {PK,
                        {SQN,
                            {active, infinity},
                            extract_metadata(Obj, Size)}},
    SecChanges = convert_indexspecs(IndexSpecs, SQN, PK),
    [PrimaryChange] ++ SecChanges.

addto_ledgercache(Changes, Cache) ->
    lists:foldl(fun({K, V}, Acc) -> gb_trees:enter(K, V, Acc) end,
                    Cache,
                    Changes).

maybepush_ledgercache(MaxCacheSize, Cache, Penciller) ->
    CacheSize = gb_trees:size(Cache),
    if
        CacheSize > MaxCacheSize ->
            case leveled_penciller:pcl_pushmem(Penciller,
                                                gb_trees:to_list(Cache)) of
                ok ->
                    {ok, gb_trees:empty()};
                pause ->
                    {pause, gb_trees:empty()};
                refused ->
                    {ok, Cache}
            end;
        true ->
            {ok, Cache}
    end.

load_fun(KeyInLedger, ValueInLedger, _Position, Acc0, ExtractFun) ->
    {MinSQN, MaxSQN, Output} = Acc0,
    {SQN, PK} = KeyInLedger,
    % VBin may already be a term
    {VBin, VSize} = ExtractFun(ValueInLedger), 
    {Obj, IndexSpecs} = case is_binary(VBin) of
                            true ->
                                binary_to_term(VBin);
                            false ->
                                VBin
                        end,
    case SQN of
        SQN when SQN < MinSQN ->
            {loop, Acc0};    
        SQN when SQN < MaxSQN ->
            Changes = preparefor_ledgercache(PK, SQN, Obj, VSize, IndexSpecs),
            {loop, {MinSQN, MaxSQN, Output ++ Changes}};
        MaxSQN ->
            io:format("Reached end of load batch with SQN ~w~n", [SQN]),
            Changes = preparefor_ledgercache(PK, SQN, Obj, VSize, IndexSpecs),
            {stop, {MinSQN, MaxSQN, Output ++ Changes}};
        SQN when SQN > MaxSQN ->
            io:format("Skipping as exceeded MaxSQN ~w with SQN ~w~n",
                        [MaxSQN, SQN]),
            {stop, Acc0}
    end.


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

reset_filestructure() ->
    RootPath  = "../test",
    leveled_inker:clean_testdir(RootPath ++ "/" ++ ?JOURNAL_FP),
    leveled_penciller:clean_testdir(RootPath ++ "/" ++ ?LEDGER_FP),
    RootPath.

generate_multiple_objects(Count, KeyNumber) ->
    generate_multiple_objects(Count, KeyNumber, []).
    
generate_multiple_objects(0, _KeyNumber, ObjL) ->
    ObjL;
generate_multiple_objects(Count, KeyNumber, ObjL) ->
    Obj = {"Bucket",
            "Key" ++ integer_to_list(KeyNumber),
            crypto:rand_bytes(1024),
            [],
            [{"MDK", "MDV" ++ integer_to_list(KeyNumber)},
                {"MDK2", "MDV" ++ integer_to_list(KeyNumber)}]},
    {B1, K1, V1, Spec1, MD} = Obj,
    Content = #r_content{metadata=MD, value=V1},
    Obj1 = #r_object{bucket=B1, key=K1, contents=[Content], vclock=[{'a',1}]},
    generate_multiple_objects(Count - 1, KeyNumber + 1, ObjL ++ [{Obj1, Spec1}]).


single_key_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} = book_start(#bookie_options{root_path=RootPath}),
    {B1, K1, V1, Spec1, MD} = {"Bucket1",
                                "Key1",
                                "Value1",
                                [],
                                {"MDK1", "MDV1"}},
    Content = #r_content{metadata=MD, value=V1},
    Object = #r_object{bucket=B1, key=K1, contents=[Content], vclock=[{'a',1}]},
    ok = book_riakput(Bookie1, Object, Spec1),
    {ok, F1} = book_riakget(Bookie1, B1, K1),
    ?assertMatch(F1, Object),
    ok = book_close(Bookie1),
    {ok, Bookie2} = book_start(#bookie_options{root_path=RootPath}),
    {ok, F2} = book_riakget(Bookie2, B1, K1),
    ?assertMatch(F2, Object),
    ok = book_close(Bookie2),
    reset_filestructure().

multi_key_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} = book_start(#bookie_options{root_path=RootPath}),
    {B1, K1, V1, Spec1, MD1} = {"Bucket",
                                "Key1",
                                "Value1",
                                [],
                                {"MDK1", "MDV1"}},
    C1 = #r_content{metadata=MD1, value=V1},
    Obj1 = #r_object{bucket=B1, key=K1, contents=[C1], vclock=[{'a',1}]},
    {B2, K2, V2, Spec2, MD2} = {"Bucket",
                                "Key2",
                                "Value2",
                                [],
                                {"MDK2", "MDV2"}},
    C2 = #r_content{metadata=MD2, value=V2},
    Obj2 = #r_object{bucket=B2, key=K2, contents=[C2], vclock=[{'a',1}]},
    ok = book_riakput(Bookie1, Obj1, Spec1),
    ObjL1 = generate_multiple_objects(100, 3),
    SW1 = os:timestamp(),
    lists:foreach(fun({O, S}) -> ok = book_riakput(Bookie1, O, S) end, ObjL1),
    io:format("PUT of 100 objects completed in ~w microseconds~n",
                [timer:now_diff(os:timestamp(),SW1)]),
    ok = book_riakput(Bookie1, Obj2, Spec2),
    {ok, F1A} = book_riakget(Bookie1, B1, K1),
    ?assertMatch(F1A, Obj1),
    {ok, F2A} = book_riakget(Bookie1, B2, K2),
    ?assertMatch(F2A, Obj2),
    ObjL2 = generate_multiple_objects(100, 103),
    SW2 = os:timestamp(),
    lists:foreach(fun({O, S}) -> ok = book_riakput(Bookie1, O, S) end, ObjL2),
    io:format("PUT of 100 objects completed in ~w microseconds~n",
                [timer:now_diff(os:timestamp(),SW2)]),
    {ok, F1B} = book_riakget(Bookie1, B1, K1),
    ?assertMatch(F1B, Obj1),
    {ok, F2B} = book_riakget(Bookie1, B2, K2),
    ?assertMatch(F2B, Obj2),
    ok = book_close(Bookie1),
    %% Now reopen the file, and confirm that a fetch is still possible
    {ok, Bookie2} = book_start(#bookie_options{root_path=RootPath}),
    {ok, F1C} = book_riakget(Bookie2, B1, K1),
    ?assertMatch(F1C, Obj1),
    {ok, F2C} = book_riakget(Bookie2, B2, K2),
    ?assertMatch(F2C, Obj2),
    ObjL3 = generate_multiple_objects(100, 203),
    SW3 = os:timestamp(),
    lists:foreach(fun({O, S}) -> ok = book_riakput(Bookie2, O, S) end, ObjL3),
    io:format("PUT of 100 objects completed in ~w microseconds~n",
                [timer:now_diff(os:timestamp(),SW3)]),
    {ok, F1D} = book_riakget(Bookie2, B1, K1),
    ?assertMatch(F1D, Obj1),
    {ok, F2D} = book_riakget(Bookie2, B2, K2),
    ?assertMatch(F2D, Obj2),
    ok = book_close(Bookie2),
    reset_filestructure().

indexspecs_test() ->
    IndexSpecs = [{add, "t1_int", 456},
                    {add, "t1_bin", "adbc123"},
                    {remove, "t1_bin", "abdc456"}],
    Changes = convert_indexspecs(IndexSpecs, 1, {o, "Bucket", "Key2"}),
    ?assertMatch({{i, "Bucket", "t1_int", 456, "Key2"},
                    {1, {active, infinity}, null}}, lists:nth(1, Changes)),
    ?assertMatch({{i, "Bucket", "t1_bin", "adbc123", "Key2"},
                    {1, {active, infinity}, null}}, lists:nth(2, Changes)),
    ?assertMatch({{i, "Bucket", "t1_bin", "abdc456", "Key2"},
                    {1, {tomb, infinity}, null}}, lists:nth(3, Changes)).
    
-endif.