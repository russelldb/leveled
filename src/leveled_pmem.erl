%% -------- PENCILLER MEMORY ---------
%%
%% Module that provides functions for maintaining the L0 memory of the
%% Penciller.
%%
%% It is desirable that the L0Mem can efficiently handle the push of new trees
%% whilst maintaining the capability to quickly snapshot the memory for clones
%% of the Penciller.
%%
%% ETS tables are not used due to complications with managing their mutability,
%% as the database is snapshotted.
%%
%% An attempt was made to merge all trees into a single tree on push (in a
%% spawned process), but this proved to have an expensive impact as the tree
%% got larger.
%%
%% This approach is to keep a list of trees which have been received in the
%% order which they were received.  There is then a fixed-size array of hashes
%% used to either point lookups at the right tree in the list, or inform the
%% requestor it is not present avoiding any lookups.
%%
%% The trade-off taken with the approach is that the size of the L0Cache is
%% uncertain.  The Size count is incremented based on the inbound size and so
%% does not necessarily reflect the size once the lists are merged (reflecting
%% rotating objects)

-module(leveled_pmem).

-behaviour(gen_server).

-include("include/leveled.hrl").

%% Gen server to support an accumalator for a given build up of penciller
%% memory

-export([
        init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3
        ]).

-export([
        pmem_start/0,
        pmem_pushtomerge/2,
        pmem_clonerequest/3,
        pmem_queryrequest/3,
        pmem_fetchandclose/1,
        pmem_close/1
        ]).

%% External functions for manipulating penciller memory

-export([
        prepare_for_index/2,
        add_to_cache/4,
        check_levelzero/3,
        check_levelzero/4,
        merge_trees/4,
        add_to_index/3,
        new_index/0,
        clear_index/1,
        check_index/2
        ]).      

-include_lib("eunit/include/eunit.hrl").

-define(SKIP_WIDTH, 128).
-define(TREE_TYPE, idxt).
-define(TIMEOUT_MS, 60000).

-record(state,
            {
                current_clones = [] :: list(tuple()),
                copied_clones = [] :: list(tuple()),
                accumulated_list = [] :: list(tuple()),
                persisted = false :: boolean()
                        }).


% -type index_array() :: array:array().
-type index_array() :: any(). % To live with OTP16

%%%============================================================================
%%% API
%%%
%%% Related to the accumulator of the penciller memory, used by clones and L0
%%% files
%%%
%%%============================================================================

pmem_start() ->
    gen_server:start(?MODULE, [], []).

pmem_pushtomerge(Pid, LedgerCache) ->
    gen_server:cast(Pid, {push_to_merge, LedgerCache}).

pmem_clonerequest(Pid, Clone, LedgerCache) ->
    gen_server:call(Pid, {clone_request, Clone, LedgerCache}).

pmem_queryrequest(Pid, Clone, Query) ->
    gen_server:call(Pid, {query_request, Clone, Query}, 10000).

pmem_fetchandclose(Pid) ->
    gen_server:call(Pid, fetch_and_close, infinity).

pmem_close(Pid) ->
    gen_server:call(Pid, close).


%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init(_Opts) ->
    {ok, #state{}}.


handle_call({clone_request, Clone, LedgerCache}, _From, State) ->
    UpdCurrentClones = [{Clone, LedgerCache}|State#state.current_clones],
    {reply, ok, State#state{current_clones = UpdCurrentClones}, ?TIMEOUT_MS};

handle_call({query_request, Clone, {StartKey, EndKey}}, _From, State) ->
    {Reply, State0} =
        case lists:keytake(Clone, 1, State#state.current_clones) of
            {value, {Clone, CloneLedgerCache}, UpdCurrClones} ->
                CacheRange = leveled_tree:match_range(StartKey,
                                                        EndKey,
                                                        CloneLedgerCache),
                PMem = State#state.accumulated_list,
                PMemTree = leveled_tree:from_orderedlist(PMem,
                                                            ?TREE_TYPE,
                                                            ?SKIP_WIDTH),
                PMemRange = leveled_tree:match_range(StartKey,
                                                        EndKey,
                                                        PMemTree),
                QueryResult = lists:ukeymerge(1, CacheRange, PMemRange),
                {QueryResult, State#state{current_clones=UpdCurrClones}};
            false ->
                {value, {Clone, CloneTree}, UpdCopiedClones} =
                    lists:keytake(Clone, 1, State#state.copied_clones),
                QueryResult = leveled_tree:match_range(StartKey,
                                                        EndKey,
                                                        CloneTree),
                {QueryResult, State#state{copied_clones=UpdCopiedClones}}
        end,
    {reply, Reply, State0, ?TIMEOUT_MS};
handle_call({query_request, Clone, no_lookup}, _From, State) ->
    {Reply, State0} =
        case lists:keytake(Clone, 1, State#state.current_clones) of
            {value, {Clone, CloneLedgerCache}, UpdCurrClones} ->
                PMem0 =
                    lists:ukeymerge(1,
                                    leveled_tree:to_list(CloneLedgerCache),
                                    State#state.accumulated_list),
                {PMem0, State#state{current_clones=UpdCurrClones}};
            false ->
                {value, {Clone, CloneTree}, UpdCopiedClones} =
                    lists:keytake(Clone, 1, State#state.copied_clones),
                {leveled_tree:to_list(CloneTree),
                    State#state{copied_clones=UpdCopiedClones}}
        end,
    {reply, Reply, State0, ?TIMEOUT_MS};

handle_call(fetch_and_close, _From, State) ->
    {reply,
        State#state.accumulated_list,
        State#state{persisted=true},
        ?TIMEOUT_MS};
    
handle_call(clones_waiting, _From, State) ->
    {reply,
        {length(State#state.copied_clones),
            length(State#state.current_clones)},
        State,
        ?TIMEOUT_MS};

handle_call(close, _From, State) ->
    {stop, normal, ok, State}.


handle_cast({push_to_merge, LedgerCache}, State=#state{persisted=Persisted})
                                                    when Persisted == false ->
    PMem = State#state.accumulated_list,
    CloneMapFun =
        fun({ClonePID, CloneLedgerCache}) ->
            UpdList = lists:ukeymerge(1,
                                        leveled_tree:to_list(CloneLedgerCache),
                                        PMem),
            UpdTree = leveled_tree:from_orderedlist(UpdList,
                                                    ?TREE_TYPE,
                                                    ?SKIP_WIDTH),
            {ClonePID, UpdTree}
        end,
    
    State0 = 
        case State#state.current_clones of
            [] ->
                State;
            CurrentClones ->
                CopiedClones = lists:map(CloneMapFun, CurrentClones)
                                ++ State#state.copied_clones,
                State#state{current_clones = [], copied_clones = CopiedClones}
        end,

    PMem0 = lists:ukeymerge(1,
                            leveled_tree:to_list(LedgerCache),
                            PMem),
    {noreply, State0#state{accumulated_list = PMem0}, ?TIMEOUT_MS}.


handle_info(timeout, State=#state{persisted=Persisted}) when Persisted == true ->
    {stop, normal, State};
handle_info(timeout, State) ->
    {noreply, State}.

terminate(normal, _State) ->
    ok;
terminate(Reason, _State) ->
    io:format("Reason: ~w~n", [Reason]).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%============================================================================
%%% External Functions
%%%============================================================================

-spec prepare_for_index(index_array(), integer()|no_lookup) -> index_array().
%% @doc
%% Add the hash of a key to the index.  This is 'prepared' in the sense that
%% this index is not use until it is loaded into the main index.
%%
%% prepare_for_index is called from the Bookie when been added to the ledger
%% cache, but the index is not used until that ledger cache is in the
%% penciller L0 memory
prepare_for_index(IndexArray, no_lookup) ->
    IndexArray;
prepare_for_index(IndexArray, Hash) ->
    {Slot, H0} = split_hash(Hash),
    Bin = array:get(Slot, IndexArray),
    array:set(Slot, <<Bin/binary, 1:1/integer, H0:23/integer>>, IndexArray).

-spec add_to_index(index_array(), index_array(), integer()) -> index_array().
%% @doc
%% Expand the penciller's current index array with the details from a new
%% ledger cache tree sent from the Bookie.  The tree will have a cache slot
%% which is the index of this ledger_cache in the list of the ledger_caches
add_to_index(LM1Array, L0Index, CacheSlot) when CacheSlot < 128 ->
    IndexAddFun =
        fun(Slot, Acc) ->
            Bin0 = array:get(Slot, Acc),
            BinLM1 = array:get(Slot, LM1Array),
            array:set(Slot,
                        <<Bin0/binary,
                            0:1/integer, CacheSlot:7/integer,
                            BinLM1/binary>>,
                        Acc)
        end,
    lists:foldl(IndexAddFun, L0Index, lists:seq(0, 255)).

-spec new_index() -> index_array().
%% @doc
%% Create a new index array
new_index() ->
    array:new([{size, 256}, {default, <<>>}]).

-spec clear_index(index_array()) -> index_array().
%% @doc
%% Create a new index array
clear_index(_L0Index) ->
    new_index().

-spec check_index(integer(), index_array()|empty_index) -> list(integer()).
%% @doc
%% return a list of positions in the list of cache arrays that may contain the
%% key associated with the hash being checked
check_index(_Hash, empty_index) ->
    [];
check_index(Hash, L0Index) ->
    {Slot, H0} = split_hash(Hash),
    Bin = array:get(Slot, L0Index),
    find_pos(Bin, H0, [], 0).


-spec add_to_cache(integer(),
                    {tuple(), integer(), integer()},
                    integer(),
                    list()) ->
                        {integer(), integer(), list()}.
%% @doc
%% The penciller's cache is a list of leveled_trees, this adds a new tree to
%% that cache, providing an update to the approximate size of the cache and
%% the Ledger's SQN.
add_to_cache(L0Size, {LevelMinus1, MinSQN, MaxSQN}, LedgerSQN, TreeList) ->
    LM1Size = leveled_tree:tsize(LevelMinus1),
    case LM1Size of
        0 ->
            {LedgerSQN, L0Size, TreeList};
        _ ->
            if
                MinSQN >= LedgerSQN ->
                    {MaxSQN,
                        L0Size + LM1Size,
                        lists:append(TreeList, [LevelMinus1])}
            end
    end.

-spec check_levelzero(tuple(), list(integer()), list())
                                            -> {boolean(), tuple|not_found}.
%% @doc
%% Check for the presence of a given Key in the Level Zero cache, with the
%% index array having been checked first for a list of potential positions
%% in the list of ledger caches - and then each potential ledger_cache being
%% checked (with the most recently received cache being checked first) until a
%% match is found.
check_levelzero(Key, PosList, TreeList) ->
    check_levelzero(Key, leveled_codec:magic_hash(Key), PosList, TreeList).

-spec check_levelzero(tuple(), integer(), list(integer()), list())
                                            -> {boolean(), tuple|not_found}.
%% @doc
%% Check for the presence of a given Key in the Level Zero cache, with the
%% index array having been checked first for a list of potential positions
%% in the list of ledger caches - and then each potential ledger_cache being
%% checked (with the most recently received cache being checked first) until a
%% match is found.
check_levelzero(_Key, _Hash, _PosList, []) ->
    {false, not_found};
check_levelzero(_Key, _Hash, [], _TreeList) ->
    {false, not_found};
check_levelzero(Key, Hash, PosList, TreeList) ->
    check_slotlist(Key, Hash, PosList, TreeList).

-spec merge_trees(tuple(), tuple(), list(tuple()), tuple()) -> list().
%% @doc
%% Return a list of keys and values across the level zero cache (and the
%% currently unmerged bookie's ledger cache) that are between StartKey
%% and EndKey (inclusive).
merge_trees(StartKey, EndKey, TreeList, LevelMinus1) ->
    lists:foldl(fun(Tree, Acc) ->
                        R = leveled_tree:match_range(StartKey,
                                                        EndKey,
                                                        Tree),
                        lists:ukeymerge(1, Acc, R) end,
                    [],
                    [LevelMinus1|lists:reverse(TreeList)]).

%%%============================================================================
%%% Internal Functions
%%%============================================================================


find_pos(<<>>, _Hash, PosList, _SlotID) ->
    PosList;
find_pos(<<1:1/integer, Hash:23/integer, T/binary>>, Hash, PosList, SlotID) ->
    find_pos(T, Hash, PosList ++ [SlotID], SlotID);
find_pos(<<1:1/integer, _Miss:23/integer, T/binary>>, Hash, PosList, SlotID) ->
    find_pos(T, Hash, PosList, SlotID);
find_pos(<<0:1/integer, NxtSlot:7/integer, T/binary>>, Hash, PosList, _SlotID) ->
    find_pos(T, Hash, PosList, NxtSlot).


split_hash(Hash) ->
    Slot = Hash band 255,
    H0 = (Hash bsr 8) band 8388607,
    {Slot, H0}.

check_slotlist(Key, _Hash, CheckList, TreeList) ->
    SlotCheckFun =
                fun(SlotToCheck, {Found, KV}) ->
                    case Found of
                        true ->
                            {Found, KV};
                        false ->
                            CheckTree = lists:nth(SlotToCheck, TreeList),
                            case leveled_tree:match(Key, CheckTree) of
                                none ->
                                    {Found, KV};
                                {value, Value} ->
                                    {true, {Key, Value}}
                            end
                    end
                    end,
    lists:foldl(SlotCheckFun,
                    {false, not_found},
                    lists:reverse(CheckList)).

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

generate_randomkeys_aslist(Seqn, Count, BucketRangeLow, BucketRangeHigh) ->
    lists:ukeysort(1,
                    generate_randomkeys(Seqn,
                                            Count,
                                            [],
                                            BucketRangeLow,
                                            BucketRangeHigh)).
        
generate_randomkeys(Seqn, Count, BucketRangeLow, BucketRangeHigh) ->
    KVL = generate_randomkeys(Seqn,
                                Count,
                                [],
                                BucketRangeLow,
                                BucketRangeHigh),
    leveled_tree:from_orderedlist(lists:ukeysort(1, KVL), ?CACHE_TYPE).

generate_randomkeys(_Seqn, 0, Acc, _BucketLow, _BucketHigh) ->
    Acc;
generate_randomkeys(Seqn, Count, Acc, BucketLow, BRange) ->
    BNumber = string:right(integer_to_list(BucketLow + random:uniform(BRange)),
                                            4, $0),
    KNumber = string:right(integer_to_list(random:uniform(1000)), 4, $0),
    {K, V} = {{o, "Bucket" ++ BNumber, "Key" ++ KNumber, null},
                {Seqn, {active, infinity}, null}},
    generate_randomkeys(Seqn + 1,
                        Count - 1,
                        [{K, V}|Acc],
                        BucketLow,
                        BRange).


compare_method_test() ->
    R = lists:foldl(fun(_X, {LedgerSQN, L0Size, L0TreeList}) ->
                            LM1 = generate_randomkeys(LedgerSQN + 1,
                                                        2000, 1, 500),
                            add_to_cache(L0Size,
                                            {LM1,
                                                LedgerSQN + 1,
                                                LedgerSQN + 2000},
                                            LedgerSQN,
                                            L0TreeList)
                            end,
                        {0, 0, []},
                        lists:seq(1, 16)),
    
    {SQN, Size, TreeList} = R,
    ?assertMatch(32000, SQN),
    ?assertMatch(true, Size =< 32000),
    
    TestList = leveled_tree:to_list(generate_randomkeys(1, 2000, 1, 800)),

    FindKeyFun =
        fun(Key) ->
                fun(Tree, {Found, KV}) ->
                    case Found of
                        true ->
                            {true, KV};
                        false ->
                            L0 = leveled_tree:match(Key, Tree),
                            case L0 of
                                none ->
                                    {false, not_found};
                                {value, Value} ->
                                    {true, {Key, Value}}
                            end
                    end
                end
            end,
    
    S0 = lists:foldl(fun({Key, _V}, Acc) ->
                            R0 = lists:foldr(FindKeyFun(Key),
                                                {false, not_found},
                                                TreeList),
                            [R0|Acc] end,
                        [],
                        TestList),
    
    PosList = lists:seq(1, length(TreeList)),
    S1 = lists:foldl(fun({Key, _V}, Acc) ->
                            R0 = check_levelzero(Key, PosList, TreeList),
                            [R0|Acc]
                            end,
                        [],
                        TestList),
    
    ?assertMatch(S0, S1),
    
    StartKey = {o, "Bucket0100", null, null},
    EndKey = {o, "Bucket0200", null, null},
    
    SWa = os:timestamp(),
    FetchFun = fun(Slot) -> lists:nth(Slot, TreeList) end,
    FoldSlotFun =
        fun(Slot, Acc) ->
            T = FetchFun(Slot),
            lists:ukeymerge(1, leveled_tree:to_list(T), Acc)
        end,
    
    DumpList = lists:foldl(FoldSlotFun, [], lists:seq(1, length(TreeList))),
    
    Q0 = lists:foldl(fun({K, V}, Acc) ->
                            P = leveled_codec:endkey_passed(EndKey, K),
                            case {K, P} of
                                {K, false} when K >= StartKey ->
                                    [{K, V}|Acc];
                                _ ->
                                    Acc
                            end
                            end,
                        [],
                        DumpList),
    Tree = leveled_tree:from_orderedlist(lists:ukeysort(1, Q0), ?CACHE_TYPE),
    Sz0 = leveled_tree:tsize(Tree),
    io:format("Crude method took ~w microseconds resulting in tree of " ++
                    "size ~w~n",
                [timer:now_diff(os:timestamp(), SWa), Sz0]),
    SWb = os:timestamp(),
    Q1 = merge_trees(StartKey, EndKey, TreeList, leveled_tree:empty(?CACHE_TYPE)),
    Sz1 = length(Q1),
    io:format("Merge method took ~w microseconds resulting in tree of " ++
                    "size ~w~n",
                [timer:now_diff(os:timestamp(), SWb), Sz1]),
    ?assertMatch(Sz0, Sz1).

with_index_test() ->
    IndexPrepareFun =
        fun({K, _V}, Acc) ->
            H = leveled_codec:magic_hash(K),
            prepare_for_index(Acc, H)
        end,
    LoadFun =
        fun(_X, {{LedgerSQN, L0Size, L0TreeList}, L0Idx, SrcList}) ->
            LM1 = generate_randomkeys_aslist(LedgerSQN + 1, 2000, 1, 500),
            LM1Array = lists:foldl(IndexPrepareFun, new_index(), LM1),
            LM1SL = leveled_tree:from_orderedlist(lists:ukeysort(1, LM1), ?CACHE_TYPE),
            UpdL0Index = add_to_index(LM1Array, L0Idx, length(L0TreeList) + 1),
            R = add_to_cache(L0Size,
                                {LM1SL, LedgerSQN + 1, LedgerSQN + 2000},
                                LedgerSQN,
                                L0TreeList),
            {R, UpdL0Index, lists:ukeymerge(1, LM1, SrcList)}
        end,
    
    R0 = lists:foldl(LoadFun, {{0, 0, []}, new_index(), []}, lists:seq(1, 16)),
    
    {{SQN, Size, TreeList}, L0Index, SrcKVL} = R0,
    ?assertMatch(32000, SQN),
    ?assertMatch(true, Size =< 32000),

    CheckFun =
        fun({K, V}, {L0Idx, L0Cache}) ->
            H = leveled_codec:magic_hash(K),
            PosList = check_index(H, L0Idx),
            ?assertMatch({true, {K, V}},
                            check_slotlist(K, H, PosList, L0Cache)),
            {L0Idx, L0Cache}
        end,
    
    _R1 = lists:foldl(CheckFun, {L0Index, TreeList}, SrcKVL).

test_genserver_setup() ->
    SK = {o, "Bucket0020", null, null},
    EK = {o, "Bucket0049", null, null},
    PseudoEK = {o, "Bucket0049", "Key9999", null},
    PredFun =
        fun({K, _V}) ->
            PostSK = K > SK,
            PreEK = K < PseudoEK,
            case {PostSK, PreEK} of
                {true, true} ->
                    true;
                _ ->
                    false
            end
        end,
    KL1 = generate_randomkeys_aslist(1, 2000, 1, 99),
    KL2 = generate_randomkeys_aslist(2001, 2000, 1, 99),
    KL3 = generate_randomkeys_aslist(4001, 2000, 1, 99),
    KL4 = generate_randomkeys_aslist(6001, 2000, 1, 99),
    KL5 = generate_randomkeys_aslist(6001, 2000, 1, 99),

    {SK, EK, PredFun, KL1, KL2, KL3, KL4, KL5}.

get_cloneswaiting(PM) ->
    gen_server:call(PM, clones_waiting).

pmemgenserver_basiccopied_test() ->
    {SK, EK, PredFun, KL1, KL2, _KL3, _KL4, _KL5} = test_genserver_setup(),
    {ok, PM1} = pmem_start(),
    ok = pmem_pushtomerge(PM1, leveled_tree:from_orderedlist(KL1, tree, 32)),
    LC1 = leveled_tree:from_orderedlist(lists:sublist(KL2, 1000), tree, 32),
    ok = pmem_clonerequest(PM1, clone1, LC1),
    ok = pmem_pushtomerge(PM1, leveled_tree:from_orderedlist(KL2, tree, 32)),
    
    ExpectedClone1KL = lists:ukeymerge(1, lists:sublist(KL2, 1000), KL1),
    {Clone1R, _NotClone1R} = lists:partition(PredFun, ExpectedClone1KL),
    
    % Will query as copied clone
    Test1R = pmem_queryrequest(PM1, clone1, {SK, EK}),
    ?assertMatch(Clone1R, Test1R),
    
    ok = pmem_close(PM1).

pmemgenserver_basiccurrent_test() ->
    {SK, EK, PredFun, KL1, KL2, _KL3, _KL4, _KL5} = test_genserver_setup(),
    {ok, PM1} = pmem_start(),
    ok = pmem_pushtomerge(PM1, leveled_tree:from_orderedlist(KL1, tree, 32)),
    LC1 = leveled_tree:from_orderedlist(lists:sublist(KL2, 1000), tree, 32),
    ok = pmem_clonerequest(PM1, clone1, LC1),
    
    ExpectedClone1KL = lists:ukeymerge(1, lists:sublist(KL2, 1000), KL1),
    {Clone1R, _NotClone1R} = lists:partition(PredFun, ExpectedClone1KL),
    
    % Will query as current clone
    Test1R = pmem_queryrequest(PM1, clone1, {SK, EK}),
    ?assertMatch(Clone1R, Test1R),
    
    ok = pmem_close(PM1).

pmemgenserver_multiclone_test() ->
    {SK, EK, PredFun, KL1, KL2, KL3, KL4, KL5} = test_genserver_setup(),
    {ok, PM1} = pmem_start(),
    ok = pmem_pushtomerge(PM1, leveled_tree:from_orderedlist(KL1, tree, 32)),
    LC1 = leveled_tree:from_orderedlist(lists:sublist(KL2, 1000), tree, 32),
    ok = pmem_clonerequest(PM1, clone1, LC1),
    LC2 = leveled_tree:from_orderedlist(lists:sublist(KL2, 1800), tree, 32),
    ok = pmem_clonerequest(PM1, clone2, LC2),
    ok = pmem_pushtomerge(PM1, leveled_tree:from_orderedlist(KL2, tree, 32)),
    LC3 = leveled_tree:from_orderedlist(lists:sublist(KL3, 1000), tree, 32),
    ok = pmem_clonerequest(PM1, clone3, LC3),
    ?assertMatch({2, 1}, get_cloneswaiting(PM1)),
    ok = pmem_pushtomerge(PM1, leveled_tree:from_orderedlist(KL3, tree, 32)),
    ok = pmem_pushtomerge(PM1, leveled_tree:from_orderedlist(KL4, tree, 32)),
    
    ExpectedClone1KL = lists:ukeymerge(1, lists:sublist(KL2, 1000), KL1),
    {Clone1R, _NotClone1R} = lists:partition(PredFun, ExpectedClone1KL),
    ExpectedClone2KL = lists:ukeymerge(1, lists:sublist(KL2, 1800), KL1),
    {Clone2R, _NotClone2R} = lists:partition(PredFun, ExpectedClone2KL),
    ExpectedClone3KL = lists:ukeymerge(1,
                                        lists:sublist(KL3, 1000),
                                        lists:ukeymerge(1, KL2, KL1)),
    {Clone3R, _NotClone3R} = lists:partition(PredFun, ExpectedClone3KL),
    
    Test3R = pmem_queryrequest(PM1, clone3, {SK, EK}),
    Test1R = pmem_queryrequest(PM1, clone1, {SK, EK}),
    Test2R = pmem_queryrequest(PM1, clone2, {SK, EK}),
    
    ?assertMatch(Clone3R, Test3R),
    ?assertMatch(Clone1R, Test1R),
    ?assertMatch(Clone2R, Test2R),
    
    ok = pmem_pushtomerge(PM1, leveled_tree:from_orderedlist(KL5, tree, 32)),
    ?assertMatch({0, 0}, get_cloneswaiting(PM1)),
    
    ok = pmem_close(PM1).

pmemgenserver_queryafterclose_test() ->
    {SK, EK, PredFun, KL1, KL2, KL3, KL4, KL5} = test_genserver_setup(),
    {ok, PM1} = pmem_start(),
    ok = pmem_pushtomerge(PM1, leveled_tree:from_orderedlist(KL1, tree, 32)),
    LC1 = leveled_tree:from_orderedlist(lists:sublist(KL2, 1000), tree, 32),
    ok = pmem_clonerequest(PM1, clone1, LC1),
    ok = pmem_pushtomerge(PM1, leveled_tree:from_orderedlist(KL2, tree, 32)),
    ok = pmem_pushtomerge(PM1, leveled_tree:from_orderedlist(KL3, tree, 32)),
    ok = pmem_pushtomerge(PM1, leveled_tree:from_orderedlist(KL4, tree, 32)),
    ok = pmem_pushtomerge(PM1, leveled_tree:from_orderedlist(KL5, tree, 32)),
    TotalList = pmem_fetchandclose(PM1),
    ExpectedTotalList = 
        lists:ukeymerge(1, KL5,
                lists:ukeymerge(1, KL4,
                        lists:ukeymerge(1, KL3,
                                lists:ukeymerge(1, KL2, KL1)))),
    ?assertMatch(ExpectedTotalList, TotalList),
    
    ExpectedClone1KL = lists:ukeymerge(1, lists:sublist(KL2, 1000), KL1),
    {Clone1R, _NotClone1R} = lists:partition(PredFun, ExpectedClone1KL),
    Test1R = pmem_queryrequest(PM1, clone1, {SK, EK}),
    ?assertMatch(Clone1R, Test1R),
    
    ok = pmem_close(PM1).

coverage_cheat_test() ->
    % {noreply, _State0} = handle_info(timeout, #state{}),
    {ok, _State1} = code_change(null, #state{}, null).


-endif.