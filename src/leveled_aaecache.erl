%% -------- Overview ---------
%%
%% Cache of a tictac tree for a particular partition.  There should be a 
%% separate process for each partition.  On receiving an add, alter or remove
%% operation the process managing the database can call alter_object to update
%% the cache and return Indexspecs for storage.
%%
%% The tree is split into two levels, the lower level being N lots of N 
%% segments, with the upper (root) level being a binary to summarise each of
%% the n segments.
%%
%% The root is updated in response to a request for an exchange (return_root),
%% and the segments are updated at the time of alter_object.
%%
%% There are two index specs for each change.  A Tree Segment Key and an 
%% Object Segment Key.  The Tree Segment Keys will be used to rebuild the 
%% cache on startup.  The Object SegmentKey cna be used to find keys/clocks 
%% by segment when the store supports secondary indexes, but not the finding 
%% of objects by segment.
%% 
%% 

-module(leveled_aaecache).

-behaviour(gen_server).
-include("include/leveled.hrl").

-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3,
        startup_cache/3,
        alter_object/5,
        return_root/1,
        return_leaf/1
]).


-record(state, 
                {
                    partition :: integer(),
                    tree :: leveled_tictac:tictactree(), 
                    dirty_segments = [] :: list(integer()),
                    rebuild_queue = [] :: list() 
                }).


%%%============================================================================
%%% API
%%%============================================================================

-spec startup_cache(integer(), list(), list()) -> {ok, pid()}.
%% @doc
%% Startup a new cache for each partition, loading a list of Tree Segment Keys 
%% returned from the backend at startup.
%%
%% The caloling process should know in advance what partitions should be 
%% anticipated - in riak by calling riak_kv_util:responsible_preflists/1 - and
%% then start a separate cache for each preflist
startup_cache(Partition, TreeSegmentEntries, Opts) ->
    {ok, Cache} = gen_server:start(?MODULE, [Opts], []),
    R = gen_server:call(Cache, 
                            {load_cache, Partition, TreeSegmentEntries},
                            infinity),
    {R, Cache}.


-spec alter_object(pid(), binary(), list()|none, list()|none, boolean()) 
                                                                    -> list().
%% @doc
%% Upate the AAE cache, and provide addtiional object specifications 
%% given a new object clock and an old clock (or none in the case of a 
%% fresh addition).
%%
%% The ObjectSpecs are intended to be treated like IndexSpecs in that:
%% - they should be transactional with the object change
%% - they should support a sorted fold in key order
%% - they should not be directly fetchable (and so do not need to be 
%% included in backend bloom filters)
%% 
%% However ObjectSpecs differ from IndexSpecs in that:
%% - they have a value, and should be fetched through fold_objects 
%% (by bucket)
%% - the key should not be enriched with the actual object key
%%
%% SegmentObject ObjectSpecs should be of the form {IndexOp, IndexValue}:
%% {add, 
%%      {<<"$objectseg">>, <<SegmentID>>, <<Bkey>>}, 
%%      {<<Version>>, <<AddClock>>}},
%% - if add/alter
%% {rmv, {<<"$objectseg">>, <<SegmentID>>, <<BKey>>}, null}
%% - if delete
%%
%% There should also be a SegmentTree ObjectSpec of 
%% {add, 
%%      {<<"$bucketseg_bin">>, <<SegmentID>>}, 
%%      {<<Varsion>>, <<Hash:128/integer>>}
%%
%% Backend must support object_specs capabaility.  If backend supports the
%% segment_fold capability, then the SegmentObject ObjectSpecs are not 
%% required.   
alter_object(Cache, Key, AddClock, RemoveClock, BackendIsSegReady) ->
    gen_server:call(Cache, 
                    {alter, 
                        Key, AddClock, RemoveClock, 
                        BackendIsSegReady},
                    infinity).

-spec return_root(pid(), integer()) -> {ok, binary()}.
%% @doc
%% Request the root hash of a tree, returning the binary that represents the 
%% top buckets
return_root(Cache) ->
    gen_server:call(Cache, return_root, infinity).


-spec return_leaf(pid(), integer()) -> {ok, binary(), list(integer())}.
%% @doc
%% Returns all the segment hashes for a given partition and bucket, a long 
%% with a list of relevant dirty segments - segments which should not 
%% currently be trusted for comaprisons
return_leaf(Cache, Leaf) ->
    gen_server:call(Cache, {return_leaf, Leaf}, infinity).


-spec cache_close(pid()) -> ok.
%% @doc
%% Clean shutdown.
cache_close(Pid) ->
    gen_server:call(Pid, close, infinity).


%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([_Opts]) ->
    {ok, #state{}}.

handle_call({alter, _Key, _AddClock, _RmvClock, _SegNotReqd}, _From, State) ->
    {reply, ok, State};
handle_call(return_root, _From, State) ->
    {reply, ok, State};
handle_call({return_leaf, _Leaf}, _From, State) ->
    {reply, ok, State};
handle_call(close, _From, State) ->
    {stop, normal, ok, State}.


handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% Internal functions
%%%============================================================================