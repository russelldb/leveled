
-module(leveled_manifest).

-include("include/leveled.hrl").

-export([
            findfile/3,
            get_level/2,
            get_range/3,
            update_level/3,
            to_list/1
        ]).      

-include_lib("eunit/include/eunit.hrl").

-define(MAX_LEVELS, 8).

%%%============================================================================
%%% API
%%%============================================================================

findfile(Key, Level, Manifest) ->
    LevelManifest = get_level(Level, Manifest),
    FoldFun =
        fun(File, Acc) ->
            case Acc of
                not_present when
                        Key >= File#manifest_entry.start_key,
                        File#manifest_entry.end_key >= Key ->
                    File#manifest_entry.owner;
                FoundDetails ->
                    FoundDetails
            end
        end,
    lists:foldl(FoldFun, not_present, LevelManifest).


get_level(Level, Manifest) ->
    case lists:keysearch(Level, 1, Manifest) of
        {value, {Level, Value}} ->
            Value;
        false ->
            []
    end.

get_range(StartRangeKey, EndRangeKey, ManifestEntries) ->
    InRangeFun  =
        fun(Ref) ->
            case {Ref#manifest_entry.start_key,
                    Ref#manifest_entry.end_key} of
                    {_, EK} when StartRangeKey > EK ->
                        false;
                    {SK, _} ->
                        case leveled_codec:endkey_passed(EndRangeKey, SK) of
                            true ->
                                false;
                            _ ->
                                true
                        end     
            end
        end,
    lists:partition(InRangeFun, ManifestEntries).

to_list(LevelList) ->
    LevelList.

update_level(List, Level, Manifest) ->
    lists:keystore(Level, 1, Manifest, {Level, List}).

print_manifest(Manifest) ->
    lists:foreach(fun(L) ->
                        leveled_log:log("P0022", [L]),
                        Level = leveled_manifest:get_level(L, Manifest),
                        lists:foreach(fun print_manifest_entry/1, Level)
                        end,
                    lists:seq(0, ?MAX_LEVELS - 1)),
    ok.

print_manifest_entry(Entry) ->
    {S1, S2, S3} = leveled_codec:print_key(Entry#manifest_entry.start_key),
    {E1, E2, E3} = leveled_codec:print_key(Entry#manifest_entry.end_key),
    leveled_log:log("P0023",
                    [S1, S2, S3, E1, E2, E3, Entry#manifest_entry.filename]).


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

print_manifest_test() ->
    M1 = #manifest_entry{start_key={i, "Bucket1", {<<"Idx1">>, "Fld1"}, "K8"},
                                end_key={i, 4565, {"Idx1", "Fld9"}, "K93"},
                                filename="Z1"},
    M2 = #manifest_entry{start_key={i, self(), {null, "Fld1"}, "K8"},
                                end_key={i, <<200:32/integer>>, {"Idx1", "Fld9"}, "K93"},
                                filename="Z1"},
    M3 = #manifest_entry{start_key={?STD_TAG, self(), {null, "Fld1"}, "K8"},
                                end_key={?RIAK_TAG, <<200:32/integer>>, {"Idx1", "Fld9"}, "K93"},
                                filename="Z1"},
    print_manifest([{1, [M1, M2, M3]}]).

-endif.