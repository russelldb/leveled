%% -------------------------------------------------------------------
%%
%% bitcask: Eric Brewer-inspired key/value store
%%
%% Copyright (c) 2010 Basho Technologies, Inc. All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(bitcask_nifs).

-export([init/0,
         file_open/2,
         file_close/1,
         file_pread/3,
         file_pwrite/3,
         file_read/2,
         file_write/2,
         file_position/2]).

-on_load(init/0).


-spec init() ->
        ok | {error, any()}.
init() ->
    case code:priv_dir(eleveleddb) of
        {error, bad_name} ->
            case code:which(?MODULE) of
                Filename when is_list(Filename) ->
                    SoName = filename:join([filename:dirname(Filename),
                                            "../priv", 
                                            "eleveleddb"]);
                _ ->
                    SoName = filename:join("../priv", 
                                            "eleveleddb")
            end;
         Dir ->
            SoName = filename:join(Dir, "eleveleddb")
    end,
    erlang:load_nif(SoName, 0).


%% ===================================================================
%% Internal functions
%% ===================================================================
%%
%% Most of the functions below are actually defined in c_src/bitcask_nifs.c
%% See that file for the real functionality of the bitcask_nifs module.
%% The definitions here are only to satisfy trivial static analysis.
%%



file_open(Filename, Opts) ->
    bitcask_bump:big(),
    file_open_int(Filename, Opts).

file_open_int(_Filename, _Opts) ->
    erlang:nif_error({error, not_loaded}).

file_close(Ref) ->
    bitcask_bump:big(),
    file_close_int(Ref).

file_close_int(_Ref) ->
    erlang:nif_error({error, not_loaded}).

file_pread(Ref, Offset, Size) ->
    bitcask_bump:big(),
    file_pread_int(Ref, Offset, Size).

file_pread_int(_Ref, _Offset, _Size) ->
    erlang:nif_error({error, not_loaded}).

file_pwrite(Ref, Offset, Bytes) ->
    bitcask_bump:big(),
    file_pwrite_int(Ref, Offset, Bytes).

file_pwrite_int(_Ref, _Offset, _Bytes) ->
    erlang:nif_error({error, not_loaded}).

file_read(Ref, Size) ->
    bitcask_bump:big(),
    file_read_int(Ref, Size).

file_read_int(_Ref, _Size) ->
    erlang:nif_error({error, not_loaded}).

file_write(Ref, Bytes) ->
    bitcask_bump:big(),
    file_write_int(Ref, Bytes).

file_write_int(_Ref, _Bytes) ->
    erlang:nif_error({error, not_loaded}).

file_position(Ref, Position) ->
    bitcask_bump:big(),
    file_position_int(Ref, Position).

file_position_int(_Ref, _Position) ->
    erlang:nif_error({error, not_loaded}).


%% ===================================================================
%% EUnit tests
%% ===================================================================
