%% -------- Active Journal writer NIF wrapper ---------
%%
%% Module to provide the equivalent to the subset of functionality required
%% in the file module by the CDB active journal writer.  Within this module 
%% This subset will be provided via the nifile nif.
%%
%% This is a test with regards to write throughput in Erlang, and an issue 
%% with the lack of o_sync support in OTP 16.  In tests writes in the CDB 
%% active journal appear to have been delayed behind background write activity,
%% without there being expected bisk busyness and write queueing in hardware.
%%
%% Speculation is that there is some logical queueing in the Erlang virtual 
%% machine. 
%%
%% However, it is known that Erlang nifs can cause schedule problems, 
%% espcecially where nif requests may take > 1ms.  When writing and reading
%% from the active journal such delays will be common place.  The expected 
%% mitigation for this is to:
%% - Use this NIF in this process (the active journal writer), and this process
%% only;
%% - Add the bitcask bump reductions logic into these commands
%%



-module(leveled_file).


%% All requests have an extra arity to receive a module atom as well as standard 
%% inputs.  The atom 'native' be passed if the nifs are not to be used (e.g. on 
%% windows or in eunit tests).  The atom 'external' should be passed to use the 
%% nifs

-export([open/3,
			close/1,
			position/2,
			pread/3,
			pwrite/3,
			read/2,
			write/2,
			truncate/1,
			advise/4,
			datasync/1]).


open(native, Filename, Modes) ->
	{ok, FD} = file:open(Filename, Modes),
	{ok, {native, FD}};
open(external, Filename, Modes) ->
	% Ignore modes other than o_sysnc, NIF will always open as read write
	% don't use the NIFS in any read-only scenarios
	CreateMode = 
		case filelib:is_file(Filename) of 
			true ->
				[];
			false ->
				[create]
		end,
	IntModes = 
		case lists:member(o_sync, Modes) of 
			true ->
				CreateMode ++ [o_sync];
			false ->
				CreateMode
		end,

	{ok, FD} = bitcask_nifs:file_open(Filename, IntModes),
	{ok, {external, FD}}.


close({native, FD}) ->
	file:close(FD);
close({external, FD}) -> 
	bitcask_nifs:file_close(FD).

position({native, FD}, Position) ->
	file:position(FD, Position);
position({external, FD}, Position) ->
	bitcask_nifs:file_position(FD, Position).

pread({native, FD}, Position, Size) ->
	file:pread(FD, Position, Size);
pread({external, FD}, Position, Size) ->
	bitcask_nifs:file_pread(FD, Position, Size).

pwrite({native, FD}, Position, Bytes) ->
	file:pwrite(FD, Position, Bytes);
pwrite({external, FD}, Position, Bytes) ->
	bitcask_nifs:file_pwrite(FD, Position, Bytes).

read({native, FD}, Bytes) ->
	file:read(FD, Bytes);
read({external, FD}, Bytes) ->
	bitcask_nifs:file_read(FD, Bytes).

write({native, FD}, Bytes) ->
	file:write(FD, Bytes);
write({external, FD}, Bytes) ->
	bitcask_nifs:file_write(FD, Bytes).

%% these functions only to be called with native file.
%% The active writer may use truncate at startup - but at this stage it will be 
%% using the native erlang file module, it switches to an external file module
%% before entering the writer state.
truncate({native, FD}) ->
	file:truncate(FD).

advise({native, FD}, Position, Offset, Advice) ->
	file:advise(FD, Position, Offset, Advice).

datasync({native, FD}) ->
	file:datasync(FD).



