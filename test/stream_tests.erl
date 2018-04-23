%%%-------------------------------------------------------------------
%%% @author sjanota
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 16. Nov 2015 09:50
%%%-------------------------------------------------------------------
-module(stream_tests).
-author("sjanota").

-include_lib("testutils/include/testing.hrl").

%% API
-export([]).

-ifdef(TEST).

-define(LIST, [1, 2, 3, 4]).
-define(STREAM, stream:from_list(?LIST)).

?TEST_FUN().
foreach_does_not_modify_stream() ->
    Stream1 = stream:foreach(?STREAM, fun(I) -> I * 2 end),
    assert_stream(Stream1, ?LIST).

?TEST_FUN().
map_test() ->
    Stream1 = stream:map(?STREAM, fun(I) -> I * 2 end),
    assert_stream(Stream1, [2, 4, 6, 8]).

?TEST_FUN().
filter_test() ->
    Stream1 = stream:filter(?STREAM, fun(I) -> I rem 2 == 0 end),
    assert_stream(Stream1, [2, 4]).

?TEST_FUN().
takewhile_test() ->
    Stream1 = stream:takewhile(?STREAM, fun(I) -> I < 3 end),
    assert_stream(Stream1, [1, 2]).

?TEST_FUN().
dropwhile_test() ->
    Stream1 = stream:dropwhile(?STREAM, fun(I) -> I =/= 3 end),
    assert_stream(Stream1, [3, 4]).

?TEST_FUN().
reduce_test() ->
    Sum = stream:reduce(?STREAM, 0, fun(I, Acc) -> Acc + I end),
    ?assertEqual(Sum, 10).

?TEST_FUN().
to_map_test() ->
    Map = stream:to_map(?STREAM, fun integer_to_binary/1, fun integer_to_list/1),
    ?assertEqual(#{<<"1">> => "1", <<"2">> => "2", <<"3">> => "3", <<"4">> => "4"}, Map).

?TEST_FUN().
find_test() ->
    Found1 = stream:find(?STREAM, fun(I) -> I rem 3 == 0 end),
    ?assertEqual({ok, 3}, Found1),
    Found2 = stream:find(?STREAM, fun(I) -> I rem 5 == 0 end),
    ?assertEqual(undefined, Found2).

?TEST_FUN().
any_test() ->
    Found1 = stream:any(?STREAM, fun(I) -> I rem 3 == 0 end),
    ?assertEqual(true, Found1),
    Found2 = stream:any(?STREAM, fun(I) -> I rem 5 == 0 end),
    ?assertEqual(false, Found2).

?TEST_FUN().
all_test() ->
    Found1 = stream:all(?STREAM, fun(I) -> I < 3 end),
    ?assertEqual(false, Found1),
    Found2 = stream:all(?STREAM, fun(I) -> I < 5 end),
    ?assertEqual(true, Found2).

?TEST_FUN().
concat_test() ->
    S1 = stream:from_list([1,2]),
    S2 = stream:from_list([3,4]),
    C = stream:concat(S1, S2),
    assert_stream(C, [1,2,3,4]).

?TEST_FUN().
flat_map_test() ->
    S = stream:from_list([1,2]),
    F = fun(E) -> [E, E+2] end,
    Map = stream:flat_map(S, F),
    assert_stream(Map, [1, 3, 2, 4]).

?TEST_FUN().
from_file_test() ->
    Filename = ".stream.test",
    ok = file:write_file(Filename, <<"This is a stream:from_file/1 test">>),
    S = stream:from_file(Filename, 5),
    ok = assert_stream(S, [<<>>, <<"This ">>, <<"is a ">>, <<"strea">>, <<"m:fro">>, <<"m_fil">>, <<"e/1 t">>, <<"est">>]),
    ok = file:delete(Filename).

?TEST_FUN().
from_file_create_stream_in_another_process_test() ->
    Filename = ".stream.test",
    ok = file:write_file(Filename, <<"Create file stream in another process">>),
    TestPID = self(),
    spawn(fun() -> S = stream:from_file(Filename), TestPID ! {stream, S} end),
    S = receive {stream, Stream} -> Stream end,
    ok = assert_stream(S, [<<>>, <<"Create file stream in another process">>]),
    ok = file:delete(Filename).

?TEST_FUN().
from_file_error_test() ->
    Filename = ".stream.test",
    Fd = self(),
    ?MECK(file, [
        {open, {ok, Fd}},
        {close, ok}
    ]),
    ?MECK_LOOP(file, [
        {read, [{ok, <<"Stre">>}, {ok, <<"am er">>}, {error, read_timeout}, {ok, <<"ror">>}]}
    ]),
    S = stream:from_file(Filename, 5),
    ok = assert_stream(S, [<<>>, <<"Stre">>, <<"am er">>, {error, read_timeout}]).

?TEST_FUN().
from_file_open_error_test() ->
    Filename = ".stream.test",
    Fd = self(),
    ?MECK(file, [
        {open, {error, terminated}}
    ]),
    S = stream:from_file(Filename, 5),
    ok = assert_stream(S, [{error, terminated}]).


assert_stream(S, Expected) ->
    L = stream:to_list(S),
    ?assertEqual(Expected, L).

-endif.
