%%%-------------------------------------------------------------------
%%% @author sjanota
%%% @copyright (C) 2015
%%% @doc
%%%
%%% @end
%%% Created : 09. Nov 2015 11:39
%%%-------------------------------------------------------------------
-module(stream).
-author("sjanota").

%% API
-export([
    from_list/1,
    foreach/2,
    map/2,
    filter/2,
    to_list/1,
    reduce/3,
    takewhile/2,
    dropwhile/2,
    to_map/1,
    to_map/3,
    from_reader/1,
    find/2,
    any/2,
    all/2,
    empty/0,
    singleton/1,
    with_head/2,
    flat_map/2,
    concat/2,
    from_stateful_reader/2,
    from_file/1,
    from_file/2,
    next/1]).

-type stream(T) :: fun(() -> {Head :: T | end_of_stream, Tail :: fun(() -> stream(T))}).
-type stream() :: stream(any()).

-export_type([stream/1, stream/0]).

-type predicate(T) :: fun((T) -> boolean()).

-define(DEFAULT_FILE_CHUNK_SIZE, 1024).

%%%-------------------------------------------------------------------
%%% Create
%%%-------------------------------------------------------------------
-spec from_list(list(T)) -> stream(T).
from_list([]) ->
    empty();
from_list([H | T]) ->
    fun() -> {H, from_list(T)} end.

-spec singleton(E) -> stream(E).
singleton(E) ->
    fun() -> {E, empty()} end.

-spec with_head(E, stream(E)) -> stream(E).
with_head(E, Stream) ->
    fun() -> {E, Stream} end.

-spec empty() -> stream().
empty() ->
    fun() -> empty_result() end.

-spec from_reader(fun(() -> T | end_of_stream)) -> stream(T).
from_reader(Reader) ->
    fun() -> {Reader(), from_reader(Reader)} end.

-spec from_stateful_reader(fun((any()) -> {T, NewState :: any()} | end_of_stream), InitState :: any()) -> stream(T).
from_stateful_reader(Reader, InitState) ->
    fun() ->
        case Reader(InitState) of
            {Val, State} -> {Val, from_stateful_reader(Reader, State)};
            end_of_stream -> empty_result()
        end
    end.

-spec concat(stream(), stream()) -> stream().
concat(S1, S2) ->
    fun() ->
        transform(S1,
            fun() -> S2() end,
            fun(H, T) -> {H, concat(T, S2)} end
        )
    end.

%% @doc Create stream from disk file. When stream is consumed content of the file is read in 1024-byte chunks by default.
%% In case of any error a tuple {error, ErrorReason} is returned by stream.
%% If process that started consuming the stream is terminated then stream returns {error, terminated}.
-spec from_file(File :: file:filename()) -> stream(binary() | {error, any()}).
from_file(Filename) when is_list(Filename) ->
    from_file(Filename, ?DEFAULT_FILE_CHUNK_SIZE).

%% @doc Create stream from disk file with stream chunks of given size.
%% When stream is consumed content of the file is read in chunks of ChunkSize-bytes.
%% In case of any error a tuple {error, ErrorReason} is returned by stream.
%% If process that started consuming the stream is terminated then stream returns {error, terminated}.
-spec from_file(File :: file:filename(), ChunkSize :: pos_integer()) -> stream(binary() | {error, any()}).
from_file(Filename, ChunkSize) when is_list(Filename) ->
    stream_from_file(Filename, ChunkSize).

%%%-------------------------------------------------------------------
%%% Operate
%%%-------------------------------------------------------------------
-spec next(stream(T)) -> {T, stream(T)}.
next(Stream) ->
    Stream().

-spec foreach(stream(T), fun((T) -> _)) -> stream(T).
foreach(Stream, Fun) ->
    fun() ->
        transform(Stream, fun(Head, Tail) ->
            Fun(Head),
            {Head, foreach(Tail, Fun)}
        end)
    end.

-spec map(stream(T1), fun((T1) -> T2)) -> stream(T2).
map(Stream, Fun) ->
    fun() ->
        transform(Stream, fun(Head, Tail) -> {Fun(Head), map(Tail, Fun)} end)
    end.

-spec flat_map(stream(T1), fun((T1) -> T2)) -> stream(T2).
flat_map(Stream, Fun) ->
    fun() ->
        transform(Stream, fun(Head, Tail) ->
            [NewHead | Rest] = Fun(Head),
            RestStream = from_list(Rest),
            {NewHead, concat(RestStream, flat_map(Tail, Fun))}
        end)
    end.

-spec filter(stream(T), predicate(T)) -> stream(T).
filter(Stream, Fun) ->
    fun() ->
        fun N(S) ->
            transform(S, fun(H, T) ->
                case Fun(H) of
                    true -> {H, filter(T, Fun)};
                    false -> N(T)
                end
            end)
        end(Stream)
    end.

-spec takewhile(stream(T), predicate(T)) -> stream(T).
takewhile(Stream, Fun) ->
    fun() ->
        transform(Stream, fun(H, T) ->
            case Fun(H) of
                true -> {H, takewhile(T, Fun)};
                false -> empty_result()
            end
        end)
    end.

-spec dropwhile(stream(T), predicate(T)) -> stream(T).
dropwhile(Stream, Fun) ->
    fun() ->
        transform(Stream, fun(H, T) ->
            case Fun(H) of
                true ->
                    Deeper = dropwhile(T, Fun),
                    Deeper();
                false -> {H, T}
            end
        end)
    end.

%%%-------------------------------------------------------------------
%%% Reduce
%%%-------------------------------------------------------------------
-spec reduce(stream(T), InitAcc :: A, fun((El :: T, Acc :: A) -> NewAcc :: A)) -> A.
reduce(Stream, Acc, Combiner) ->
    case Stream() of
        {end_of_stream, _} -> Acc;
        {H, T} ->
            NewAcc = Combiner(H, Acc),
            reduce(T, NewAcc, Combiner)
    end.

-spec to_list(stream(T)) -> list(T).
to_list(Stream) ->
    L = reduce(Stream, [], fun(E, Acc) -> [E | Acc] end),
    lists:reverse(L).

-spec to_map(stream({K,V})) -> #{K => V}.
to_map(Stream) ->
    reduce(Stream, #{}, fun({K, V}, Acc) -> maps:put(K, V, Acc) end).

-spec to_map(stream(T), fun((T) -> Key :: K), fun((T) -> Value :: V)) -> #{K => V}.
to_map(Stream, ToKey, ToValue) ->
    to_map(map(Stream, fun(E) -> {ToKey(E), ToValue(E)} end)).

-spec find(stream(T), predicate(T)) -> {ok, T} | undefined.
find(Stream, Predicate) ->
    transform(Stream, fun() -> undefined end, fun(H, T) ->
        case Predicate(H) of
            true ->
                {ok, H};
            false ->
                find(T, Predicate)
        end
    end).

-spec any(stream(T), predicate(T)) -> boolean().
any(Stream, Predicate) ->
    case find(Stream, Predicate) of
        {ok, _} -> true;
        undefined -> false
    end.

-spec all(stream(T), predicate(T)) -> boolean().
all(Stream, Predicate) ->
    case find(Stream, fun(H) -> not Predicate(H) end) of
        {ok, _} -> false;
        undefined -> true
    end.

%%%-------------------------------------------------------------------
%%% Local
%%%-------------------------------------------------------------------
transform(Stream, OnNonEmpty) ->
    transform(Stream, fun() -> empty_result() end, OnNonEmpty).

transform(Stream, OnEmpty, OnNonEmpty) ->
    case Stream() of
        {end_of_stream, _} -> OnEmpty();
        {H, T} -> OnNonEmpty(H, T)
    end.

empty_result() ->
    {end_of_stream, empty()}.

-spec stream_from_file(File :: file:filename() | file:io_device(), ChunkSize :: pos_integer()) -> stream(binary()).
stream_from_file(Filename, ChunkSize) when is_list(Filename) ->
    fun() ->
        case file:open(Filename, [binary]) of
            {ok, File} ->
                {<<>>, stream_from_file(File, ChunkSize)};
            {error, _} = Err ->
                {Err, fun empty_result/0}
        end
    end;
stream_from_file(File, ChunkSize) when is_pid(File), is_integer(ChunkSize), ChunkSize > 0 ->
    fun() ->
        case file:read(File, ChunkSize) of
            {ok, Bytes} ->
                {Bytes, stream_from_file(File, ChunkSize)};
            {error, _} = Err ->
                ok = file:close(File),
                {Err, fun empty_result/0};
            eof ->
                ok = file:close(File),
                empty_result()
        end
    end.