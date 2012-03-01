%% @doc A term-frequency index.
-module(mi_tf).
-export([delta/3,
         free/1,
         get/2,
         load_file/2,
         new/1,
         temp/1,
         temp_close/1,
         temp_delta/3,
         temp_flush/2,
         temp_size/1]).
-include("merge_index.hrl").
-define(TF_BUFFER_SIZE, 100).
-define(PREAMBLE, "mi_tf").
-define(VERSION, 1).
-define(OPTIONS, 0).
-define(PREAMBLE_LEN, 8).

-opaque mi_tf_temp() :: {mi_tf_temp, dict(), file:fd()}.
-export_type([mi_tf_temp/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-undef(TF_BUFFER_SIZE).
-define(TF_BUFFER_SIZE, 10).
-endif.

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Change the `Term' frequency by `Delta'.
-spec delta(mi_instance(), mi_ift(), integer()) -> NewCount::pos_integer().

delta(Instance, Term, Delta) ->
    case ets:member(Instance, Term) of
        true ->
            ets:update_counter(Instance, Term, Delta);
        false ->
            Val = 0 + Delta,
            ets:insert(Instance, {Term, Val}),
            Val
    end.

%% @doc Free the resources used by the term-frequency index under the
%% given `Instance'.
-spec free(mi_instance()) -> ok.

free(Instance) ->
    ets:delete(Instance),
    ok.

%% @doc Get the `Frequency' of the `Term' for a given `Instance'.
-spec get(mi_instance(), mi_ift()) -> Frequency::pos_integer().

get(Instance, Term) ->
    [{Term,Frequency}] = ets:lookup(Instance, Term),
    Frequency.

%% @doc Load term-frequency entries from `Filename' and update
%% `Instance'.
-spec load_file(mi_instance(), file:filename()) -> ok.

load_file(Instance, Filename) ->
    {ok, File} = file:open(Filename, [read, raw, read_ahead, binary]),
    {_Vsn, _Opts} = read_preamble(File),
    ok = load_entries(Instance, File).

%% @doc Create a new term-frequency index under the given `Instance'
%% name.
-spec new(mi_instance()) -> ok.

new(Instance) ->
    ets:new(Instance, [named_table, {read_concurrency, true}]),
    Instance.

%% @doc Create a temporary term-frequency structure.  This is used for
%% merging term-frequency indxes.
-spec temp(file:filename()) -> mi_tf_temp().

temp(Name) ->
    %% TODO Should utf8 be specified if writing binaries? I notice
    %% it's not specified for segment files.
    {ok, File} = file:open(Name, [write, raw]),
    write_preamble(File),
    {mi_tf_temp, dict:new(), File}.

%% @doc Close the `Temp' index and flush to disk.
-spec temp_close(mi_tf_temp()) -> ok.

temp_close({mi_tf_temp, _, File}=Temp) ->
    {flush,_} = temp_flush(Temp, true),
    file:sync(File),
    ok = file:close(File).

%% @doc Update the `Term' frequency by `Delta'.
-spec temp_delta(mi_tf_temp(), mi_ift(), integer()) -> mi_tf_temp().

temp_delta({mi_tf_temp, Temp, _File}, Term, Delta) ->
    {mi_tf_temp, dict:update_counter(Term, Delta, Temp), _File}.

%% @doc Possibly flush temporary data in `Temp' to `File'.
-spec temp_flush(mi_tf_temp(), boolean()) ->
                        {flush, mi_tf_temp()} | {no_flush, mi_tf_temp()}.

temp_flush({mi_tf_temp, Temp, File}=T, false) ->
    Size = dict:size(Temp),
    if Size >= ?TF_BUFFER_SIZE ->
            write(Temp, File),
            {flush, {mi_tf_temp, dict:new(), File}};
       true ->
            {no_flush, T}
    end;
temp_flush({mi_tf_temp, Temp, File}, true) ->
    write(Temp, File),
    {flush, {mi_tf_temp, dict:new(), File}}.

%% @doc Return the `Size' of the temp term-frequency index.
-spec temp_size(mi_tf_temp()) -> Size::non_neg_integer().

temp_size({mi_tf_temp, Temp, _}) ->
    dict:size(Temp).

%%%===================================================================
%%% Private
%%%===================================================================

read_preamble(File) ->
    {ok, Data} = file:read(File, ?PREAMBLE_LEN),
    <<?PREAMBLE,Vsn:8/integer-unsigned,Opts:16/integer-unsigned>> = Data,
    {Vsn, Opts}.

write(Temp, File) ->
    dict:fold(write_entry(File), ok, Temp).

write_entry(File) ->
    fun(Term, Frequency, ok) ->
            Bin = make_entry(Term, Frequency),
            io:format("File: ~p, K: ~p V: ~p, Bin: ~p~n", [File, Term, Frequency, Bin]),
            ok = file:write(File, Bin)
    end.

make_entry(Term, Frequency) when is_binary(Term) ->
    %% 16-bit length for terms is probably overkill
    Len = size(Term) + 8,
    [<<Len:16/integer,Term/binary,Frequency:64/integer>>].

load_entries(Instance, File) ->
    case file:read(File, 2) of
        {ok, <<Len:16/integer-unsigned>>} ->
            ok = load_entry(Instance, File, Len),
            ok = load_entries(Instance, File);
        eof ->
            ok
    end.

load_entry(Instance, File, Len) ->
    TermLen = Len - 8,
    case file:read(File, Len) of
        {ok, <<Term:TermLen/binary,Frequency:64/integer-unsigned>>} ->
            delta(Instance, Term, Frequency);
        eof ->
            {corrupted_tf_file, implement_tf_rebuild}
    end.

write_preamble(File) ->
    Bin = <<?PREAMBLE,?VERSION:8/integer,?OPTIONS:16/integer>>,
    ok = file:write(File, Bin).

%%%===================================================================
%%% Tests
%%%===================================================================

-ifdef(TEST).

mit_tf_temp_test() ->
    T1 = mi_tf:temp("temp_test.tf"),
    T2 = mi_tf:temp_delta(T1, <<"ryan">>, 15),
    ?assertMatch({no_flush, T2}, mi_tf:temp_flush(T2, false)),
    T3 = mi_tf:temp_delta(T2, <<"andy">>, 33),
    T4 = mi_tf:temp_delta(T3, <<"phark">>, 4),
    T5 = mi_tf:temp_delta(T4, <<"andrew">>, 89),
    T6 = mi_tf:temp_delta(T5, <<"joe">>, 6),
    T7 = mi_tf:temp_delta(T6, <<"ian">>, 9),
    ?assertMatch({no_flush, T7}, mi_tf:temp_flush(T7, false)),
    T8 = mi_tf:temp_delta(T7, <<"jared">>, 101),
    T9 = mi_tf:temp_delta(T8, <<"jon">>, 87),
    T10 = mi_tf:temp_delta(T9, <<"dizzy">>, 77),
    ?assertMatch({no_flush, T10}, mi_tf:temp_flush(T10, false)),
    T11 = mi_tf:temp_delta(T10, <<"bryan">>, 66),
    {R1, T12} = mi_tf:temp_flush(T11, false),
    ?assertEqual(flush, R1),
    T13 = mi_tf:temp_delta(T12, <<"scott">>, 50),
    {R2, T14} = mi_tf:temp_flush(T13, true),
    ?assertEqual(flush, R2),
    T15 = mi_tf:temp_delta(T14, <<"sean">>, 39),
    ?assertMatch(ok, mi_tf:temp_close(T15)).
-endif.
