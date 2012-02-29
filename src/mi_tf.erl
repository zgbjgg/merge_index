%% @doc A term-frequency index.
-module(mi_tf).
-export([delta/3,
         free/1,
         get/2,
         new/1]).
-include("merge_index.hrl").

%% @doc Change the `Term' frequency by `Delta'.
-spec delta(mi_instance(), mi_term(), integer()) -> NewCount::pos_integer().

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
-spec get(mi_instance(), mi_term()) -> Frequency::pos_integer().

get(Instance, Term) ->
    [{Term,Frequency}] = ets:lookup(Instance, Term),
    Frequency.

%% @doc Create a new term-frequency index under the given `Instance'
%% name.
-spec new(mi_instance()) -> ok.

new(Instance) ->
    ets:new(Instance, [named_table, {read_concurrency, true}]),
    Instance.
