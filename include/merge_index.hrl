%%%===================================================================
%%% Types
%%%===================================================================

-type(index() :: any()).
-type(field() :: any()).
-type(mi_term() :: any()).
-type(size() :: all | integer()).
-type(posting() :: {Index::index(),
                    Field::field(),
                    Term::mi_term(),
                    Value::any(),
                    Props::[proplists:property()],
                    Timestamp::integer()}).
-type(iterator() :: fun(() -> {any(), iterator()}
                                  | eof
                                  | {error, Reason::any()})).

-type(mi_ift() :: {index(), field(), mi_term()}).
-type(mi_instance() :: atom()).

%%%===================================================================
%%% Records
%%%===================================================================

-record(segment,{root,
                 offsets_table,
                 size}).
