-module(dderloci).

-include_lib("imem/include/imem_sql.hrl").

%% API
-export([
    exec/2,
    inject_rowid/1
]).

inject_rowid(Sql) ->
    {ok,{[{PT,_}],_}} = sqlparse:parsetree(Sql),
    {NewSql, _NewPT} = case PT of
        {select, Args} ->
            {fields, Flds} = lists:keyfind(fields, 1, Args),
            {from, [FirstTable|_]=Forms} = lists:keyfind(from, 1, Args),
            NewFields =
                [list_to_binary(case FirstTable of
                    {as, _, Alias} -> [Alias, ".ROWID"];
                    Tab -> [Tab, ".ROWID"]
                end) | lists:flatten([case F of
                                        <<"*">> ->
                                        lists:reverse(lists:foldl(
                                            fun(T, AFields) ->
                                                case T of
                                                    {as, _, Alias} -> [list_to_binary([Alias,".*"]) | AFields];
                                                    Tab -> [list_to_binary([Tab,".*"]) | AFields]
                                                end
                                            end,
                                            [],
                                            Forms));
                                        _ -> F
                                     end
                                     || F <- Flds]
                )],
            NewArgs = lists:keyreplace(fields, 1, Args, {fields, NewFields}),
            NPT = {select, NewArgs},
            {list_to_binary(sqlparse:fold(NPT)), NPT};
        _ -> {Sql, PT}
    end,
%io:format(user, "~n________________________~nSQL ~p~n", [NewSql]),
%io:format(user, "Old SQL ~p~n", [Sql]),
%io:format(user, "Old parse tree ~p~n", [PT]),
%io:format(user, "New parse tree ~p~n________________________~n", [_NewPT]),
    NewSql.

exec(Sql, {oci_port, _, _} = Connection) ->
    NewSql = inject_rowid(Sql),
    Statement = Connection:prep_sql(NewSql),    
    case Statement:exec_stmt() of
        {ok, Clms} ->
            % ROWID is hidden from columns
            [_|Columns] = Clms,
            NewClms = cols_to_rec(Columns),
            {ok
            , #stmtResult{ stmtCols = NewClms
                         , rowFun   = fun(Row) ->
                                        [_|NewRow] = lists:reverse(Row),
                                        translate_datatype(NewRow, NewClms)
                                      end
                         , stmtRef  = Statement
                         , sortFun  = fun(R) -> R end
                         , sortSpec = []}
            };
        _ ->
            Statement:close(),
            Statement1 = Connection:prep_sql(Sql),
            case Statement1:exec_stmt() of
                {ok, Clms} ->
                    NewClms = cols_to_rec(Clms),
                    {ok
                    , #stmtResult{ stmtCols = NewClms
                                 , rowFun   = fun(Row) ->
                                                translate_datatype(lists:reverse(Row), NewClms)
                                              end
                                 , stmtRef  = Statement1
                                 , sortFun  = fun(R) -> R end
                                 , sortSpec = []}
                    };
                Error ->
                    Statement1:close(),
                    Error
            end
    end.

cols_to_rec([]) -> [];
cols_to_rec([{Alias,Type,Len}|Rest]) ->
    [#stmtCol{ tag = Alias
             , alias = Alias
             , type = Type
             , len = Len
             , prec = undefined
             , readonly = false} | cols_to_rec(Rest)].

translate_datatype(Row, Cols) ->
    [case C#stmtCol.type of
        date ->
            << Century:8, Year:8, Month:8, Day:8, Hour:8, Minute:8, Second:8 >> = R,
            list_to_binary(io_lib:format("~2..0B-~2..0B-~2..0B~2..0B ~2..0B:~2..0B:~2..0B", [Day,Month,Century-100,Year-100,Hour,Minute,Second]));
        _ -> R
    end
    || {C,R} <- lists:zip(Cols, Row)].
