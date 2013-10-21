-module(dderloci).

-include_lib("imem/include/imem_sql.hrl").
-include_lib("erloci/src/oci.hrl").

%% API
-export([
    exec/2,
    inject_rowid/1,
    edatetime_to_ora/1,
    prepare_stmt/4,
    execute_stmt/4
]).

inject_rowid(Sql) ->
    {ok,{[{PT,_}],_}} = sqlparse:parsetree(Sql),
    {NewSql, _NewPT, TableName} = case PT of
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
            {list_to_binary(sqlparse:fold(NPT)), NPT, FirstTable};
        _ -> {Sql, PT, <<"">>}
    end,
%io:format(user, "~n________________________~nSQL ~p~n", [NewSql]),
%io:format(user, "Old SQL ~p~n", [Sql]),
%io:format(user, "Old parse tree ~p~n", [PT]),
%io:format(user, "New parse tree ~p~n________________________~n", [_NewPT]),
    {TableName, NewSql}.

exec({oci_port, _, _} = Connection, Sql) ->
    %% For now only the first table is counted.
    {TableName, NewSql} = inject_rowid(Sql),
    Statement = Connection:prep_sql(NewSql),
    case Statement:exec_stmt() of
        {ok, Clms} ->
            % ROWID is hidden from columns
            io:format("The original columns, ~p~n", [Clms]),
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
            , TableName};
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
                    , TableName};
                Error ->
                    Statement1:close(),
                    Error
            end
    end.

-spec prepare_stmt(upd | ins, tuple(), list(), binary()) -> {ok, term()} | {error, binary()}.
prepare_stmt(ins, {oci_port, _, _} = Connection, Columns, TableName) ->
    io:format("The column info ~p~n", [Columns]),
    UpdColumns = ["(", create_ins_columns(Columns), ")"],
    Sql = iolist_to_binary([<<"insert into ">>, TableName, " ", UpdColumns, " values ", "(", create_ins_vars(Columns), ")"]),
    io:format("The insert sql ~p~n", [Sql]),
    Stmt = Connection:prep_sql(Sql),
    BindTypes = create_bind_types(Columns),
    io:format("The bind types: ~p~n", [BindTypes]),
    ResBind = Stmt:bind_vars(BindTypes),
    io:format("The bind result ~p~n", [ResBind]),
    {ok, Stmt};
prepare_stmt(upd, {oci_port, _, _} = Connection, Columns, TableName) ->
    Sql = iolist_to_binary([<<"update ">>, TableName, " set ", create_upd_vars(Columns), " where ", TableName, ".ROWID = :CASCUE"]),
    io:format("The update sql ~p~n", [Sql]),
    Stmt = Connection:prep_sql(Sql),
    BindTypes = [{<<":CASCUE">>, 'SQLT_STR'} | create_bind_types(Columns)],
    io:format("The bind types: ~p~n", [BindTypes]),
    ResBind = Stmt:bind_vars(BindTypes),
    io:format("The bind result ~p~n", [ResBind]),
    {ok, Stmt}.

execute_stmt(upd, PrepStmt, ChangeList, Columns) ->
    RowsToUpdate = [list_to_tuple(create_bind_vals([lists:last(lists:nth(3, Row)) | lists:nthtail(3, Row)], [#stmtCol{type = 'SQLT_STR'} | Columns])) || Row <- ChangeList],
    io:format("The rows to update ~p", [RowsToUpdate]),
    PrepStmt:exec_stmt(RowsToUpdate);
execute_stmt(ins, PrepStmt, ChangeList, Columns) ->
    RowsToInsert = [list_to_tuple(create_bind_vals(lists:nthtail(3, Row), Columns)) || Row <- ChangeList],
    io:format("The rows to insert ~p", [RowsToInsert]),
    PrepStmt:exec_stmt(RowsToInsert).

cols_to_rec([]) -> [];
cols_to_rec([{Alias,Type,Len}|Rest]) ->
    io:format("row received: ~p type ~p~n", [Alias, Type]),
    [#stmtCol{ tag = Alias
             , alias = Alias
             , type = Type
             , len = Len
             , prec = undefined
             , readonly = false} | cols_to_rec(Rest)].

translate_datatype(Row, Cols) ->
    [case C#stmtCol.type of
        'SQLT_DAT' ->
            << Century:8, Year:8, Month:8, Day:8, Hour:8, Minute:8, Second:8 >> = R,
            list_to_binary(io_lib:format("~2..0B-~2..0B-~2..0B~2..0B ~2..0B:~2..0B:~2..0B", [Day,Month,Century-100,Year-100,Hour-1,Minute-1,Second-1]));
        _ -> R
    end
    || {C,R} <- lists:zip(Cols, Row)].

-spec dderltime_to_ora(binary()) -> binary().
dderltime_to_ora(DDerlTime) ->
    <<DBin:2/binary, $-, MBin:2/binary, $-, YBin:4/binary, 32, HBin:2/binary, $:, MinBin:2/binary, $:, SecBin:2/binary>> = DDerlTime,
    Century = (binary_to_integer(YBin) div 100) + 100,
    Year = (binary_to_integer(YBin) rem 100) + 100,
    Month = binary_to_integer(MBin),
    Day = binary_to_integer(DBin),
    Hour = binary_to_integer(HBin) + 1,
    Minute = binary_to_integer(MinBin) + 1,
    Second = binary_to_integer(SecBin) + 1,
    <<Century, Year, Month, Day, Hour, Minute, Second>>.

edatetime_to_ora({Meg,Mcr,Mil} = Now)
    when is_integer(Meg)
    andalso is_integer(Mcr)
    andalso is_integer(Mil) ->
    edatetime_to_ora(calendar:now_to_datetime(Now));
edatetime_to_ora({{FullYear,Month,Day},{Hour,Minute,Second}}) ->
    Century = (FullYear div 100) + 100,
    Year = (FullYear rem 100) + 100,
    << Century:8, Year:8, Month:8, Day:8, Hour:8, Minute:8, Second:8 >>.

create_bind_types([]) -> [];
create_bind_types([#stmtCol{} = Col | Rest]) ->
    [{iolist_to_binary([":", Col#stmtCol.tag]), bind_types_map(Col#stmtCol.type)} | create_bind_types(Rest)].

create_ins_columns([#stmtCol{} = Col]) -> [Col#stmtCol.tag];
create_ins_columns([#stmtCol{} = Col | Rest]) -> [Col#stmtCol.tag, ", ", create_ins_columns(Rest)].

create_ins_vars([#stmtCol{} = Col]) -> [":", Col#stmtCol.tag];
create_ins_vars([#stmtCol{} = Col | Rest]) -> [":", Col#stmtCol.tag, ", ", create_ins_vars(Rest)].

create_upd_vars([#stmtCol{} = Col]) -> [Col#stmtCol.tag, "= :", Col#stmtCol.tag];
create_upd_vars([#stmtCol{} = Col | Rest]) -> [Col#stmtCol.tag, "= :", Col#stmtCol.tag, ", ", create_upd_vars(Rest)].

create_bind_vals([], _Cols) -> [];
create_bind_vals([Value | Rest], [Col | RestCols]) ->
    case Col#stmtCol.type of
        'SQLT_DAT' ->
            [dderltime_to_ora(Value) | create_bind_vals(Rest, RestCols)];
        _ ->
            [Value | create_bind_vals(Rest, RestCols)]
    end.

bind_types_map('SQLT_NUM') -> 'SQLT_STR';
bind_types_map('SQLT_INT') -> 'SQLT_STR';
bind_types_map('SQLT_FLT') -> 'SQLT_STR';
bind_types_map(Type) -> Type.
