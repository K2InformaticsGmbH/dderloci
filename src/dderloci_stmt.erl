-module(dderloci_stmt).
-behaviour(gen_server).

-export([prepare/4,
    execute/1]).

%% gen_server callbacks
-export([init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3]).

-include_lib("imem/include/imem_sql.hrl").

-define(InitTimeout, 10000).
-define(NoCommit, 0).
-define(AutoCommit, 1).

-record(stmt, {columns = [],
               del_rows = [],
               upd_rows = [],
               ins_rows = [],
               del_stmt,
               upd_stmt,
               ins_stmt,
               connection}).

-record(row, {index, id, pos, op, values}).

%%% API Implementation

-spec prepare(binary(), list(), tuple(), list()) -> {error, term()} | {ok, pid()}.
prepare(TableName, ChangeList, Connection, Columns) ->
	gen_server:start(?MODULE, [TableName, ChangeList, Connection, Columns], [{timeout, ?InitTimeout}]).

-spec execute(pid()) -> {error, term()} | list().
execute(Pid) ->
	gen_server:call(Pid, execute).

%%% gen_server callbacks

init([TableName, ChangeList, Connection, Columns]) ->
    case create_stmts(TableName, Connection, ChangeList, Columns) of
        {ok, Stmt} -> {ok, Stmt};
        {error, Error} -> {stop, Error}
    end.

handle_call(execute, _From, #stmt{columns = Columns, connection = Connection} = Stmt) ->
    try
        case process_delete(Stmt#stmt.del_stmt, Stmt#stmt.del_rows, Columns) of
            {ok, DeleteChangeList} ->
                case process_update(Stmt#stmt.upd_stmt, Stmt#stmt.upd_rows, Columns) of
                    {ok, UpdateChangeList} ->
                        case process_insert(Stmt#stmt.ins_stmt, Stmt#stmt.ins_rows, Columns) of
                            {ok, InsertChangeList} ->
                                io:format("we are about to commit...~n"),
                                Connection:commit(),
                                {stop, normal, DeleteChangeList ++ UpdateChangeList ++ InsertChangeList, Stmt};
                            Error ->
                                Connection:rollback(),
                                {stop, normal, Error, Stmt}
                        end;
                    Error ->
                        Connection:rollback(),
                        {stop, normal, Error, Stmt}
                end;
            Error ->
                Connection:rollback(),
                {stop, normal, Error, Stmt}
        end
    catch _Class:Error2 ->
            Connection:rollback(),
            {stop, normal, {error, Error2}, Stmt}
    end.

handle_cast(_Ignored, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
	{noreply, State}.

%% TODO: Check if some cleanup is needed.
terminate(_Reason, #stmt{}) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%% Private functions

-spec create_stmts(binary(), tuple(), list(), list()) -> {ok, #stmt{}} | {error, term()}.
create_stmts(TableName, Connection, ChangeList, Columns) ->
    {DeleteList, UpdateListTotal, InsertListTotal} = split_changes(ChangeList),
    UpdateList = split_by_columns_mod(UpdateListTotal, Columns, []),
    InsertList = split_by_non_empty(InsertListTotal, []),
    io:format("The new update list ~n**********~n~p~n**********~n", [UpdateList]),
    io:format("The new insert list ~n**********~n~p~n**********~n", [InsertList]),
    case create_stmts([{del, DeleteList}, {upd, UpdateList}, {ins, InsertList}], TableName, Connection, Columns, []) of
        {ok, Stmt} ->
            {ok, Stmt#stmt{del_rows = DeleteList, upd_rows = UpdateList, ins_rows = InsertList, connection = Connection}};
        Error ->
            Error
    end.

-spec create_stmts([{atom(), list()}], binary(), tuple(), list(), list()) -> {ok, #stmt{}} | {error, term()}.
create_stmts([], _TableName, _Connection, _Columns, []) ->
    {error, <<"empty change list">>};
create_stmts([], _TableName, _Connection, Columns, ResultStmts) ->
    DelStmt = proplists:get_value(del, ResultStmts),
    UpdStmt = proplists:get_value(upd, ResultStmts),
    InsStmt = proplists:get_value(ins, ResultStmts),
    {ok, #stmt{columns = Columns, del_stmt = DelStmt, upd_stmt = UpdStmt, ins_stmt = InsStmt}};
create_stmts([{del, []} | Rest], TableName, Connection, Columns, ResultStmt) ->
    io:format("~n-----> Nothing to delete, continue with the next one <----- ~n"),
    create_stmts(Rest, TableName, Connection, Columns, ResultStmt);
create_stmts([{del, _DelList} | Rest], TableName, Connection, Columns, ResultStmt) ->
    Sql = iolist_to_binary([<<"delete from ">>, TableName, " where ",  TableName, ".ROWID = :IDENTIFIER"]),
    io:format("The delete sql ~p~n", [Sql]),
    case Connection:prep_sql(Sql) of
        {error, {_ErrorCode, ErrorMsg}} ->
            %TODO: add ?Error ...(ErrorCode)...
            close_stmts(ResultStmt),
            {error, ErrorMsg};
        Stmt ->
            BindTypes = [{<<":IDENTIFIER">>, 'SQLT_STR'}],
            io:format("The bind types: ~p~n", [BindTypes]),
            ResBind = Stmt:bind_vars(BindTypes),
            io:format("The bind result ~p~n", [ResBind]),
            create_stmts(Rest, TableName, Connection, Columns, [{del, Stmt} | ResultStmt])
    end;
create_stmts([{upd, []} | Rest], TableName, Connection, Columns, ResultStmt) ->
    io:format("~n-----> Nothing to update, continue with the next one <----- ~n"),
    create_stmts(Rest, TableName, Connection, Columns, ResultStmt);
%[{{1,2,3},
% [{row,{{<<"AAAHNHAABAAALGRAAO">>,<<"Á\"">>,<<>>,<<"Á\v3">>,<<>>,<<128>>},{}},
%       <<"AAAHNHAABAAALGRAAO">>,15,upd,
%       [<<"22">>,<<"0.29">>,<<"12.0">>,<<>>,<<"0">>]}]},
%{{1,3},
% [{row,{{<<"AAAHNHAABAAALGRAAR">>,
%         <<128>>,
%         <<194,23>>,
%         <<"?DFOf">>,
%         <<190,4,34,47>>,
%         <<>>},
%        {}},
%       <<"AAAHNHAABAAALGRAAR">>,18,upd,
%       [<<"12">>,<<"2200">>,<<"9383.393">>,<<"0.0000033346">>,<<>>]},
%  {row,{{<<"AAAHNHAABAAALGRAAT">>,<<"Á\"">>,<<>>,<<128>>,<<>>,<<>>},{}},
%       <<"AAAHNHAABAAALGRAAT">>,20,upd,
%       [<<"44">>,<<>>,<<"8">>,<<>>,<<>>]}]}]
create_stmts([{upd, UpdList} | Rest], TableName, Connection, Columns, ResultStmts) ->
    [{ModifiedCols, _Rows} | RestUpdList] = UpdList,
    FilterColumns = filter_columns(ModifiedCols, Columns),
    UpdVars = create_upd_vars(FilterColumns),
    % TODO: use where part to do optimistic locking.
    % WhereVars = create_where_vars(Columns),
    Sql = iolist_to_binary([<<"update ">>, TableName, " set ", UpdVars, " where ", TableName, ".ROWID = :IDENTIFIER"]),
    io:format("The update sql ~p~n", [Sql]),
    case Connection:prep_sql(Sql) of
        {error, {_ErrorCode, ErrorMsg}} ->
            %TODO: add ?Error ...(ErrorCode)...
            close_stmts(ResultStmts),
            {error, ErrorMsg};
        Stmt ->
            BindTypes = [{<<":IDENTIFIER">>, 'SQLT_STR'} | create_bind_types(FilterColumns)],
            io:format("The bind types: ~p~n", [BindTypes]),
            ResBind = Stmt:bind_vars(BindTypes),
            io:format("The bind result ~p and the stmt ~p~n", [ResBind, Stmt]),
            case proplists:get_value(upd, ResultStmts) of
                undefined ->
                    NewResultStmts = [{upd, [Stmt]} | ResultStmts];
                UpdtStmts ->
                    NewResultStmts = lists:keyreplace(upd, 1, ResultStmts, {upd, UpdtStmts ++ [Stmt]})
            end,
            create_stmts([{upd, RestUpdList} | Rest], TableName, Connection, Columns, NewResultStmts)
    end;
create_stmts([{ins, []} | Rest], TableName, Connection, Columns, ResultStmt) ->
    io:format("~n-----> Nothing to insert, continue with the next one <----- ~n"),
    create_stmts(Rest, TableName, Connection, Columns, ResultStmt);
%[{{1,2,3},[{row,{{}},undefined,2,ins,[<<"3.33">>,<<"22">>,<<"23">>]}]},
%{{1,3},
% [{row,{{}},undefined,3,ins,[<<"4.0">>,<<>>,<<"231">>]},
%  {row,{{}},undefined,4,ins,[<<"87.4">>,<<>>,<<"44">>]}]},
%{{2,3},
% [{row,{{}},undefined,5,ins,[<<>>,<<"12">>,<<"55">>]},
%  {row,{{}},undefined,6,ins,[<<>>,<<"33">>,<<"22">>]}]}]
create_stmts([{ins, InsList} | Rest], TableName, Connection, Columns, ResultStmts) ->
    [{NonEmptyCols, _Rows} | RestInsList] = InsList,
    InsColumns = ["(", create_ins_columns(filter_columns(NonEmptyCols, Columns)), ")"],
    Sql = iolist_to_binary([<<"insert into ">>, TableName, " ", InsColumns, " values ", "(", create_ins_vars(filter_columns(NonEmptyCols, Columns)), ")"]),
    io:format("The insert sql ~p~n", [Sql]),
    case Connection:prep_sql(Sql) of
        {error, {_ErrorCode, ErrorMsg}} ->
            %%TODO: ?Error...
            close_stmts(ResultStmts),
            {error, ErrorMsg};
        Stmt ->
            BindTypes = create_bind_types(filter_columns(NonEmptyCols, Columns)),
            io:format("The bind types: ~p~nThe stmt ~p~n", [BindTypes, Stmt]),
            ResBind = Stmt:bind_vars(BindTypes),
            io:format("The bind result ~p~n", [ResBind]),
            case proplists:get_value(ins, ResultStmts) of
                undefined ->
                    NewResultStmts = [{ins, [Stmt]} | ResultStmts];
                InsStmts ->
                    NewResultStmts = lists:keyreplace(ins, 1, ResultStmts, {ins, InsStmts ++ [Stmt]})
            end,
            create_stmts([{ins, RestInsList} | Rest], TableName, Connection, Columns, NewResultStmts)
    end.

-spec process_delete(term(), list(), list()) -> {ok, list()} | {error, term()}.
process_delete(undefined, [], _Columns) -> {ok, []};
process_delete(PrepStmt, Rows, _Columns) ->
    RowsToDelete = [list_to_tuple(create_bind_vals([Row#row.id], [#stmtCol{type = 'SQLT_STR'}])) || Row <- Rows],
    io:format("The rows to delete ~p", [RowsToDelete]),
    case PrepStmt:exec_stmt(RowsToDelete, ?NoCommit) of
        {rowids, _RowIds} -> %% TODO: Check if the modified rows are the correct.
            ChangedKeys = [{Row#row.pos, {{}}} || Row <- Rows],
            {ok, ChangedKeys};
        {error, {_ErrorCode, ErrorMsg}}->
            {error, ErrorMsg}
    end.

-spec process_update(list(), list(), [#stmtCol{}]) -> {ok, list()} | {error, term()}.
process_update(undefined, [], _Columns) -> {ok, []};
process_update([], [], _Colums) -> {ok, []};
process_update([PrepStmt | RestStmts], [{ModifiedCols, Rows} | RestRows], Columns) ->
    %% TODO: No need to filter the rows for optimistic locking...
    FilterRows = [Row#row{values = filter_columns(ModifiedCols, Row#row.values)} || Row <- Rows],
    case process_one_update(PrepStmt, FilterRows, filter_columns(ModifiedCols, Columns), Rows, Columns) of
        {ok, ChangedKeys} ->
            case process_update(RestStmts, RestRows, Columns) of
               {ok, RestChangedKeys} ->
                    {ok, ChangedKeys ++ RestChangedKeys};
               ErrorRest ->
                    ErrorRest
            end;
        Error ->
            Error
    end.

-spec process_one_update(term(), [#row{}], [#stmtCol{}], [#row{}], [#stmtCol{}]) -> {ok, list()} | {error, term()}.
process_one_update(PrepStmt, FilterRows, FilterColumns, Rows, Columns) ->
    %% TODO: Implement updates using the old values on the where clause, (optimistic locking).
    RowsToUpdate = [list_to_tuple(create_bind_vals([Row#row.id | Row#row.values], [#stmtCol{type = 'SQLT_STR'} | FilterColumns])) || Row <- FilterRows],
    io:format("The rows to update ~p and the stmt ~p~n", [RowsToUpdate, PrepStmt]),
    case PrepStmt:exec_stmt(RowsToUpdate, ?NoCommit) of
        {rowids, RowIds} ->
            case check_rowid(RowIds, Rows) of
                true->
                    ChangedKeys = [{Row#row.pos, {list_to_tuple(create_changedkey_vals([Row#row.id | Row#row.values], [#stmtCol{type = 'SQLT_STR'} | Columns])), {}}} || Row <- Rows],
                    {ok, ChangedKeys};
                false ->
                    io:format("The rowids returned ~p doesn't match the rows to update ~p~n", [RowIds, RowsToUpdate]),
                    %% TODO: What is a good message here ?
                    {error, <<"Error updating the rows.">>}
            end;
        {error, {_ErrorCode, ErrorMsg}}->
            {error, ErrorMsg}
    end.

-spec process_insert(term(), list(), list()) -> {ok, list()} | {error, term()}.
process_insert(undefined, [], _Columns) -> {ok, []};
process_insert([], [], _Columns) -> {ok, []};
process_insert([PrepStmt | RestStmts], [{NonEmptyCols, Rows} | RestRows], Columns) ->
    FilterRows = [Row#row{values = filter_columns(NonEmptyCols, Row#row.values)} || Row <- Rows],
    case process_one_insert(PrepStmt, FilterRows, filter_columns(NonEmptyCols, Columns), Rows, Columns) of
        {ok, ChangedKeys} ->
            case process_insert(RestStmts, RestRows, Columns) of
                {ok, RestChangedKeys} ->
                    {ok, ChangedKeys ++ RestChangedKeys};
                ErrorRest ->
                    ErrorRest
            end;
        Error ->
            Error
    end.

-spec process_one_insert(term(), [#row{}], [#stmtCol{}], [#row{}], [#stmtCol{}]) -> {ok, list()} | {error, term()}.
process_one_insert(PrepStmt, FilterRows, FilterColumns, Rows, Columns) ->
    RowsToInsert = [list_to_tuple(create_bind_vals(Row#row.values, FilterColumns)) || Row <- FilterRows],
    io:format("The rows to insert ~p, and the stmt ~p~n", [RowsToInsert, PrepStmt]),
    case PrepStmt:exec_stmt(RowsToInsert, ?NoCommit) of
        {rowids, RowIds} ->
            if
                length(RowIds) =:= length(RowsToInsert) ->
                    case inserted_changed_keys(RowIds, Rows, Columns) of
                        {error, ErrorMsg} ->
                            io:format("An error ~p", [ErrorMsg]),
                            {error, ErrorMsg};
                        ChangedKeys ->
                            io:format("The changed keys ~p", [ChangedKeys]),
                            {ok, ChangedKeys}
                    end;
                true ->
                    io:format("The rowids returned ~p doesn't match the number of rows to insert ~p~n", [RowIds, RowsToInsert]),
                    %% TODO: What is a good message here ?
                    {error, <<"Error inserting the rows.">>}
            end;
        {error, {_ErrorCode, ErrorMsg}}->
            {error, ErrorMsg}
    end.

-spec split_changes(list()) -> {[#row{}], [#row{}], [#row{}]}.
split_changes(ChangeList) ->
    split_changes(ChangeList, {[], [], []}).

split_changes([], Result) -> Result;
split_changes([ListRow | ChangeList], Result) ->
    [Pos, Op, Index | Values] = ListRow,
    case Index of
        {Idx, {}} -> RowId = element(1, Idx);
        _ ->         RowId = undefined
    end,
    Row = #row{index  = Index,
               id     = RowId,
               pos    = Pos,
               op     = Op,
               values = Values},
    NewResult = add_to_split_result(Row, Result),
    split_changes(ChangeList, NewResult).

%%TODO: Change for less verbose option setelement...
add_to_split_result(#row{op = del} = Row, {DeleteRows, UpdateRows, InsertRows}) ->
    {[Row | DeleteRows], UpdateRows, InsertRows};
add_to_split_result(#row{op = upd} = Row, {DeleteRows, UpdateRows, InsertRows}) ->
    {DeleteRows, [Row | UpdateRows], InsertRows};
add_to_split_result(#row{op = ins} = Row, {DeleteRows, UpdateRows, InsertRows}) ->
    {DeleteRows, UpdateRows, [Row | InsertRows]}.

filter_columns(ModifiedCols, Columns) ->
    ModifiedColsList = tuple_to_list(ModifiedCols),
    [lists:nth(ColIdx, Columns) || ColIdx <- ModifiedColsList].

create_upd_vars([#stmtCol{} = Col]) -> [Col#stmtCol.tag, " = :", Col#stmtCol.tag];
create_upd_vars([#stmtCol{} = Col | Rest]) -> [Col#stmtCol.tag, " = :", Col#stmtCol.tag, ", ", create_upd_vars(Rest)].

create_where_vars([]) -> [];
create_where_vars([#stmtCol{} = Col | Rest]) -> [Col#stmtCol.tag, " = :", Col#stmtCol.tag, " and ", create_where_vars(Rest)].

create_bind_types([]) -> [];
create_bind_types([#stmtCol{} = Col | Rest]) ->
    [{iolist_to_binary([":", Col#stmtCol.tag]), bind_types_map(Col#stmtCol.type)} | create_bind_types(Rest)].

create_ins_columns([#stmtCol{} = Col]) -> [Col#stmtCol.tag];
create_ins_columns([#stmtCol{} = Col | Rest]) -> [Col#stmtCol.tag, ", ", create_ins_columns(Rest)].

create_ins_vars([#stmtCol{} = Col]) -> [":", Col#stmtCol.tag];
create_ins_vars([#stmtCol{} = Col | Rest]) -> [":", Col#stmtCol.tag, ", ", create_ins_vars(Rest)].

create_changedkey_vals([], _Cols) -> [];
create_changedkey_vals([<<>> | Rest], [#stmtCol{} | RestCols]) ->
    [<<>> | create_changedkey_vals(Rest, RestCols)];
create_changedkey_vals([Value | Rest], [#stmtCol{type = Type, prec = PrecOrig} | RestCols]) ->
    case Type of
        'SQLT_DAT' ->
            ImemDatetime = imem_datatype:io_to_datetime(Value),
            [dderloci_utils:edatetime_to_ora(ImemDatetime) | create_changedkey_vals(Rest, RestCols)];
        'SQLT_NUM' ->
            if
                PrecOrig < -84 -> PrecFloat = 127;
                true -> PrecFloat = PrecOrig
            end,
            ValueWithScale = dderloci_utils:apply_scale(Value, PrecFloat),
            <<_SizeNumber:8, Number/binary>> = dderloci_utils:oranumber_encode(ValueWithScale),
            [Number | create_changedkey_vals(Rest, RestCols)];
        _ ->
            [Value | create_changedkey_vals(Rest, RestCols)]
    end.

create_bind_vals([], _Cols) -> [];
create_bind_vals([<<>> | Rest], [_Col | RestCols]) ->
    [<<>> | create_bind_vals(Rest, RestCols)];
create_bind_vals([Value | Rest], [Col | RestCols]) ->
    case Col#stmtCol.type of
        'SQLT_DAT' ->
            ImemDatetime = imem_datatype:io_to_datetime(Value),
            [dderloci_utils:edatetime_to_ora(ImemDatetime) | create_bind_vals(Rest, RestCols)];
        'SQLT_NUM' ->
            [dderloci_utils:oranumber_encode(Value) | create_bind_vals(Rest, RestCols)];
        _ ->
            [Value | create_bind_vals(Rest, RestCols)]
    end.

bind_types_map('SQLT_NUM') -> 'SQLT_VNU';
bind_types_map('SQLT_DAT') -> 'SQLT_ODT';
%% There is no really support for this types at the moment so use string to send the data...
bind_types_map('SQLT_INT') -> 'SQLT_STR';
bind_types_map('SQLT_FLT') -> 'SQLT_STR';
bind_types_map(Type) -> Type.

-spec inserted_changed_keys([binary()], [#row{}], list()) -> [tuple()].
inserted_changed_keys([], [], _) -> [];
inserted_changed_keys([RowId | RestRowIds], [Row | RestRows], Columns) ->
    [{Row#row.pos, {list_to_tuple(create_changedkey_vals([RowId | Row#row.values], [#stmtCol{type = 'SQLT_STR'} | Columns])), {}}} | inserted_changed_keys(RestRowIds, RestRows, Columns)];
inserted_changed_keys(_, _, _) ->
    {error, <<"Invalid row keys returned by the oracle driver">>}.

-spec split_by_columns_mod([#row{}], [#stmtCol{}], [{tuple(), [#row{}]}]) -> [{tuple(), [#row{}]}].
split_by_columns_mod([], _Columns, Result) -> Result;
split_by_columns_mod([#row{} = Row | RestRows], Columns, Result) ->
    ModifiedCols = list_to_tuple(get_modified_cols(Row, Columns)),
    io:format("The modified cols: ~p~n", [ModifiedCols]),
    case proplists:get_value(ModifiedCols, Result) of
        undefined ->
            NewResult = [{ModifiedCols, [Row]} | Result];
        RowsSameCol ->
            NewResult = lists:keyreplace(ModifiedCols, 1, Result, {ModifiedCols, [Row | RowsSameCol]})
    end,
    split_by_columns_mod(RestRows, Columns, NewResult).

-spec get_modified_cols(#row{}, [#stmtCol{}]) -> [integer()].
get_modified_cols(#row{index = Index, values = Values}, Columns) ->
    {OriginalValuesTuple, {}} = Index,
    [_RowId | OriginalValues] = tuple_to_list(OriginalValuesTuple),
    %% If we dont have rowid should be read only field.
    LengthOrig = length(OriginalValues),
    LengthOrig = length(Values),
    get_modified_cols(OriginalValues, Values, Columns, 1).

-spec get_modified_cols([binary()], [binary()], [#stmtCol{}], pos_integer()) -> [integer()].
get_modified_cols([], [], [], _) -> [];
get_modified_cols([<<>> | RestOrig], [<<>> | RestValues], [#stmtCol{} | Columns], Pos) ->
    get_modified_cols(RestOrig, RestValues, Columns, Pos + 1);
get_modified_cols([<<>> | RestOrig], [_Value | RestValues], [#stmtCol{} | Columns], Pos) ->
    [Pos | get_modified_cols(RestOrig, RestValues, Columns, Pos + 1)];
get_modified_cols([OrigVal | RestOrig], [Value | RestValues], [#stmtCol{type = 'SQLT_DAT'} | Columns], Pos) ->
    case dderloci_utils:ora_to_dderltime(OrigVal) of
        Value ->
            get_modified_cols(RestOrig, RestValues, Columns, Pos + 1);
        _ ->
            [Pos | get_modified_cols(RestOrig, RestValues, Columns, Pos + 1)]
    end;
get_modified_cols([OrigVal | RestOrig], [Value | RestValues], [#stmtCol{type = 'SQLT_NUM'} | Columns], Pos) ->
    SizeNum = size(OrigVal),
    {Mantissa, Exponent} = dderloci_utils:oranumber_decode(<<SizeNum, OrigVal/binary>>),
    Number = imem_datatype:decimal_to_io(Mantissa, Exponent),
    if
        Number =:= Value ->
            get_modified_cols(RestOrig, RestValues, Columns, Pos + 1);
        true ->
            [Pos | get_modified_cols(RestOrig, RestValues, Columns, Pos + 1)]
    end;
get_modified_cols([OrigVal | RestOrig], [OrigVal | RestValues], [#stmtCol{} | Columns], Pos) ->
    get_modified_cols(RestOrig, RestValues, Columns, Pos + 1);
get_modified_cols([_OrigVal | RestOrig], [_Value | RestValues], [#stmtCol{} | Columns], Pos) ->
    [Pos | get_modified_cols(RestOrig, RestValues, Columns, Pos + 1)].

-spec split_by_non_empty([#row{}], [{tuple(),[#row{}]}]) -> [{tuple(), [#row{}]}].
split_by_non_empty([], Result) -> Result;
split_by_non_empty([#row{values = Values} = Row | RestRows], Result) ->
    NonEmptyCols = list_to_tuple(get_non_empty_cols(Values, 1)),
    io:format("The non empty cols: ~p~n", [NonEmptyCols]),
    case proplists:get_value(NonEmptyCols, Result) of
        undefined ->
            NewResult = [{NonEmptyCols, [Row]} | Result];
        RowsSameCol ->
            NewResult = lists:keyreplace(NonEmptyCols, 1, Result, {NonEmptyCols, [Row | RowsSameCol]})
    end,
    split_by_non_empty(RestRows, NewResult).

-spec get_non_empty_cols([binary()], pos_integer()) -> [integer()].
get_non_empty_cols([], _) -> [];
get_non_empty_cols([<<>> | RestValues], Pos) ->
    get_non_empty_cols(RestValues, Pos + 1);
get_non_empty_cols([_Value | RestValues], Pos) ->
    [Pos | get_non_empty_cols(RestValues, Pos + 1)].

-spec close_stmts(list()) -> ok.
close_stmts([]) -> ok;
close_stmts([{del, Stmt} | RestStmts]) ->
    Stmt:close(),
    close_stmts(RestStmts);
close_stmts([{upd, Stmts} | RestStmts]) ->
    [Stmt:close() || Stmt <- Stmts],
    close_stmts(RestStmts);
close_stmts([{ins, Stmt} | RestStmts]) ->
    Stmt:close(),
    close_stmts(RestStmts).

-spec check_rowid([binary()], [#row{}]) -> boolean().
check_rowid(RowIds, Rows) when length(RowIds) =:= length(Rows) ->
    check_member(RowIds, Rows);
check_rowid(_, _) -> false.

-spec check_member([binary()], [#row{}]) -> boolean().
check_member(_, []) -> true;
check_member(RowIds, [Row | RestRows]) ->
    lists:member(Row#row.id, RowIds) andalso check_member(RowIds, RestRows).
