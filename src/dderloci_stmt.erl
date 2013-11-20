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
    {DeleteList, UpdateList, InsertList} = split_changes(ChangeList),
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
            [Stmt:close() || {_Op, Stmt} <- ResultStmt],
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
create_stmts([{upd, _UpdList} | Rest], TableName, Connection, Columns, ResultStmt) ->
    Sql = iolist_to_binary([<<"update ">>, TableName, " set ", create_upd_vars(Columns), " where ", TableName, ".ROWID = :IDENTIFIER"]),
    io:format("The update sql ~p~n", [Sql]),
    case Connection:prep_sql(Sql) of
        {error, {_ErrorCode, ErrorMsg}} ->
            %TODO: add ?Error ...(ErrorCode)...
            [Stmt:close() || {_Op, Stmt} <- ResultStmt],
            {error, ErrorMsg};
        Stmt ->
            BindTypes = [{<<":IDENTIFIER">>, 'SQLT_STR'} | create_bind_types(Columns)],
            io:format("The bind types: ~p~n", [BindTypes]),
            ResBind = Stmt:bind_vars(BindTypes),
            io:format("The bind result ~p~n", [ResBind]),
            create_stmts(Rest, TableName, Connection, Columns, [{upd, Stmt} | ResultStmt])
    end;
create_stmts([{ins, []} | Rest], TableName, Connection, Columns, ResultStmt) ->
    io:format("~n-----> Nothing to insert, continue with the next one <----- ~n"),
    create_stmts(Rest, TableName, Connection, Columns, ResultStmt);
create_stmts([{ins, _InsList} | Rest], TableName, Connection, Columns, ResultStmt) ->
    io:format("The column info ~p~n", [Columns]),
    InsColumns = ["(", create_ins_columns(Columns), ")"],
    Sql = iolist_to_binary([<<"insert into ">>, TableName, " ", InsColumns, " values ", "(", create_ins_vars(Columns), ")"]),
    io:format("The insert sql ~p~n", [Sql]),
    case Connection:prep_sql(Sql) of
        {error, {_ErrorCode, ErrorMsg}} ->
            %%TODO: ?Error...
            [Stmt:close() || {_Op, Stmt} <- ResultStmt],
            {error, ErrorMsg};
        Stmt ->
            BindTypes = create_bind_types(Columns),
            io:format("The bind types: ~p~n", [BindTypes]),
            ResBind = Stmt:bind_vars(BindTypes),
            io:format("The bind result ~p~n", [ResBind]),
            create_stmts(Rest, TableName, Connection, Columns, [{ins, Stmt} | ResultStmt])
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

-spec process_update(term(), list(), list()) -> {ok, list()} | {error, term()}.
process_update(undefined, [], _Columns) -> {ok, []};
process_update(PrepStmt, Rows, Columns) ->
    RowsToUpdate = [list_to_tuple(create_bind_vals([Row#row.id | Row#row.values], [#stmtCol{type = 'SQLT_STR'} | Columns])) || Row <- Rows],
    io:format("The rows to update ~p", [RowsToUpdate]),
    case PrepStmt:exec_stmt(RowsToUpdate, ?NoCommit) of
        {rowids, _RowIds} -> %% TODO: Check if the modified rows are the correct.
            ChangedKeys = [{Row#row.pos, {list_to_tuple(create_changedkey_vals([Row#row.id | Row#row.values], [#stmtCol{type = 'SQLT_STR'} | Columns])), {}}} || Row <- Rows],
            {ok, ChangedKeys};
        {error, {_ErrorCode, ErrorMsg}}->
            {error, ErrorMsg}
    end.

-spec process_insert(term(), list(), list()) -> {ok, list()} | {error, term()}.
process_insert(undefined, [], _Columns) -> {ok, []};
process_insert(PrepStmt, Rows, Columns) ->
    RowsToInsert = [list_to_tuple(create_bind_vals(Row#row.values, Columns)) || Row <- Rows],
    io:format("The rows to insert ~p", [RowsToInsert]),
    case PrepStmt:exec_stmt(RowsToInsert, ?NoCommit) of
        {rowids, RowIds} ->
            case inserted_changed_keys(RowIds, Rows, Columns) of
                {error, ErrorMsg} -> {error, ErrorMsg};
                ChangedKeys -> {ok, ChangedKeys}
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

create_upd_vars([#stmtCol{} = Col]) -> [Col#stmtCol.tag, "= :", Col#stmtCol.tag];
create_upd_vars([#stmtCol{} = Col | Rest]) -> [Col#stmtCol.tag, "= :", Col#stmtCol.tag, ", ", create_upd_vars(Rest)].

create_bind_types([]) -> [];
create_bind_types([#stmtCol{} = Col | Rest]) ->
    [{iolist_to_binary([":", Col#stmtCol.tag]), bind_types_map(Col#stmtCol.type)} | create_bind_types(Rest)].

create_ins_columns([#stmtCol{} = Col]) -> [Col#stmtCol.tag];
create_ins_columns([#stmtCol{} = Col | Rest]) -> [Col#stmtCol.tag, ", ", create_ins_columns(Rest)].

create_ins_vars([#stmtCol{} = Col]) -> [":", Col#stmtCol.tag];
create_ins_vars([#stmtCol{} = Col | Rest]) -> [":", Col#stmtCol.tag, ", ", create_ins_vars(Rest)].

create_changedkey_vals([], _Cols) -> [];
create_changedkey_vals([<<>> | Rest], [_Col | RestCols]) ->
    [<<>> | create_changedkey_vals(Rest, RestCols)];
create_changedkey_vals([Value | Rest], [Col | RestCols]) ->
    case Col#stmtCol.type of
        'SQLT_DAT' ->
            ImemDatetime = imem_datatype:io_to_datetime(Value),
            [dderloci_utils:edatetime_to_ora(ImemDatetime) | create_changedkey_vals(Rest, RestCols)];
        'SQLT_NUM' ->
            <<_SizeNumber:8, Number/binary>> = dderloci_utils:oranumber_encode(Value),
            [Number | create_changedkey_vals(Rest, RestCols)];
        _ ->
            [Value | create_changedkey_vals(Rest, RestCols)]
    end.

create_bind_vals([], _Cols) -> [];
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
