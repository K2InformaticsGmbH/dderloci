-module(dderloci).
-behaviour(gen_server).

-include("dderloci.hrl").

%% API
-export([
    exec/3,
    change_password/4,
    add_fsm/2,
    fetch_recs_async/3,
    fetch_close/1,
    filter_and_sort/6,
    close/1,
    run_table_cmd/3
]).

%% gen_server callbacks
-export([init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3]).

-record(qry, {select_sections
             ,contain_rowid
             ,stmt_result
             ,fsm_ref
             ,max_rowcount
             ,pushlock
             ,contain_rownum
             }).

-define(PREFETCH_SIZE, 250).

%% Exported functions
-spec exec(tuple(), binary(), integer()) -> ok | {ok, pid()} | {error, term()}.
exec({oci_port, _, _} = Connection, Sql, MaxRowCount) ->
    case sqlparse:parsetree(Sql) of
        {ok,{[{{select, SelectSections},_}],_}} ->
            {TableName, NewSql, RowIdAdded} = inject_rowid(SelectSections, Sql);
        _ ->
            TableName = <<"">>,
            NewSql = Sql,
            RowIdAdded = false,
            SelectSections = []
    end,
    case run_query(Connection, Sql, NewSql, RowIdAdded) of
        {ok, #stmtResult{} = StmtResult, ContainRowId} ->
            LowerSql = string:to_lower(binary_to_list(Sql)),
            case string:str(LowerSql, "rownum") of
                0 -> ContainRowNum = false;
                _ -> ContainRowNum = true
            end,
            {ok, Pid} = gen_server:start(?MODULE, [SelectSections, StmtResult, ContainRowId, MaxRowCount, ContainRowNum], []),
            SortSpec = gen_server:call(Pid, build_sort_spec),
            %% Mask the internal stmt ref with our pid.
            {ok, StmtResult#stmtResult{stmtRef = Pid, sortSpec = SortSpec}, TableName};
        NoSelect -> NoSelect
    end.

-spec change_password(tuple(), binary(), binary(), binary()) -> ok | {error, term()}.
change_password({oci_port, _, _} = Connection, User, OldPassword, NewPassword) ->
    run_table_cmd(Connection, iolist_to_binary(["ALTER USER ", User, " IDENTIFIED BY ", NewPassword, " REPLACE ", OldPassword])).

-spec add_fsm(pid(), term()) -> ok.
add_fsm(Pid, FsmRef) ->
    gen_server:cast(Pid, {add_fsm, FsmRef}).

-spec fetch_recs_async(pid(), list(), integer()) -> ok.
fetch_recs_async(Pid, Opts, Count) ->
    gen_server:cast(Pid, {fetch_recs_async, lists:member({fetch_mode, push}, Opts), Count}).

-spec fetch_close(pid()) -> ok.
fetch_close(Pid) ->
    gen_server:call(Pid, fetch_close).

-spec filter_and_sort(pid(), tuple(), list(), list(), list(), binary()) -> {ok, binary(), fun()}.
filter_and_sort(Pid, Connection, FilterSpec, SortSpec, Cols, Query) ->
    gen_server:call(Pid, {filter_and_sort, Connection, FilterSpec, SortSpec, Cols, Query}).

-spec close(pid()) -> term().
close(Pid) ->
    gen_server:call(Pid, close).

%% Gen server callbacks
init([SelectSections, StmtResult, ContainRowId, MaxRowCount, ContainRowNum]) ->
    {ok, #qry{
            select_sections = SelectSections,
            stmt_result = StmtResult,
            contain_rowid = ContainRowId,
            max_rowcount = MaxRowCount,
            contain_rownum = ContainRowNum}}.

handle_call({filter_and_sort, Connection, FilterSpec, SortSpec, Cols, Query}, _From, #qry{stmt_result = StmtResult} = State) ->
    #stmtResult{stmtCols = StmtCols} = StmtResult,
    %% TODO: improve this to use/update parse tree from the state.
    Res = filter_and_sort_internal(Connection, FilterSpec, SortSpec, Cols, Query, StmtCols),
    {reply, Res, State};
handle_call(build_sort_spec, _From, #qry{stmt_result = StmtResult, select_sections = SelectSections} = State) ->
    #stmtResult{stmtCols = StmtCols} = StmtResult,
    SortSpec = build_sort_spec(SelectSections, StmtCols),
    {reply, SortSpec, State};
handle_call(get_state, _From, State) ->
    {reply, State, State};
handle_call(fetch_close, _From, #qry{} = State) ->
    {reply, ok, State#qry{pushlock = true}};
handle_call(close, _From, #qry{stmt_result = StmtResult} = State) ->
    #stmtResult{stmtRef = StmtRef} = StmtResult,
    {stop, normal, StmtRef:close(), State#qry{stmt_result = StmtResult#stmtResult{stmtRef = undefined}}};
handle_call(_Ignored, _From, State) ->
    {noreply, State}.

handle_cast({add_fsm, FsmRef}, #qry{} = State) -> {noreply, State#qry{fsm_ref = FsmRef}};
handle_cast({fetch_recs_async, _, _}, #qry{pushlock = true} = State) ->
    {noreply, State};
handle_cast({fetch_push, _, _}, #qry{pushlock = true} = State) ->
    {noreply, State};
handle_cast({fetch_recs_async, true, FsmNRows}, #qry{max_rowcount = MaxRowCount} = State) ->
    case FsmNRows rem MaxRowCount of
        0 -> RowsToRequest = MaxRowCount;
        Result -> RowsToRequest = MaxRowCount - Result
    end,
    gen_server:cast(self(), {fetch_push, 0, RowsToRequest}),
    {noreply, State};
handle_cast({fetch_recs_async, false, _}, #qry{fsm_ref = FsmRef, stmt_result = StmtResult, contain_rowid = ContainRowId} = State) ->
    #stmtResult{stmtRef = StmtRef, stmtCols = Clms} = StmtResult,
    case StmtRef:fetch_rows(?DEFAULT_ROW_SIZE) of
        {{rows, Rows}, Completed} ->
            FsmRef:rows({fix_row_format(Rows, Clms, ContainRowId), Completed});
        {error, Error} ->
            FsmRef:rows({error, Error})
    end,
    {noreply, State};
handle_cast({fetch_push, NRows, Target}, #qry{fsm_ref = FsmRef, stmt_result = StmtResult} = State) ->
    #qry{contain_rowid = ContainRowId, contain_rownum = ContainRowNum} = State,
    #stmtResult{stmtRef = StmtRef, stmtCols = Clms} = StmtResult,
    MissingRows = Target - NRows,
    if
        MissingRows > ?DEFAULT_ROW_SIZE ->
            RowsToFetch = ?DEFAULT_ROW_SIZE;
        true ->
            RowsToFetch = MissingRows
    end,
    case StmtRef:fetch_rows(RowsToFetch) of
        {{rows, Rows}, Completed} ->
            RowsFixed = fix_row_format(Rows, Clms, ContainRowId),
            NewNRows = NRows + length(RowsFixed),
            if
                Completed -> FsmRef:rows({RowsFixed, Completed});
                (NewNRows >= Target) andalso (not ContainRowNum) -> FsmRef:rows_limit(NewNRows, RowsFixed);
                true ->
                    FsmRef:rows({RowsFixed, false}),
                    gen_server:cast(self(), {fetch_push, NewNRows, Target})
            end;
        {error, Error} ->
            FsmRef:rows({error, Error})
    end,
    {noreply, State};
handle_cast(_Ignored, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
	{noreply, State}.

terminate(_Reason, #qry{stmt_result = #stmtResult{stmtRef = undefined}}) -> ok;
terminate(_Reason, #qry{stmt_result = #stmtResult{stmtRef = StmtRef}}) -> StmtRef:close().

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% Internal functions %%%

-spec inject_rowid(list(), binary()) -> {binary(), binary()}.
inject_rowid(Args, Sql) ->
    {fields, Flds} = lists:keyfind(fields, 1, Args),
    {from, [FirstTable|_]=Forms} = lists:keyfind(from, 1, Args),
    NewFields = expand_star(Flds, Forms) ++ [add_rowid_field(FirstTable)],
    NewArgs = lists:keyreplace(fields, 1, Args, {fields, NewFields}),
    NPT = {select, NewArgs},
    case sqlparse:pt_to_string(NPT) of
        {error, _Reason} ->
            {FirstTable, Sql, false};
        NewSql ->
            {FirstTable, NewSql, true}
    end.

-spec add_rowid_field(tuple() | binary()) -> binary().
add_rowid_field(Table) -> qualify_field(Table, "ROWID").

-spec qualify_field(tuple() | binary(), binary() | list()) -> binary().
qualify_field(Table, Field) -> iolist_to_binary(add_field(Table, Field)).

-spec add_field(tuple() | binary(), binary() | list()) -> iolist().
add_field({as, _, Alias}, Field) -> [Alias, ".", Field];
add_field({{as, _, Alias}, _}, Field) -> [Alias, ".", Field];
add_field({Tab, _}, Field) -> [Tab, ".", Field];
add_field(Tab, Field) -> [Tab, ".", Field].

-spec expand_star(list(), list()) -> list().
expand_star([<<"*">>], Forms) -> qualify_star(Forms);
expand_star(Flds, _Forms) -> Flds.

-spec qualify_star(list()) -> list().
qualify_star([]) -> [];
qualify_star([Table | Rest]) -> [qualify_field(Table, "*") | qualify_star(Rest)].

run_query(Connection, Sql, NewSql, RowIdAdded) ->
    %% For now only the first table is counted.
    Statement = Connection:prep_sql(NewSql),
    case Statement:exec_stmt() of
        {cols, Clms} ->
            % ROWID is hidden from columns
            if
                RowIdAdded ->
                    [_|ColumnsR] = lists:reverse(Clms),
                    Columns = lists:reverse(ColumnsR);
                true ->
                    Columns = Clms
            end,
            NewClms = cols_to_rec(Columns),
            SortFun = build_sort_fun(NewSql, NewClms),
            {ok
            , #stmtResult{ stmtCols = NewClms
                         , rowFun   =
                               fun({{}, Row}) ->
                                       if
                                           RowIdAdded ->
                                               [_|NewRowR] = lists:reverse(tuple_to_list(Row)),
                                               translate_datatype(Statement, lists:reverse(NewRowR), NewClms);
                                           true ->
                                               translate_datatype(Statement, tuple_to_list(Row), NewClms)
                                       end
                               end
                         , stmtRef  = Statement
                         , sortFun  = SortFun
                         , sortSpec = []}
            , RowIdAdded};
        {rowids, _} ->
            Statement:close(),
            ok;
        {executed, _} ->
            Statement:close(),
            ok;
        _RowIdError ->
            Statement:close(),
            Statement1 = Connection:prep_sql(Sql),
            case Statement1:exec_stmt() of
                {cols, Clms} ->
                    NewClms = cols_to_rec(Clms),
                    SortFun = build_sort_fun(Sql, NewClms),
                    {ok
                    , #stmtResult{ stmtCols = NewClms
                                 , rowFun   = fun({{}, Row}) ->
                                                translate_datatype(Statement, tuple_to_list(Row), NewClms)
                                              end
                                 , stmtRef  = Statement1
                                 , sortFun  = SortFun
                                 , sortSpec = []}
                    , false};
                Error ->
                    Statement1:close(),
                    Error
            end
    end.

can_expand([<<"*">>], _, _) -> true;
can_expand(SelectFields, [TableName], AllFields) when is_binary(TableName) ->
    LowerSelectFields = [string:to_lower(binary_to_list(X)) || X <- SelectFields, is_binary(X)],
    LowerAllFields = [string:to_lower(binary_to_list(X)) || X <- AllFields],
    length(LowerSelectFields) =:= length(LowerAllFields) andalso [] =:= (LowerSelectFields -- LowerAllFields);
can_expand(_, _, _) -> false.

build_sort_spec(SelectSections, StmtCols) ->
    FullMap = build_full_map(StmtCols),
    case lists:keyfind('order by', 1, SelectSections) of
        {'order by', OrderBy} ->
            [process_sort_order(ColOrder, FullMap) || ColOrder <- OrderBy];
        _ ->
            []
    end.

process_sort_order({Name, <<>>}, Map) ->
    process_sort_order({Name, <<"asc">>}, Map);
process_sort_order({Name, Dir}, []) when is_binary(Name)-> {Name, Dir};
process_sort_order({Name, Dir}, [#bind{alias = Alias, cind = Pos} | Rest]) when is_binary(Name) ->
    case string:to_lower(binary_to_list(Name)) =:= string:to_lower(binary_to_list(Alias)) of
        true -> {Pos, Dir};
        false -> process_sort_order({Name, Dir}, Rest)
    end;
process_sort_order({Fun, Dir}, Map) ->
    process_sort_order({sqlparse:pt_to_string(Fun), Dir}, Map).


%%% Model how imem gets the new filter and sort results %%%%
%       NewSortFun = imem_sql:sort_spec_fun(SortSpec, FullMaps, ColMaps),
%       %?Debug("NewSortFun ~p~n", [NewSortFun]),
%       OrderBy = imem_sql:sort_spec_order(SortSpec, FullMaps, ColMaps),
%       %?Debug("OrderBy ~p~n", [OrderBy]),
%       Filter =  imem_sql:filter_spec_where(FilterSpec, ColMaps, WhereTree),
%       %?Debug("Filter ~p~n", [Filter]),
%       Cols1 = case Cols0 of
%           [] ->   lists:seq(1,length(ColMaps));
%           _ ->    Cols0
%       end,
%       AllFields = imem_sql:column_map_items(ColMaps, ptree),
%       % ?Debug("AllFields ~p~n", [AllFields]),
%       NewFields =  [lists:nth(N,AllFields) || N <- Cols1],
%       % ?Debug("NewFields ~p~n", [NewFields]),
%       NewSections0 = lists:keyreplace('fields', 1, SelectSections, {'fields',NewFields}),
%       NewSections1 = lists:keyreplace('where', 1, NewSections0, {'where',Filter}),
%       %?Debug("NewSections1 ~p~n", [NewSections1]),
%       NewSections2 = lists:keyreplace('order by', 1, NewSections1, {'order by',OrderBy}),
%       %?Debug("NewSections2 ~p~n", [NewSections2]),
%       NewSql = sqlparse:pt_to_string({select,NewSections2}),     % sql_box:flat_from_pt({select,NewSections2}),
%       %?Debug("NewSql ~p~n", [NewSql]),
%       {ok, NewSql, NewSortFun}

filter_and_sort_internal(_Connection, FilterSpec, SortSpec, Cols, Query, StmtCols) ->
    FullMap = build_full_map(StmtCols),
    case Cols of
        [] ->   Cols1 = lists:seq(1,length(FullMap));
        _ ->    Cols1 = Cols
    end,
    % AllFields = imem_sql:column_map_items(ColMaps, ptree), %%% This should be the correct way if doing it.
    AllFields = [C#bind.alias || C <- FullMap],
    SortSpecExplicit = [{Col, Dir} || {Col, Dir} <- SortSpec, is_integer(Col)],
    NewSortFun = imem_sql_expr:sort_spec_fun(SortSpecExplicit, FullMap, FullMap),
    case sqlparse:parsetree(Query) of
        {ok,{[{{select, SelectSections},_}],_}} ->
            {fields, Flds} = lists:keyfind(fields, 1, SelectSections),
            {from, Tables} = lists:keyfind(from, 1, SelectSections),
            {where, WhereTree} = lists:keyfind(where, 1, SelectSections),
            case can_expand(Flds, Tables, AllFields) of
                true ->
                    NewFields = [lists:nth(N,AllFields) || N <- Cols1],
                    NewSections0 = lists:keyreplace('fields', 1, SelectSections, {'fields',NewFields});
                false ->
                    NewSections0 = SelectSections
            end,
            Filter =  imem_sql_expr:filter_spec_where(FilterSpec, FullMap, WhereTree),
            NewSections1 = lists:keyreplace('where', 1, NewSections0, {'where',Filter}),
            OrderBy = imem_sql_expr:sort_spec_order(SortSpec, FullMap, FullMap),
            NewSections2 = lists:keyreplace('order by', 1, NewSections1, {'order by',OrderBy}),
            NewSql = sqlparse:pt_to_string({select, NewSections2});
        _->
            NewSql = Query
    end,
    {ok, NewSql, NewSortFun}.

-spec to_imem_type(atom()) -> atom().
to_imem_type('SQLT_NUM') -> number;
to_imem_type(_) -> binstr.

build_full_map(Clms) ->
    [#bind{ tag = list_to_atom([$$|integer_to_list(T)])
              , name = Alias
              , alias = Alias
              , tind = 2
              , cind = T
              , type = to_imem_type(OciType)
              , len = 300
              , prec = undefined }
     || {T, #stmtCol{alias = Alias, type = OciType}} <- lists:zip(lists:seq(1,length(Clms)), Clms)].

%   Tables = case lists:keyfind(from, 1, SelectSections) of
%       {_, TNames} ->  Tabs = [imem_sql:table_qname(T) || T <- TNames],
%                       [{_,MainTab,_}|_] = Tabs,
%                       case lists:member(MainTab,[ddSize|?DataTypes]) of
%                           true ->     ?ClientError({"Virtual table can only be joined", MainTab});
%                           false ->    Tabs
%                       end;
%       TError ->       ?ClientError({"Invalid from in select structure", TError})
%   end,
%   imem_sql:column_map(Tables,[]);

build_sort_fun(_Sql, _Clms) ->
    fun(_Row) -> {} end.

-spec cols_to_rec([tuple()]) -> [#stmtCol{}].
cols_to_rec([]) -> [];
cols_to_rec([{Alias,'SQLT_NUM',_Len,Prec,Scale}|Rest]) ->
    [#stmtCol{ tag = Alias
             , alias = Alias
             , type = 'SQLT_NUM'
             , len = Prec
             , prec = Scale
             , readonly = false} | cols_to_rec(Rest)];
cols_to_rec([{Alias,Type,Len,Prec,_Scale}|Rest]) ->
    [#stmtCol{ tag = Alias
             , alias = Alias
             , type = Type
             , len = Len
             , prec = Prec
             , readonly = false} | cols_to_rec(Rest)].

translate_datatype(_Stmt, [], []) -> [];
translate_datatype(Stmt, [<<>> | RestRow], [#stmtCol{} | RestCols]) ->
    [<<>> | translate_datatype(Stmt, RestRow, RestCols)];
translate_datatype(Stmt, [R | RestRow], [#stmtCol{type = 'SQLT_DAT'} | RestCols]) ->
    [dderloci_utils:ora_to_dderltime(R) | translate_datatype(Stmt, RestRow, RestCols)];
translate_datatype(Stmt, [R | RestRow], [#stmtCol{type = 'SQLT_NUM'} | RestCols]) ->
    SizeNum = size(R),
    {Mantissa, Exponent} = dderloci_utils:oranumber_decode(<<SizeNum, R/binary>>),
    Number = imem_datatype:decimal_to_io(Mantissa, Exponent),
    [Number | translate_datatype(Stmt, RestRow, RestCols)];
translate_datatype(Stmt, [{_Pointer, Size, Path, Name} | RestRow], [#stmtCol{type = 'SQLT_BFILEE'} | RestCols]) ->
    SizeBin = integer_to_binary(Size),
    [<<Path/binary, $#, Name/binary, 32, $[, SizeBin/binary, $]>> | translate_datatype(Stmt, RestRow, RestCols)];
translate_datatype(Stmt, [{Pointer, Size} | RestRow], [#stmtCol{type = 'SQLT_BLOB'} | RestCols]) ->
    if
        Size > ?PREFETCH_SIZE ->
            {lob, Trunc} = Stmt:lob(Pointer, 1, ?PREFETCH_SIZE),
            SizeBin = integer_to_binary(Size),
            AsIO = imem_datatype:binary_to_io(Trunc),
            [<<AsIO/binary, $., $., 32, $[, SizeBin/binary, $]>> | translate_datatype(Stmt, RestRow, RestCols)];
        true ->
            {lob, Full} = Stmt:lob(Pointer, 1, Size),
            AsIO = imem_datatype:binary_to_io(Full),
            [AsIO | translate_datatype(Stmt, RestRow, RestCols)]
    end;
translate_datatype(Stmt, [{Pointer, Size} | RestRow], [#stmtCol{type = 'SQLT_CLOB'} | RestCols]) ->
    if
        Size > ?PREFETCH_SIZE ->
            {lob, Trunc} = Stmt:lob(Pointer, 1, ?PREFETCH_SIZE),
            SizeBin = integer_to_binary(Size),
            [<<Trunc/binary, $., $., 32, $[, SizeBin/binary, $]>> | translate_datatype(Stmt, RestRow, RestCols)];
        true ->
            {lob, Full} = Stmt:lob(Pointer, 1, Size),
            [Full | translate_datatype(Stmt, RestRow, RestCols)]
    end;
translate_datatype(Stmt, [Raw | RestRow], [#stmtCol{type = 'SQLT_BIN'} | RestCols]) ->
    [imem_datatype:binary_to_io(Raw) | translate_datatype(Stmt, RestRow, RestCols)];
translate_datatype(Stmt, [R | RestRow], [#stmtCol{} | RestCols]) ->
    [R | translate_datatype(Stmt, RestRow, RestCols)].

-spec fix_row_format([list()], [#stmtCol{}], boolean()) -> [tuple()].
fix_row_format([], _, _) -> [];
fix_row_format([Row | Rest], Columns, ContainRowId) ->
    %% TODO: we have to add the table name at the start of the rows i.e
    %  rows [
    %        {{temp,1,2,3},{}},
    %        {{temp,4,5,6},{}}
    %  ]

    %% TODO: Convert the types to imem types??
    % db_to_io(Type, Prec, DateFmt, NumFmt, _StringFmt, Val),
    % io_to_db(Item,Old,Type,Len,Prec,Def,false,Val) when is_binary(Val);is_list(Val)
    if
        ContainRowId ->
            {RestRow, [RowId]} = lists:split(length(Row) - 1, Row),
            [{{}, list_to_tuple(fix_null(RestRow, Columns) ++ [RowId])} | fix_row_format(Rest, Columns, ContainRowId)];
        true ->
            [{{}, list_to_tuple(fix_null(Row, Columns))} | fix_row_format(Rest, Columns, ContainRowId)]
    end.

fix_null([], []) -> [];
fix_null([<<Length:8, RestNum/binary>> | RestRow], [#stmtCol{type = 'SQLT_NUM'} | RestCols]) ->
    <<Number:Length/binary, _Discarded/binary>> = RestNum,
    [Number | fix_null(RestRow, RestCols)];
fix_null([<<0, 0, 0, 0, 0, 0, 0, _/binary>> | RestRow], [#stmtCol{type = 'SQLT_DAT'} | RestCols]) -> %% Null format for date.
    [<<>> | fix_null(RestRow, RestCols)];
fix_null([Cell | RestRow], [#stmtCol{} | RestCols]) ->
    [Cell | fix_null(RestRow, RestCols)].

-spec run_table_cmd(tuple(), atom(), binary()) -> ok | {error, term()}.
run_table_cmd({oci_port, _, _} = _Connection, restore_table, _TableName) -> {error, <<"Command not implemented">>};
run_table_cmd({oci_port, _, _} = _Connection, snapshot_table, _TableName) -> {error, <<"Command not implemented">>};
run_table_cmd({oci_port, _, _} = Connection, truncate_table, TableName) ->
    run_table_cmd(Connection, iolist_to_binary([<<"truncate table ">>, TableName]));
run_table_cmd({oci_port, _, _} = Connection, drop_table, TableName) ->
    run_table_cmd(Connection, iolist_to_binary([<<"drop table ">>, TableName])).

-spec run_table_cmd(tuple(), binary()) -> ok | {error, term()}.
run_table_cmd(Connection, SqlCmd) ->
    Statement = Connection:prep_sql(SqlCmd),
    case Statement:exec_stmt() of
        {executed, _} ->
            Statement:close(),
            ok;
        Error ->
            {error, Error}
    end.
