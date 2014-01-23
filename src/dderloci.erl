-module(dderloci).
-behaviour(gen_server).

-include("dderloci.hrl").

%% API
-export([
    exec/2,
    change_password/4,
    add_fsm/2,
    fetch_recs_async/2,
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
             }).

%% Exported functions
-spec exec(tuple(), binary()) -> ok | {ok, pid()} | {error, term()}.
exec({oci_port, _, _} = Connection, Sql) ->
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
            %% TODO: Parse the rownum instead of hard coded
            {ok, Pid} = gen_server:start(?MODULE, [SelectSections, StmtResult, ContainRowId, 10000], []),
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

-spec fetch_recs_async(pid(), list()) -> ok.
fetch_recs_async(Pid, Opts) ->
    gen_server:cast(Pid, {fetch_recs_async, lists:member({fetch_mode, push}, Opts), 0}).

-spec fetch_close(pid()) -> ok.
fetch_close(Pid) ->
    gen_server:cast(Pid, fetch_close).

-spec filter_and_sort(pid(), tuple(), list(), list(), list(), binary()) -> {ok, binary(), fun()}.
filter_and_sort(Pid, Connection, FilterSpec, SortSpec, Cols, Query) ->
    gen_server:call(Pid, {filter_and_sort, Connection, FilterSpec, SortSpec, Cols, Query}).

-spec close(pid()) -> term().
close(Pid) ->
    gen_server:call(Pid, close).

%% Gen server callbacks
init([SelectSections, StmtResult, ContainRowId, MaxRowCount]) ->
    {ok, #qry{select_sections = SelectSections, stmt_result = StmtResult, contain_rowid = ContainRowId, max_rowcount = MaxRowCount}}.

handle_call({filter_and_sort, Connection, FilterSpec, SortSpec, Cols, Query}, _From, #qry{stmt_result = StmtResult, contain_rowid = ContainRowId} = State) ->
    #stmtResult{stmtCols = StmtCols} = StmtResult,
    %% TODO: improve this to use/update parse tree from the state.
    Res = filter_and_sort(Connection, FilterSpec, SortSpec, Cols, Query, StmtCols, ContainRowId),
    {reply, Res, State};
handle_call(build_sort_spec, _From, #qry{stmt_result = StmtResult, select_sections = SelectSections, contain_rowid = ContainRowId} = State) ->
    #stmtResult{stmtCols = StmtCols} = StmtResult,
    SortSpec = build_sort_spec(SelectSections, StmtCols, ContainRowId),
    {reply, SortSpec, State};
handle_call(get_state, _From, State) ->
    {reply, State, State};
handle_call(close, _From, #qry{stmt_result = StmtResult} = State) ->
    #stmtResult{stmtRef = StmtRef} = StmtResult,
    {stop, normal, StmtRef:close(), State#qry{stmt_result = StmtResult#stmtResult{stmtRef = undefined}}};
handle_call(Ignored, _From, State) ->
    io:format("unexpected call ~p~n", [Ignored]),
    {noreply, State}.

handle_cast({add_fsm, FsmRef}, #qry{} = State) -> {noreply, State#qry{fsm_ref = FsmRef}};
handle_cast(fetch_close, #qry{} = State) -> {noreply, State#qry{pushlock = true}};
handle_cast({fetch_recs_async, PushMode, NRows}, #qry{fsm_ref = FsmRef, stmt_result = StmtResult} = State) ->
    #qry{contain_rowid = ContainRowId, max_rowcount = MaxRowCount, pushlock = Pushlock} = State,
    #stmtResult{stmtRef = StmtRef, stmtCols = Clms} = StmtResult,
    case StmtRef:fetch_rows(?DEFAULT_ROW_SIZE) of
        {{rows, Rows}, Completed} ->
            RowsFixed = fix_row_format(Rows, Clms, ContainRowId),
            NewNRows = NRows + length(RowsFixed),
            if
                Completed -> FsmRef:rows({RowsFixed, Completed});
                NewNRows >= MaxRowCount -> FsmRef:rows_limit(NewNRows, RowsFixed);
                Pushlock -> FsmRef:rows({RowsFixed, Completed});
                PushMode ->
                    FsmRef:rows({RowsFixed, Completed}),
                    gen_server:cast(self(), {fetch_recs_async, PushMode, NewNRows});
                true -> FsmRef:rows({RowsFixed, Completed})
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
%       NewSql = sqlparse:fold({select,NewSections2}),     % sql_box:flat_from_pt({select,NewSections2}),
%       %?Debug("NewSql ~p~n", [NewSql]),
%       {ok, NewSql, NewSortFun}

%% Internal functions %%%

-spec inject_rowid(list(), binary()) -> {binary(), binary()}.
inject_rowid(Args, Sql) ->
    {fields, Flds} = lists:keyfind(fields, 1, Args),
    {from, [FirstTable|_]=Forms} = lists:keyfind(from, 1, Args),
    NewFields = [add_rowid_field(FirstTable) | expand_star(Flds, Forms)],
    NewArgs = lists:keyreplace(fields, 1, Args, {fields, NewFields}),
    NPT = {select, NewArgs},
    case sqlparse:fold(NPT) of
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
            io:format("The original columns, ~p~n", [Clms]),
            if
                RowIdAdded ->
                    [_|Columns] = Clms;
                true ->
                    Columns = Clms
            end,
            NewClms = cols_to_rec(Columns),
            SortFun = build_sort_fun(NewSql, NewClms),
            {ok
            , #stmtResult{ stmtCols = NewClms
                         , rowFun   =
                               fun({Row, {}}) ->
                                       if
                                           RowIdAdded ->
                                               [_|NewRow] = tuple_to_list(Row),
                                               translate_datatype(NewRow, NewClms);
                                           true ->
                                               translate_datatype(tuple_to_list(Row), NewClms)
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
        RowIdError ->
            io:format("The row id select error ~p", [RowIdError]),
            Statement:close(),
            Statement1 = Connection:prep_sql(Sql),
            case Statement1:exec_stmt() of
                {cols, Clms} ->
                    NewClms = cols_to_rec(Clms),
                    {ok
                    , #stmtResult{ stmtCols = NewClms
                                 , rowFun   = fun({Row, {}}) ->
                                                translate_datatype(tuple_to_list(Row), NewClms)
                                              end
                                 , stmtRef  = Statement1
                                 , sortFun  = fun(_Row) -> {} end
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

build_sort_spec(SelectSections, StmtCols, ContainRowId) ->
    FullMap = build_full_map(StmtCols, ContainRowId),
    case lists:keyfind('order by', 1, SelectSections) of
        {'order by', OrderBy} ->
            [process_sort_order(ColOrder, FullMap, ContainRowId) || ColOrder <- OrderBy];
        _ ->
            []
    end.

process_sort_order({Name, <<>>}, Map, ContainRowId) ->
    process_sort_order({Name, <<"asc">>}, Map, ContainRowId);
process_sort_order(ColOrder, [], _) -> ColOrder;
process_sort_order({Name, Dir}, [#ddColMap{alias = Alias, cind = Pos} | Rest], ContainRowId) ->
    Match = string:to_lower(binary_to_list(Name)) =:= string:to_lower(binary_to_list(Alias)),
    if
        Match ->
            if
                ContainRowId -> NewPos = Pos - 1;
                true -> NewPos = Pos
            end,
            {NewPos, Dir};
        true -> process_sort_order({Name, Dir}, Rest, ContainRowId)
    end.

filter_and_sort(Connection, _FilterSpec, SortSpec, Cols, Query, StmtCols, ContainRowId) ->
    io:format("The filterspec ~p~n The Sort spec ~p~n the col_order ~p~n the Query ~p~n the fullmap ~p~n", [_FilterSpec, SortSpec, Cols, Query, StmtCols]),
    FullMap = build_full_map(StmtCols, ContainRowId),
    case Cols of
        [] ->   Cols1 = lists:seq(1,length(FullMap));
        _ ->    Cols1 = Cols
    end,
    % AllFields = imem_sql:column_map_items(ColMaps, ptree), %%% This should be the correct way if doing it.
    AllFields = [C#ddColMap.alias || C <- FullMap],
    SortSpecExplicit = [{Col, Dir} || {Col, Dir} <- SortSpec, is_integer(Col)],
    NewSortFun = imem_sql:sort_spec_fun(SortSpecExplicit, FullMap, FullMap),
    case sqlparse:parsetree(Query) of
        {ok,{[{{select, SelectSections},_}],_}} ->
            {fields, Flds} = lists:keyfind(fields, 1, SelectSections),
            {from, Tables} = lists:keyfind(from, 1, SelectSections),
            case can_expand(Flds, Tables, AllFields) of
                true ->
                    NewFields = [lists:nth(N,AllFields) || N <- Cols1],
                    NewSections0 = lists:keyreplace('fields', 1, SelectSections, {'fields',NewFields});
                false ->
                    NewSections0 = SelectSections
            end,
            OrderBy = imem_sql:sort_spec_order(SortSpec, FullMap, FullMap),
            NewSections1 = lists:keyreplace('order by', 1, NewSections0, {'order by',OrderBy}),
            NewSql = sqlparse:fold({select, NewSections1});
        _->
            NewSql = Query
    end,
    {ok, NewSql, NewSortFun}.

-spec expand_fields(tuple(), [binary()], [binary()], [binary()]) -> [binary()].
expand_fields(Connection, [<<"*">>], Tables, _) ->
    add_table_name([{T, Connection:describe(T, 'OCI_PTYPE_TABLE')} || T <- Tables, is_binary(T)]);
expand_fields(_, Fields, _, _) -> Fields.

add_table_name([]) -> [];
add_table_name([{TableName, {ok, FieldsTuple}} | Rest]) ->
    [iolist_to_binary([TableName, ".", F]) || {F, _Type, _Length} <- FieldsTuple] ++ add_table_name(Rest).

build_full_map(Clms, true) -> build_full_map(Clms, 1);
build_full_map(Clms, false) -> build_full_map(Clms, 0);
build_full_map(Clms, RowIdOffset) ->
    [#ddColMap{ tag = list_to_atom([$$|integer_to_list(T)])
              , name = binary_to_atom(Alias, utf8)
              , alias = Alias
              , tind = 1
              , cind = T+RowIdOffset
              , type = binstr
              , len = 300
              , prec = undefined }
     || {T, #stmtCol{alias = Alias}} <- lists:zip(lists:seq(1,length(Clms)), Clms)].

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
%   case sqlparse:parsetree(Sql) of
%       {ok,{[{{select, SelectSections},_}],_}} ->
%           FullMap = build_full_map(Clms),
%           io:format("The full map: ~p~n~n", [FullMap]),
%           try imem_sql:build_sort_fun(SelectSections, FullMap) of
%               Res ->
%                   io:format("The result of build_sort_fun ~p~n~n", [Res]),
%                   Res
%           catch Class:Error ->
%                   io:format("Error building the sort fun ~p:~p", [Class, Error]),
%                   fun(_Row) -> {} end
%           end;
%       _ ->
%           fun(_Row) -> {} end
%   end.

-spec cols_to_rec([tuple()]) -> [#stmtCol{}].
cols_to_rec([]) -> [];
cols_to_rec([{Alias,'SQLT_NUM',_Len,Prec,Scale}|Rest]) ->
    io:format("column: ~p type SQLT_NUM~n", [Alias]),
    [#stmtCol{ tag = Alias
             , alias = Alias
             , type = 'SQLT_NUM'
             , len = Prec
             , prec = Scale
             , readonly = false} | cols_to_rec(Rest)];
cols_to_rec([{Alias,Type,Len,Prec,_Scale}|Rest]) ->
    io:format("column: ~p type ~p~n", [Alias, Type]),
    [#stmtCol{ tag = Alias
             , alias = Alias
             , type = Type
             , len = Len
             , prec = Prec
             , readonly = false} | cols_to_rec(Rest)].

translate_datatype([], []) -> [];
translate_datatype([<<>> | RestRow], [#stmtCol{} | RestCols]) ->
    [<<>> | translate_datatype(RestRow, RestCols)];
translate_datatype([R | RestRow], [#stmtCol{type = 'SQLT_DAT'} | RestCols]) ->
    [dderloci_utils:ora_to_dderltime(R) | translate_datatype(RestRow, RestCols)];
translate_datatype([R | RestRow], [#stmtCol{type = 'SQLT_NUM'} | RestCols]) ->
    SizeNum = size(R),
    {Mantissa, Exponent} = dderloci_utils:oranumber_decode(<<SizeNum, R/binary>>),
    Number = imem_datatype:decimal_to_io(Mantissa, Exponent),
    [Number | translate_datatype(RestRow, RestCols)];
translate_datatype([R | RestRow], [#stmtCol{} | RestCols]) ->
    [R | translate_datatype(RestRow, RestCols)].

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
            [RowId | RestRow] = lists:reverse(Row),
            [{list_to_tuple([RowId | fix_null(RestRow, Columns)]), {}} | fix_row_format(Rest, Columns, ContainRowId)];
        true ->
            [{list_to_tuple(fix_null(lists:reverse(Row), Columns)), {}} | fix_row_format(Rest, Columns, ContainRowId)]
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
