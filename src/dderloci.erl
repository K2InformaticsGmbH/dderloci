-module(dderloci).

-include_lib("imem/include/imem_sql.hrl").

%% API
-export([
    exec/2,
    inject_rowid/1,
    edatetime_to_ora/1,
    filter_and_sort/6
]).

-record(qry, {parse_tree}).

-spec inject_rowid(binary()) -> {binary(), binary()}.
inject_rowid(Sql) ->
    case sqlparse:parsetree(Sql) of
        {ok,{[{{select, Args},_}],_}} ->
            {fields, Flds} = lists:keyfind(fields, 1, Args),
            {from, [FirstTable|_]=Forms} = lists:keyfind(from, 1, Args),
            NewFields =
                [list_to_binary(case FirstTable of
                    {as, _, Alias} -> [Alias, ".ROWID"];
                    {{as, _, Alias}, _} -> [Alias, ".ROWID"];
                    {Tab, _} -> [Tab, ".ROWID"];
                    Tab -> [Tab, ".ROWID"]
                end) | lists:flatten([case F of
                                        <<"*">> ->
                                        lists:reverse(lists:foldl(
                                            fun(T, AFields) ->
                                                case T of
                                                    {as, _, Alias} -> [list_to_binary([Alias,".*"]) | AFields];
                                                    {{as, _, Alias}, _} -> [list_to_binary([Alias, ".*"]) | AFields];
                                                    {Tab, _} -> [list_to_binary([Tab, ".*"]) | AFields];
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
            case sqlparse:fold(NPT) of
                {error, Reason} ->
                    io:format("Error trying to fold the query with rowid, reason: ~n**********~n~p~n**********", [Reason]),
                    {FirstTable, Sql, false};
                NewSql ->
                    {FirstTable, NewSql, true}
            end;
        _ -> {<<"">>, Sql, false}
    end.

exec({oci_port, _, _} = Connection, Sql) ->
    %% For now only the first table is counted.
    {TableName, NewSql, ContainRowId} = inject_rowid(Sql),
    Statement = Connection:prep_sql(NewSql),
    case Statement:exec_stmt() of
        {ok, Clms} ->
            % ROWID is hidden from columns
            io:format("The original columns, ~p~n", [Clms]),
            if
                ContainRowId ->
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
                                           ContainRowId ->
                                               [_|NewRow] = tuple_to_list(Row),
                                               translate_datatype(NewRow, NewClms);
                                           true ->
                                               translate_datatype(tuple_to_list(Row), NewClms)
                                       end
                               end
                         , stmtRef  = Statement
                         , sortFun  = SortFun
                         , sortSpec = []}
            , TableName
            , ContainRowId};
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
                {ok, Clms} ->
                    NewClms = cols_to_rec(Clms),
                    {ok
                    , #stmtResult{ stmtCols = NewClms
                                 , rowFun   = fun({Row, {}}) ->
                                                translate_datatype(tuple_to_list(Row), NewClms)
                                              end
                                 , stmtRef  = Statement1
                                 , sortFun  = fun(_Row) -> {} end
                                 , sortSpec = []}
                    , TableName
                    , false};
                Error ->
                    Statement1:close(),
                    Error
            end
    end.

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

filter_and_sort(_FilterSpec, SortSpec, Cols, Query, StmtCols, ContainRowId) ->
    io:format("The filterspec ~p~n The Sort spec ~p~n the col_order ~p~n the Query ~p~n the fullmap ~p~n", [_FilterSpec, SortSpec, Cols, Query, StmtCols]),
%    {ok,{[{{select, SelectSections},_}],_}} = sqlparse:parsetree(Sql),
%    OrderBy = imem_sql:sort_spec_order(SortSpec, FullMap, FullMap),
%    NewSections2 = lists:keyreplace('order by', 1, NewSections1, {'order by',OrderBy}),
    FullMap = build_full_map(StmtCols, ContainRowId),
    NewSortFun = imem_sql:sort_spec_fun(SortSpec, FullMap, FullMap),
    io:format("The return to the new sort fun... ~p~n", [NewSortFun]),
    {ok, Query, NewSortFun}.

build_full_map(Clms, true) -> build_full_map(Clms, 1);
build_full_map(Clms, false) -> build_full_map(Clms, 0);
build_full_map(Clms, RowIdOffset) ->
    [#ddColMap{ tag = list_to_atom([$$|integer_to_list(T)])
              , name = Alias
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
cols_to_rec([{Alias,Type,Len,Prec,_Scale}|Rest]) ->
    io:format("row received: ~p type ~p~n", [Alias, Type]),
    [#stmtCol{ tag = Alias
             , alias = Alias
             , type = Type
             , len = Len
             , prec = Prec
             , readonly = false} | cols_to_rec(Rest)].

translate_datatype(Row, Cols) ->
    [case C#stmtCol.type of
        'SQLT_DAT' ->
            << Century:8, Year:8, Month:8, Day:8, Hour:8, Minute:8, Second:8 >> = R,
            list_to_binary(io_lib:format("~2..0B-~2..0B-~2..0B~2..0B ~2..0B:~2..0B:~2..0B", [Day,Month,Century-100,Year-100,Hour-1,Minute-1,Second-1]));
        'SQLT_NUM' ->
             {Mantissa, Exponent} = dderloci_utils:oranumber_decode(R),
             imem_datatype:decimal_to_io(Mantissa, Exponent);
        _ -> R
    end
    || {C,R} <- lists:zip(Cols, Row)].

edatetime_to_ora({Meg,Mcr,Mil} = Now)
    when is_integer(Meg)
    andalso is_integer(Mcr)
    andalso is_integer(Mil) ->
    edatetime_to_ora(calendar:now_to_datetime(Now));
edatetime_to_ora({{FullYear,Month,Day},{Hour,Minute,Second}}) ->
    Century = (FullYear div 100) + 100,
    Year = (FullYear rem 100) + 100,
    << Century:8, Year:8, Month:8, Day:8, Hour:8, Minute:8, Second:8 >>.
