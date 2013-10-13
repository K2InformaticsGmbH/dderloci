-module(dderloci).

%% API
-export([
    prep_sql/2,
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

prep_sql(Sql, {oci_port, _, _} = Statement) ->
    NewSql = inject_rowid(Sql),
    Statement:prep_sql(NewSql, Statement).
