-module(dderloci_utils).

-export([oranumber_decode/1
        ,oranumber_encode/1
        ,ora_to_dderltime/1
        ,dderltime_to_ora/1]).

-spec oranumber_decode(binary()) -> {integer(), integer()} | {error, binary()}.
oranumber_decode(<<1:8, _/binary>>) -> {0, 0};
oranumber_decode(<<Length:8, 1:1, OraExp:7, Rest/binary>>) -> % positive numbers
    Exponent = OraExp - 65,
    MLength = Length - 1,
    <<OraMantissa:MLength/binary, _/binary>> = Rest,
    ListOraMant = binary_to_list(OraMantissa),
    ListMantissa = lists:flatten([io_lib:format("~2.10.0B", [DD-1]) || DD <- ListOraMant]),
    Mantissa = list_to_integer(ListMantissa),
    LengthMant = length(ListMantissa),
    oraexp_to_imem_prec(Mantissa, Exponent, LengthMant);
oranumber_decode(<<Length:8, 0:1, OraExp:7, Rest/binary>>) -> % negative numbers
    Exponent = 62 - OraExp,
    MLength = Length - 2,
    <<OraMantissa:MLength/binary, 102, _/binary>> = Rest,
    ListOraMant = binary_to_list(OraMantissa),
    ListMantissa = lists:flatten([io_lib:format("~2.10.0B", [101-DD]) || DD <- ListOraMant]),
    Mantissa = -1 * list_to_integer(ListMantissa),
    LengthMant = length(ListMantissa),
    oraexp_to_imem_prec(Mantissa, Exponent, LengthMant);
oranumber_decode(_) -> {error, <<"invalid oracle number">>}.

-spec oraexp_to_imem_prec(integer(), integer(), integer()) -> {integer(), integer()}.
oraexp_to_imem_prec(Mantissa, Exponent, LengthMant) ->
    oraexp_to_imem_prec(Mantissa, Exponent, LengthMant, LengthMant rem 2, Mantissa rem 10).

-spec oraexp_to_imem_prec(integer(),integer(),integer(),integer(),integer()) -> {integer(), integer()}.
oraexp_to_imem_prec(Mantissa,Exponent,LengthMant,RemLength,0) -> {Mantissa div 10, (Exponent*-2) + LengthMant-3 + RemLength};
oraexp_to_imem_prec(Mantissa,Exponent,LengthMant,RemLength,_) -> {Mantissa       , (Exponent*-2) + LengthMant-2 + RemLength}.

-spec oranumber_encode(binary()) -> binary().
oranumber_encode(<<>>) -> <<>>;
oranumber_encode(NumberBin) ->
    {Mantissa, Exponent} = split_binary_number(NumberBin),
    oranumber_encode(Mantissa, Exponent).

-spec oranumber_encode(integer(), integer()) -> binary().
oranumber_encode(0, _) -> <<1, 128>>;
oranumber_encode(Mantissa, Exponent) when Mantissa > 0 ->
    io:format("The mantissa: ~p and the exponent: ~p~n", [Mantissa, Exponent]),
    NormMant = normalize_mantissa(Mantissa, Mantissa rem 100),
    io:format("The mantissa normalized: ~p~n", [NormMant]),
    Encoded = [Exponent + 193 | encode_mantissa(NormMant, 1, [])],
    list_to_binary([length(Encoded) | Encoded]);
oranumber_encode(Mantissa, Exponent) ->
    io:format("The mantissa: ~p and the exponent: ~p~n", [Mantissa, Exponent]),
    NormMant = normalize_mantissa(Mantissa, Mantissa rem 100),
    io:format("The mantissa normalized: ~p~n", [NormMant]),
    Encoded = [62 - Exponent | encode_mantissa(NormMant, 101, [102])],
    list_to_binary([length(Encoded) | Encoded]).

-spec encode_mantissa(integer(), 1 | 101, list()) -> list().
encode_mantissa(0, _, Result) -> Result;
encode_mantissa(Mantissa, Offset, Result) ->
    encode_mantissa(Mantissa div 100, Offset, [(Mantissa rem 100) + Offset | Result]).

-spec normalize_mantissa(integer(), integer()) -> integer().
normalize_mantissa(Mantissa, 0) ->
    NewMantissa = Mantissa div 100,
    normalize_mantissa(NewMantissa, NewMantissa rem 100);
normalize_mantissa(Mantissa, _Rest) -> Mantissa.

-spec split_binary_number(binary()) -> {integer(), integer()}.
split_binary_number(NumberBin) ->
    io:format("split the number :o~n"),
    case remove_trailing_zeros(NumberBin) of
        {negative, CleanNumber} ->
            io:format("negative number :o ~p~n", [CleanNumber]),
            {Mantissa, Exponent} = parts_to_integer(binary:split(CleanNumber, [<<".">>], [])),
            {Mantissa * -1, Exponent};
        CleanNumber ->
            io:format("clean number :o ~p~n", [CleanNumber]),
            parts_to_integer(binary:split(CleanNumber, [<<".">>], []))
    end.

-spec parts_to_integer([binary()]) -> {integer(), integer(), integer()}.
parts_to_integer([<<>>]) -> {0, 0};
parts_to_integer([IntPart, <<>>]) -> parts_to_integer([IntPart]);
parts_to_integer([<<>>, RealPart]) -> parts_to_integer([<<"0">>, RealPart]);
parts_to_integer([IntPart]) ->
    SizeMant = size(IntPart),
    Exponent = (SizeMant div 2) + (SizeMant rem 2) - 1,
    Mantissa = binary_to_integer(IntPart),
    {Mantissa, Exponent};
parts_to_integer([<<"0">>, RealPart]) ->
    SizeMant = size(RealPart),
    %% Since the numbers are encoded 2 by 2 we need to add the last 0 to make the length even
    Mantissa = binary_to_integer(RealPart) * (1 +  9 * (SizeMant rem 2)),
    NewSizeMant = size(integer_to_binary(Mantissa)),
    Exponent = (NewSizeMant div 2) + (NewSizeMant rem 2) - (SizeMant div 2) - (SizeMant rem 2) - 1,
    {Mantissa, Exponent};
parts_to_integer([IntPart, RealPart]) ->
    SizeInt = size(IntPart),
    SizeReal = size(RealPart),
    Exponent = (SizeInt div 2) + (SizeInt rem 2) - 1,
    Mantissa = binary_to_integer(<<IntPart/binary, RealPart/binary>>) * (1 + 9 * (SizeReal rem 2)),
    {Mantissa, Exponent}.

-spec remove_trailing_zeros(binary()) -> binary() | {negative, binary()}.
remove_trailing_zeros(<<$-, RestNumber/binary>>) ->
    Clean = remove_trailing_zeros(RestNumber),
    {negative, Clean};
remove_trailing_zeros(OrigBin) ->
    LeftRemoved = lists:dropwhile(fun(X) -> X =:= $0 end, binary_to_list(OrigBin)),
    case lists:member($., LeftRemoved) of
        true ->
            RightRemoved = lists:dropwhile(fun(X) -> X =:= $0 end, lists:reverse(LeftRemoved)),
            list_to_binary(lists:reverse(RightRemoved));
        false ->
            list_to_binary(LeftRemoved)
    end.

-spec ora_to_dderltime(binary()) -> binary().
ora_to_dderltime(<< Year:2/little-unit:8, Month:8, Day:8, Hour:8, Minute:8, Second:8, _/binary >>) ->
    iolist_to_binary(io_lib:format("~2..0B.~2..0B.~4..0B ~2..0B:~2..0B:~2..0B", [Day,Month,Year,Hour,Minute,Second])).

-spec dderltime_to_ora(binary()) -> binary().
dderltime_to_ora(<<>>) -> <<>>;
dderltime_to_ora(DDerlTime) ->
    <<DBin:2/binary, $., MBin:2/binary, $., YBin:4/binary, 32, HBin:2/binary, $:, MinBin:2/binary, $:, SecBin:2/binary>> = DDerlTime,
    Year = binary_to_integer(YBin),
    Month = binary_to_integer(MBin),
    Day = binary_to_integer(DBin),
    Hour = binary_to_integer(HBin),
    Minute = binary_to_integer(MinBin),
    Second = binary_to_integer(SecBin),
    <<Year:2/little-unit:8, Month, Day, Hour, Minute, Second>>.
