-module(dderloci_utils).

-export([oranumber_decode/1]).

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
oraexp_to_imem_prec(Mantissa,Exponent,LengthMant,0,0) -> {Mantissa div 10, (Exponent*-2) + LengthMant-3};
oraexp_to_imem_prec(Mantissa,Exponent,LengthMant,0,_) -> {Mantissa       , (Exponent*-2) + LengthMant-2};
oraexp_to_imem_prec(Mantissa,Exponent,LengthMant,_,0) -> {Mantissa div 10, (Exponent*-2) + LengthMant-2};
oraexp_to_imem_prec(Mantissa,Exponent,LengthMant,_,_) -> {Mantissa       , (Exponent*-2) + LengthMant-1}.
