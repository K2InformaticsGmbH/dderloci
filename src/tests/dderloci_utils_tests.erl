-module(dderloci_utils_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

oranumber_decode_positive_int_test_() ->
    [
        ?_assertEqual({0, 0}, dderloci_utils:oranumber_decode(<<1, 128>>)), % num 0
        ?_assertEqual({5, 0}, dderloci_utils:oranumber_decode(<<2, 193, 6>>)), % num 5
        ?_assertEqual({1, -4}, dderloci_utils:oranumber_decode(<<2, 195, 2>>)), % num 10000
        ?_assertEqual({32, 0}, dderloci_utils:oranumber_decode(<<2, 193, 33>>)), % num 32
        ?_assertEqual({1, -5}, dderloci_utils:oranumber_decode(<<2, 195, 11>>)), % num 100000
        ?_assertEqual({2767, 0}, dderloci_utils:oranumber_decode(<<3, 194, 28, 68>>)) % num 2767
    ].

oranumber_decode_negative_int_test_() ->
    [
        ?_assertEqual({-5, 0}, dderloci_utils:oranumber_decode(<<3, 62, 96, 102>>)), % num -5
        ?_assertEqual({-112, 0}, dderloci_utils:oranumber_decode(<<4, 61, 100, 89, 102>>)), % num -112
        ?_assertEqual({-2767, 0}, dderloci_utils:oranumber_decode(<<4, 61, 74, 34, 102>>)), % num -2767
        ?_assertEqual({-11, -4}, dderloci_utils:oranumber_decode(<<3, 60, 90, 102>>)), % num -110000
        ?_assertEqual({-9999, -4}, dderloci_utils:oranumber_decode(<<4, 59, 2, 2, 102>>)), % -99990000
        ?_assertEqual({-1000001, 0}, dderloci_utils:oranumber_decode(<<6, 59, 100, 101, 101, 100, 102>>)), % num -1000001
        ?_assertEqual({-1, -6}, dderloci_utils:oranumber_decode(<<3, 59, 100, 102>>)) % num -1000000
    ].

oranumber_decode_positive_real_test_() ->
    [
        ?_assertEqual({1, 1}, dderloci_utils:oranumber_decode(<<2, 192, 11>>)), % num 0.1
        ?_assertEqual({1, 6}, dderloci_utils:oranumber_decode(<<2, 190, 2>>)), % num 0.000001
        ?_assertEqual({333122, 6}, dderloci_utils:oranumber_decode(<<4, 192, 34, 32, 23>>)), % num 0.333122
        ?_assertEqual({55, 1}, dderloci_utils:oranumber_decode(<<3, 193, 6, 51>>)), % num 5.5
        ?_assertEqual({105, 1}, dderloci_utils:oranumber_decode(<<3, 193, 11, 51>>)) % num 10.5
    ].

oranumber_decode_negative_real_test_() ->
    [
        ?_assertEqual({-1, 1}, dderloci_utils:oranumber_decode(<<3, 63, 91, 102>>)), % num -0.1
        ?_assertEqual({-1, 6}, dderloci_utils:oranumber_decode(<<3, 65, 100, 102>>)), % num -0.000001
        ?_assertEqual({-333122, 6}, dderloci_utils:oranumber_decode(<<5, 63, 68, 70, 79, 102>>)), % num -0.333122
        ?_assertEqual({-55, 1}, dderloci_utils:oranumber_decode(<<4, 62, 96, 51, 102>>)), % num -5.5
        ?_assertEqual({-105, 1}, dderloci_utils:oranumber_decode(<<4, 62, 91, 51, 102>>)) % num -10.5
    ].

-endif.
