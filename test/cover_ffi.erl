-module(cover_ffi).
-export([find_files/2, start_coverage/0, compile_coverage/1, stop_coverage/0]).

find_files(Pattern, In) ->
    Results = filelib:wildcard(binary_to_list(Pattern), binary_to_list(In)),
    lists:map(fun list_to_binary/1, Results).

start_coverage() ->
    cover:start(),
    nil.

compile_coverage(Mod) ->
    case cover:compile_beam(Mod) of
        {ok, _} ->
            {ok, nil};
        {error, not_main_node} ->
            {error, <<"not_main_node">>};
        {error, _Mod} ->
            {error, <<"invalid_module">>}
    end.

stop_coverage() ->
    case cover:analyse() of
        {result, Values, Errors} ->
            NewValues =
                [ {Mod, {Cov, NotCov}} || {{Mod,_F,_A}, {Cov, NotCov}} <- Values ],
            NewErrors =
                [ {Mod, {0, 0}} || {not_cover_compiled, Mod} <- Errors ],
            MapValues =
                lists:foldl(fun({Mod, {Cov, NotCov}}, Acc) ->
                                MergeFun = fun({OldCov, OldNotCov}) ->
                                    NewCov = OldCov + Cov,
                                    NewNotCov = OldNotCov + NotCov,
                                    {NewCov, NewNotCov}
                                    end,
                        maps:update_with(Mod, MergeFun, {Cov, NotCov}, Acc)
                    end, #{}, NewValues ++ NewErrors),
            Calc = fun(Cov, NotCov) -> Cov / (Cov + NotCov) * 100 end,
            Get = fun(Key, List) -> proplists:get_value(Key, List) end,
            ModInfo = fun(Mod) -> filename:basename(Get(source, Get(compile, Mod:module_info()))) end,
            Return = [ {cover, ModInfo(Mod), Cov, NotCov, Calc(Cov, NotCov)} || {Mod, {Cov, NotCov}} <- maps:to_list(MapValues) ],
            NSize = lists:max([string:len(Mod) || {cover, Mod, _, _, _} <- Return]),
            Size = integer_to_list(NSize),
            io:fwrite("\n\nTest coverage:\n\n"),
            io:format("~-" ++ Size ++ "s ~7s (~s)\n", ["Module", "Perc%", "Covered/Total"]),
            io:format([
                lists:duplicate(NSize, "-"), " ",
                lists:duplicate(7, "-"), " ",
                lists:duplicate(15, "-"), "\n"
            ]),
            lists:foreach(fun(Cover) -> print_cover(Cover, Size) end, Return),
            {Cov, NotCov} =
                lists:foldl(fun({cover, _Mod, Cov, NotCov, _Percent}, {CovAcc, NotCovAcc}) ->
                    {CovAcc + Cov, NotCovAcc + NotCov}
                end, {0, 0}, Return),
            io:format("~n~6.2f% (~b/~b)~n~n", [Calc(Cov, NotCov), Cov, Cov + NotCov]),
            nil;

        {error, not_main_node} ->
            nil
    end.

print_cover({cover, Mod, Cov, NotCov, Percent}, Size) ->
    Total = Cov + NotCov,
    GleamMod = filename_to_module(Mod),
    io:format("~-" ++ Size ++ "s ~6.2f% (~b/~b)~n", [GleamMod, Percent, Cov, Total]),
    nil.

filename_to_module(ModuleName) ->
    [ModName|_] = string:split(ModuleName, ".gleam"),
    [FinalModName|_] = string:split(ModName, ".erl"),
    string:replace(FinalModName, "@", "/", all).
