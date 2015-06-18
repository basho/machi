%% @doc Generates a codec-mapping module from a CSV mapping of message
%% codes to messages in .proto files.
-module(riak_pb_msgcodegen).
-export([preprocess/2,
         clean/2]).

%% -include_lib("rebar/include/rebar.hrl").
-define(FAIL, rebar_utils:abort()).
-define(ABORT(Str, Args), rebar_utils:abort(Str, Args)).

-define(CONSOLE(Str, Args), io:format(Str, Args)).

-define(DEBUG(Str, Args), rebar_log:log(debug, Str, Args)).
-define(INFO(Str, Args), rebar_log:log(info, Str, Args)).
-define(WARN(Str, Args), rebar_log:log(warn, Str, Args)).
-define(ERROR(Str, Args), rebar_log:log(error, Str, Args)).

-define(FMT(Str, Args), lists:flatten(io_lib:format(Str, Args))).

-define(MODULE_COMMENTS(CSV),
        ["%% @doc This module contains message code mappings generated from\n%% ",
         CSV,". DO NOT EDIT OR COMMIT THIS FILE!\n"]).

%% ===================================================================
%% Public API
%% ===================================================================
preprocess(Config, _AppFile) ->
    case rebar_config:get(Config, current_command, undefined) of
        'compile' ->
            case rebar_utils:find_files("src", ".*\\.csv") of
                [] ->
                    ok;
                FoundFiles ->
                    Targets = [{CSV, fq_erl_file(CSV)} || CSV <- FoundFiles ],
                    generate_each(Config, Targets)
            end;
        _Else -> ok
    end,
    {ok, Config, []}.

clean(_Config, _AppFile) ->
    CSVs = rebar_utils:find_files("src", ".*\\.csv"),
    ErlFiles = [fq_erl_file(CSV) || CSV <- CSVs],
    case ErlFiles of
        [] -> ok;
        _ -> delete_each(ErlFiles)
    end.

%% ===================================================================
%% Internal functions
%% ===================================================================

generate_each(_Config, []) ->
    ok;
generate_each(Config, [{CSV, Erl}|Rest]) ->
    case is_modified(CSV, Erl) of
        false ->
            ok;
        true ->
            Tuples = load_csv(CSV),
            Module = generate_module(mod_name(CSV), Tuples),
            Formatted = erl_prettypr:format(Module),
            ok = file:write_file(Erl, [?MODULE_COMMENTS(CSV), Formatted]),
            ?CONSOLE("Generated ~s~n", [Erl])
    end,
    generate_each(Config, Rest).

is_modified(CSV, Erl) ->
    not filelib:is_regular(Erl) orelse
        filelib:last_modified(CSV) > filelib:last_modified(Erl).

mod_name(SourceFile) ->
    filename:basename(SourceFile, ".csv").

fq_erl_file(SourceFile) ->
    filename:join(["src", erl_file(SourceFile)]).

erl_file(SourceFile) ->
    mod_name(SourceFile) ++ ".erl".

load_csv(SourceFile) ->
    {ok, Bin} = file:read_file(SourceFile),
    csv_to_tuples(unicode:characters_to_list(Bin, latin1)).

csv_to_tuples(String) ->
    Lines = string:tokens(String, [$\r,$\n]),
    [ begin
          [Code, Message, Proto] = string:tokens(Line, ","),
          {list_to_integer(Code), string:to_lower(Message), Proto ++ "_pb"}
      end
     || Line <- Lines, length(Line) > 0 andalso hd(Line) /= $#].

generate_module(Name, Tuples) ->
    %% TODO: Add generated doc comment at the top
    Mod = erl_syntax:attribute(erl_syntax:atom(module),
                               [erl_syntax:atom(Name)]),
    ExportsList = [
                    erl_syntax:arity_qualifier(erl_syntax:atom(Fun), erl_syntax:integer(1))
                    || Fun <- [msg_type, msg_code, decoder_for] ],

    Exports = erl_syntax:attribute(erl_syntax:atom(export),
                                   [erl_syntax:list(ExportsList)]),

    Clauses = generate_msg_type(Tuples) ++
              generate_msg_code(Tuples) ++
              generate_decoder_for(Tuples),

    erl_syntax:form_list([Mod, Exports|Clauses]).

generate_decoder_for(Tuples) ->
    Spec = erl_syntax:text("-spec decoder_for(non_neg_integer()) -> module().\n"),
    Name = erl_syntax:atom(decoder_for),
    Clauses = [
                erl_syntax:clause([erl_syntax:integer(Code)],
                                  none,
                                  [erl_syntax:atom(Mod)])
                || {Code, _, Mod} <- Tuples ],
    [ Spec, erl_syntax:function(Name, Clauses) ].

generate_msg_code(Tuples) ->
    Spec = erl_syntax:text("-spec msg_code(atom()) -> non_neg_integer()."),
    Name = erl_syntax:atom(msg_code),
    Clauses = [
               erl_syntax:clause([erl_syntax:atom(Msg)], none, [erl_syntax:integer(Code)])
               || {Code, Msg, _} <- Tuples ],
    [ Spec, erl_syntax:function(Name, Clauses) ].

generate_msg_type(Tuples) ->
    Spec = erl_syntax:text("-spec msg_type(non_neg_integer()) -> atom()."),
    Name = erl_syntax:atom(msg_type),
    Clauses = [
               erl_syntax:clause([erl_syntax:integer(Code)], none, [erl_syntax:atom(Msg)])
               || {Code, Msg, _} <- Tuples ],
    CatchAll = erl_syntax:clause([erl_syntax:underscore()], none, [erl_syntax:atom(undefined)]),
    [ Spec, erl_syntax:function(Name, Clauses ++ [CatchAll]) ].

delete_each([]) ->
    ok;
delete_each([File | Rest]) ->
    case file:delete(File) of
        ok ->
            ok;
        {error, enoent} ->
            ok;
        {error, Reason} ->
            ?ERROR("Failed to delete ~s: ~p\n", [File, Reason])
    end,
    delete_each(Rest).
