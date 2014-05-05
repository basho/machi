Shiny prototype/sandbox of distributed DBMS concepts.

Compile:

    ./rebar get-deps compile

Starting:

    erl -pa ebin deps/*/ebin -s dbms

Things to try:

Prettyprint a record:

    dbms:print_record().

Start some load:

    dbms:loadgen().

Show all the debug messages:

    lager:set_loglevel(lager_console_backend, debug).

Reset the loglevel because your console is flooding:

    lager:set_loglevel(lager_console_backend, info).

Show all the read-repair operations:

    lager:trace_console([{operation, repair}]).

Reset the traces:

    lager:clear_all_traces().

Show all the PUTs against vnode 13:

    lager:trace_console([{operation, put}, {id, 13}]).

Show all the keys being read between 500 and 600:

    lager:trace_console([{operation, get}, {key, '>', 500}, {key, '<', 600}]).

Trace specific GET/PUT operstions:

    dbms:traced_get(Key).
    dbms:traced_put(Key).

Manually erase a vnode:

    dbms_dynamo:wipe(VnodeID).
