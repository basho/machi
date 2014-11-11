{application,corfurl,
             [{description,"Quick prototype of CORFU in Erlang."},
              {vsn,"0.0.0"},
              {applications,[kernel,stdlib,lager]},
              {mod,{corfurl_unfinished_app,[]}},
              {registered,[]},
              {env,[{ring_size,32}]},
              {modules,[corfurl,corfurl_client,corfurl_flu,corfurl_sequencer,
                        corfurl_util]}]}.
