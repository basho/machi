-record(state, {
          flu_name        :: atom(),
          proj_store      :: pid(),
          append_pid      :: pid(),
          tcp_port        :: non_neg_integer(),
          data_dir        :: string(),
          wedged = true   :: boolean(),
          etstab          :: ets:tid(),
          epoch_id        :: 'undefined' | machi_dt:epoch_id(),
          pb_mode = undefined  :: 'undefined' | 'high' | 'low',
          high_clnt       :: 'undefined' | pid(),
          dbg_props = []  :: list(), % proplist
          props = []      :: list()  % proplist
         }).

