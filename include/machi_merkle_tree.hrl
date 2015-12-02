%% machi merkle tree records

-record(naive, {
          chunk_size = 1048576   :: pos_integer(), %% default 1 MB
          recalc = true          :: boolean(),
          root                   :: 'undefined' | binary(),
          lvl1 = []              :: [ binary() ],
          lvl2 = []              :: [ binary() ],
          lvl3 = []              :: [ binary() ],
          leaves = []            :: [ { Offset :: pos_integer(),
                                        Size :: pos_integer(),
                                        Csum :: binary()} ]
         }).

-record(mt, {
          filename               :: string(),
          tree                   :: #naive{},
          backend = 'naive'      :: 'naive'
         }).

