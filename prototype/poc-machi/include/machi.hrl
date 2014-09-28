-record(proj, {                                 % Projection
          epoch :: non_neg_integer(),
          all :: list(pid()),
          active :: list(pid())
         }).

