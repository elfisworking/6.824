Test (2A): initial election ...
  ... Passed --   3.1  3   56   14356    0
Test (2A): election after network failure ...
  ... Passed --   4.5  3  118   21630    0
Test (2A): multiple elections ...
  ... Passed --   5.9  7  678  126530    0
Test (2B): basic agreement ...
  ... Passed --   0.9  3   22    5702    3
Test (2B): RPC byte count ...
  ... Passed --   2.6  3   70  118726   11
Test (2B): agreement despite follower disconnection ...
  ... Passed --   6.1  3  133   32220    8
Test (2B): no agreement if too many followers disconnect ...
  ... Passed --   3.8  5  205   40344    3
Test (2B): concurrent Start()s ...
  ... Passed --   0.7  3   12    3094    6
Test (2B): rejoin of partitioned leader ...
  ... Passed --   6.5  3  197   44179    4
Test (2B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  27.1  5 2426  508146  103
Test (2B): RPC counts aren't too high ...
  ... Passed --   2.2  3   42   11154   12
Test (2C): basic persistence ...
  ... Passed --   4.6  3   95   22922    6
Test (2C): more persistence ...
  ... Passed --  22.0  5 1368  252664   16
Test (2C): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   3.5  3   46   10630    4
Test (2C): Figure 8 ...
  ... Passed --  33.9  5  847  173503   23
Test (2C): unreliable agreement ...
  ... Passed --  10.6  5  619  172552  256
Test (2C): Figure 8 (unreliable) ...
  ... Passed --  38.7  5 3082  570262   55
Test (2C): churn ...
  ... Passed --  16.5  5 1141  295323  370
Test (2C): unreliable churn ...
  ... Passed --  16.3  5  876  212036  170
Test (2D): snapshots basic ...
  ... Passed --   6.9  3  196   62632  251
Test (2D): install snapshots (disconnect) ...
  ... Passed --  64.2  3 1577  399597  407
Test (2D): install snapshots (disconnect+unreliable) ...
  ... Passed --  113.5  3 2732  632470  380
Test (2D): install snapshots (crash) ...
  ... Passed --  38.6  3  909  235441  378
Test (2D): install snapshots (unreliable+crash) ...
  ... Passed --  72.5  3 1577  379583  411
PASS
ok      6.824/raft      505.034s