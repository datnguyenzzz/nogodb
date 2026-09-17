### Scanning operator (I/O-bound, should be ran on the separated threads)
[0] push through the dispatcher
[1] the DynamicJoinFilter optimizer injects the DynamicJoinPruner directly into the asynchronous background I/O scanner task at compile-time.
Symmetrically, at runtime:
  1. As the background I/O thread reads the page headers of the probe table, it queries the dispatcher:
    let bounds = dispatcher.get_join_bounds(join_id);
  2. It compares the page’s min/max metadata values against the active join boundaries.
  3. The Symmetrical Win: If they are disjoint, the I/O thread drops the page instantly! 
  4. Result: Symmetrically, the worker CPU Reactors only receive and decompress page blocks that have a high probability of containing matching join partners, achieving absolute peak memory and CPU execution speeds!