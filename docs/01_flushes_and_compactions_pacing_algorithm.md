## Problem:

- Flushes mutable to immutable memtables, contend with foreground traffic, resulting in write and read latency
  spikes. Without throttling the rate of flushes they occur "as fast as possible" (controlled via `WriteOpts.BlockSize` and `WriteOpts.BlockSizeThreshold`).
  This instantaneous usage of CPU and disk IO results in potentially huge latency spikes
  for writes and reads which occur in parallel

- We can have a rate limiter to limit number of bytes per sec to limit the flush speed.
Though it's simple to implement and understand, however it is hard to configure and choose the 
optimal value, because:
    1) If the rate limit is configured too low, the memtables and L0 files to pile up, and eventually 
       the DB writes will stall and write throughput will be affected.
    2) If the rate limit is configured too high, the write and read latency spikes will persist.
    3) A different configuration is needed per system depending on the speed of the storage device.
    4) Write rates typically do not stay the same throughout the lifetime of the DB (higher throughput 
       during certain times of the day, etc) but the rate limit cannot be configured during runtime.

## Solutions:

- RocksDB: Offers auto-tuned rate limiter, by implementing the 
additive-increase/multiplicative-decrease (AIMD) algorithm
  - References:
    - https://en.wikipedia.org/wiki/Additive_increase/multiplicative_decrease
    - https://rocksdb.org/blog/2017/12/18/17-auto-tuned-rate-limiter.html