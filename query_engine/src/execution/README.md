#### 0. Core Pillars of the Architecture

```
       [ I/O Scan data ]              [ Scheduler ]
      (Un-pinned thread)                    │
              |                             ▼  (Register Pipelines & Dependency DAG)
              └----------------------▶[ DISPATCHER ]
                                            │
                ┌───────────────────────────┼───────────────────────────┐
                ▼                           ▼                           ▼
         [ NUMA 0 Queue ]            [ NUMA 1 Queue ]            [ NUMA 2 Queue ]
        (Morsel, Morsel...)         (Morsel, Morsel...)         (Morsel, Morsel...)
                ▲                           ▲                           ▲
                │ (Pull Local Work First)   │                           │
          [ Worker 0 ]                [ Worker 1 ]                [ Worker 2 ]
       (Pinned to Core 0)          (Pinned to Core 1)          (Pinned to Core 2)
                │                           │                           │
                └──────(If Local Empty, Steal from other Node)──────────┘
```

#### 1. Push-Based Vectorized Execution Loop

A conventional, pull-based Volcano style iterator (`next()`) has several shortcomings due to its virtual call overhead, compiler opt barriers, and cache-miss issues.
* Vectorized processing: data chunks are processed in small, machine word sized, tightly packed vectors with exactly VECTOR_SIZE = 2048 rows which can mostly fit inside CPU's L1/L2 cache lines-these hardware instructions operate on vectors (SIMD) directly in registers (arithmetic, filter, join, hash operations etc.)
* Push based pipeline: each chunk is passed linearly through a chain of operators (Source -> OP1 -> OP2 -> Sink) once.
* Zero allocation filtering: filter operator is only responsible for producing a Selection Vector (bitmap) containing indices of rows that pass the predicate – it doesn't copy data at all; in-effect 100% zero-copy filtering.

#### 2. Thread-per-Core & Share-Nothing (no locks)

Typical multi-threaded engines in modern database systems use a work-stealing pool. But pool workers contend a common lock for dispatching their task as well as writing to shared memory resulting in performance penalties from context switching, cache coherency, and lock convoying.
* Pinning OS threads tocores: engine creates as many OS threads as there are physical cores (N threads for N cores) & pins them to specific hardware cores using core_affinity library.
* Tokio [LocalRuntime](https://docs.rs/tokio/latest/tokio/runtime/struct.LocalRuntime.html): each thread has its own dedicated, separate `LocalRuntime` that can only run tasks belonging to itself-threads' tasks are inherently `!Send` and are never scheduled to other cores.
* Share Nothing state (eliminating writes, locks): worker threads perform most of their writes (e.g. Writing into the thread local aggregation hash maps) to dedicated, local storage. No mutexes & no write locks are needed. Global state only gets merged once on completion using a parallel reduction tree (combine method).

#### 3. Morsel-driven Parallelism

The approach of worker threads asking the central dispatcher for more work whenever their buffer is empty still causes contention when the dispatcher serves worker threads; to remove that, the data is split into bigger, dynamically sized pieces called Morsels, it equals precisely MORSEL_SIZE = 100,000 rows.

#### 4. NUMA Aware Work-Stealing Controller

For modern multi-socket CPUs, NUMA memory access patterns play a crucial role where remote sockets have 3x lower latency.
* The Dispatcher separates pending Morsels to NUMA node queues and creates slices using First-Touch allocation so the columns always reside in the local memory.
* A thread working in NUMA Node 0 will always consume morsels from NUMA Node 0-this makes its cache access speed much faster as it hits the local RAM.
* Stealing between NUMA nodes only happens when a core finishes its local work and its own NUMA queue is empty and all other NUMA queues are empty too. This strategy minimizes latency impact from remote access, paying the 3x latency penalty less frequently.

#### 5. Asynchronous I/O Decoupling (Double-Buffering)

If an execution core blocks on Disk I/O (such as waiting for sectors during a table scan), the entire core stalls, wasting CPU cycles and evicting L1/L2 cache lines.
* Non-Blocking Scans: Pinned CPU Reactor threads *never* execute synchronous disk-read system calls. Our physical scan source operator acts as a non-blocking consumer pulling from a thread-local vector queue.
* Asynchronous I/O Pool: A dedicated background thread pool handles raw file reading and decompression (Zstd decoding) in parallel.
* Double-Buffering Overlap: While the CPU is executing queries on Morsel N in local RAM, the I/O pool is pre-fetching and compiling Morsel N+1 into memory. When the reactor finishes the current batch, it performs a nanosecond pointer-swap to acquire the next pre-fetched batch, completely hiding I/O latency behind CPU execution time!