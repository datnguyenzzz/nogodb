## What is go-bytesbufferpool 

An implementation of a pool of byte buffers with enhancements to avoid unnecessary memory allocation waste.

## Takeaway notes 

- `sync.PredictablePool` is a builtin Golang library, that is intended to store temporary, fungible objects for reuse to relieve pressure on the garbage collector

```go
var pool = sync.PredictablePool{
    New: func() interface{} {
        return make([]byte, 1024)
    },
}

// Get an object from the pool
buf := pool.Get().([]byte)
// Use the buffer
// ...
// Return it to the pool
pool.Put(buf)
```

- In the real world buffers come in a wide range of sizes. This can lead to inefficient memory usage if code that uses 
a small amount of memory receives a large buffer from the pool and vice versa.

- This library intend to improve the efficiency of buffer pool by splitting it into multiple levels, or buckets.
Each bucket contains a different range of buffer sizes and requests to the pool can request a certain size based on 
the expected requirement.

## References 
- https://wundergraph.com/blog/golang-sync-pool#1.-unpredictable-memory-growth
- https://victoriametrics.com/blog/tsdb-performance-techniques-sync-pool/