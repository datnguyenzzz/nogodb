# Go Block Cache

A high-performance, thread-safe block cache implementation built for nogoDB. This library provides a concurrent, 
dynamic-sized hash table with configurable cache replacement policies, optimized for database block caching scenarios.
The hash tables implementation is based on: "Dynamic-Sized Nonblocking Hash Tables", by Yujie Liu, Kunlong Zhang, and 
Michael Spear. ACM Symposium on Principles of Distributed Computing, Jul 2014.

## Features

- **Concurrent Dynamic Hash Table**: Lock-free operations with automatic resizing
- **Multiple Cache Policies**: LRU (implemented), Clock-Pro (planned)
- **Memory Management**: Automatic eviction with configurable capacity limits
- **Thread Safety**: Full concurrency support for read/write operations
- **LazyValue Interface**: Memory-efficient value loading with reference counting
- **High Performance**: Optimized for database workloads with minimal allocation overhead

## Key Concepts

Blocks are keyed by a `(fileNum, key)` pair where:
- `fileNum`: Identifies the source file (e.g., SSTable file number)  
- `key`: Unique identifier within the file (e.g., block offset)
- Keys are unique for the lifetime of the cache instance due to immutable files and non-reused file numbers


## Benchmark

```
goos: darwin
goarch: arm64
pkg: github.com/datnguyenzzz/nogodb/lib/go-block-cache
cpu: Apple M1 Pro

Benchmark_Ristretto_Cache_Add_Read-10          	  471723	      2593 ns/op	   2096952 mem_footprint_in_bytes	     462 B/op	       7 allocs/op
Benchmark_Ristretto_Cache_Add-10               	  479282	      2520 ns/op	   2096952 mem_footprint_in_bytes	     546 B/op	       8 allocs/op
Benchmark_Ristretto_Cache_Read-10              	33852442	     33.05 ns/op	   2096952 mem_footprint_in_bytes	       6 B/op	       0 allocs/op
Benchmark_Ristretto_Cache_Add_Read_Async-10    	 2836270	     400.1 ns/op	   2096952 mem_footprint_in_bytes	     360 B/op	       2 allocs/op
Benchmark_Ristretto_Cache_Add_Async-10         	 3034792	     374.5 ns/op	   2096952 mem_footprint_in_bytes	     360 B/op	       2 allocs/op
Benchmark_Ristretto_Cache_Read_Async-10        	15597987	     80.80 ns/op	   2096952 mem_footprint_in_bytes	       0 B/op	       0 allocs/op

Benchmark_NogoDB_Cache_Add_Read-10             	  546520	      2258 ns/op	   2097152 mem_footprint_in_bytes	     393 B/op	       4 allocs/op
Benchmark_NogoDB_Cache_Add-10                  	  568741	      2192 ns/op	   2097152 mem_footprint_in_bytes	     385 B/op	       3 allocs/op
Benchmark_NogoDB_Cache_Read-10                 	29273527	     41.02 ns/op	   2097152 mem_footprint_in_bytes	       0 B/op	       0 allocs/op
Benchmark_NogoDB_Cache_Add_Read_Async-10       	 1423645	     838.4 ns/op	   2097152 mem_footprint_in_bytes	     277 B/op	       2 allocs/op
Benchmark_NogoDB_Cache_Add_Async-10            	 1482656	     841.6 ns/op	   2097152 mem_footprint_in_bytes	     272 B/op	       1 allocs/op
Benchmark_NogoDB_Cache_Read_Async-10           	 4317998	     273.1 ns/op	   2097152 mem_footprint_in_bytes	       0 B/op	       0 allocs/op
PASS
ok  	github.com/datnguyenzzz/nogodb/lib/go-block-cache	226.049s
```

## Architecture Overview

### Core Components

The cache consists of several key components working together:

1. **HashMap (`hashMap`)**: Main interface providing thread-safe operations
2. **State Management (`state`)**: Handles bucket array resizing and migration 
3. **Bucket (`bucket`)**: Individual hash buckets containing sorted key-value nodes
4. **KV Nodes (`kvType`)**: Individual cache entries with reference counting
5. **LRU Cache (`lru`)**: Manages memory pressure and eviction policies
6. **LazyValue (`handle`)**: Memory-efficient value access with cleanup

### Concurrent Dynamic-Sized Hash Tables

The hash table implementation is based on **Dynamic-Sized Nonblocking Hash Tables** research, providing:
- Lock-free bucket operations during normal access
- Automatic resizing when load thresholds are exceeded
- Non-blocking reads during resize operations
- Sorted bucket contents for efficient lookups

#### Set Operation Flow:
```
Set(fileNum, key, value):
   1. hash(fileNum, key) → bucket_id
   2. Locate bucket in current state
   3. Insert/update (fileNum,key,value) in sorted order within bucket
   4. Update LRU cache and handle memory pressure
   5. Trigger resize if overflow thresholds exceeded
```

#### Get Operation Flow:
```
Get(fileNum, key):
   1. hash(fileNum, key) → bucket_id  
   2. Binary search within sorted bucket for key
   3. Increment reference count and return LazyValue
   4. Update LRU position for cache hit
```

### Dynamic Resizing

#### Growth Operation
When buckets become overloaded (> 32 entries) or global thresholds exceeded:

```
Original State (N buckets):        After Growth (2N buckets):
+--------------+------+            +--------------+------+
|   bucket 1   | data |            |   bucket 1   | data |  
+--------------+------+            +--------------+------+
|    ...       |      |            |    ...       |      |
+--------------+------+            +--------------+------+ 
| bucket I % N | data | -------->  | bucket I     | data |  ← Redistributed
+--------------+------+            +--------------+------+
|    ...       |      |            |    ...       |      |
+--------------+------+            +--------------+------+
|   bucket N   | data |            | bucket I+N   | data |  ← New location
+--------------+------+            +--------------+------+
                                   |    ...       |      |
                                   +--------------+------+
                                   |  bucket 2N   | data |
                                   +--------------+------+
```

#### Shrink Operation  
When buckets become underutilized (< N/2 total entries):

```
Original State (N buckets):        After Shrink (N/2 buckets):
+--------------+------+            +--------------+------+
|   bucket 1   | data |            |   bucket 1   | data |
+--------------+------+            +--------------+------+
|    ...       |      |            |    ...       |      |
+--------------+------+            +--------------+------+
|   bucket I   | data | -------->  |   bucket I   | merged|  ← Combined data
+--------------+------+            +--------------+------+
|    ...       |      |            |    ...       |      |
+--------------+------+            +--------------+------+
| bucket N/2+I | data |            |  bucket N/2  | data |
+--------------+------+            +--------------+------+
|    ...       |      |
+--------------+------+
|   bucket N   | data |
+--------------+------+
```

## Cache Replacement Policies

### LRU (Least Recently Used) - **Implemented**

The LRU implementation uses a doubly-linked list to track access patterns:

- **Structure**: Circular doubly-linked list with dummy head node
- **Promotion**: Recently accessed items move to front of list  
- **Eviction**: Items are removed from the tail when capacity exceeded
- **Thread Safety**: Protected by mutex during list operations
- **Memory Tracking**: Automatic size tracking with atomic operations


### Clock-Pro - **Planned**

Clock-Pro is a patent-free alternative to Adaptive Replacement Cache (ARC):
- Approximation of LIRS (Low Inter-reference Recency Set) algorithm
- Better performance than LRU for mixed access patterns  
- Handles both temporal and spatial locality
- Reference: [USENIX 2005 Paper](http://static.usenix.org/event/usenix05/tech/general/full_papers/jiang/jiang_html/html.html)


## Usage Examples

### Basic Usage

```go
package main

import (
    "fmt"
    cache "path/to/go-block-cache"
)

func main() {
    // Create cache with LRU policy and 10MB capacity
    blockCache := cache.NewMap(
        cache.WithCacheType(cache.LRU),
        cache.WithMaxSize(10 * 1024 * 1024), // 10MB
    )
    defer blockCache.Close(false)

    // Store a block
    fileNum, blockOffset := uint64(1), uint64(1024)
    blockData := []byte("block content...")
    
    success := blockCache.Set(fileNum, blockOffset, blockData)
    if !success {
        fmt.Println("Failed to cache block")
        return
    }

    // Retrieve the block
    lazyValue, found := blockCache.Get(fileNum, blockOffset)
    if !found {
        fmt.Println("Block not found in cache")
        return
    }
    defer lazyValue.Release() // Important: always release!

    // Access the actual data
    cachedData := lazyValue.Load()
    fmt.Printf("Retrieved block: %s\n", string(cachedData))
}
```

### High-Concurrency Pattern

```go
func workerRoutine(cache IBlockCache, fileNum uint64, wg *sync.WaitGroup) {
    defer wg.Done()
    
    for i := 0; i < 1000; i++ {
        blockOffset := uint64(i * 4096) // 4KB blocks
        data := make([]byte, 4096)
        
        // Store block
        cache.Set(fileNum, blockOffset, data)
        
        // Immediately try to read it back
        if lazyValue, ok := cache.Get(fileNum, blockOffset); ok {
            // Use the data...
            _ = lazyValue.Load()
            lazyValue.Release()
        }
        // Note: May miss due to immediate LRU eviction under memory pressure
    }
}
```