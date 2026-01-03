# Adaptive Radix Tree Implementation in Go

A high-performance, memory-efficient implementation of the Adaptive Radix Tree (ART) data structure with Go generics and concurrent access support.

## Overview

This library provides a space-optimized, cache-friendly implementation of the Adaptive Radix Tree with the following features:

- **Adaptive Node Sizing**: Uses four different kvType types (4, 16, 48, and 256 children) to minimize memory footprint
- **Type Safety**: Full Go generics support for any value type
- **Concurrent Access**: Context-aware locking ensures thread safety during operations
- **Path Compression**: Efficiently stores common key prefixes to reduce memory usage
- **Compatible API**: Implements interfaces compatible with the popular [hashicorp/go-immutable-radix](https://github.com/hashicorp/go-immutable-radix) library

## What is an Adaptive Radix Tree?

An Adaptive Radix Tree (ART) is an optimized data structure that combines the advantages of radix trees and tries:

- **Space Efficiency**: Adaptively uses different kvType sizes based on actual child count
- **Cache Locality**: Compact kvType structure improves CPU cache utilization
- **Fast Operations**: O(k) complexity for lookups, insertions, and deletions (where k is key length)
- **Path Compression**: Stores common prefixes only once, reducing memory usage

## Node Types

The implementation uses four different kvType types, each optimized for a specific number of children:

1. **Node4**: Stores up to 4 children using simple arrays
2. **Node16**: Stores up to 16 children with slightly more complex structure
3. **Node48**: Stores up to 48 children with an optimized layout
4. **Node256**: Stores up to 256 children (one for each possible byte value)

Nodes automatically grow or shrink as needed, maintaining optimal memory usage at all times.

## Usage

```go
import (
    "context"
    art "github.com/datnguyenzzz/nogodb/lib/go-adaptive-radix-tree"
)

func main() {
    // Create a new tree with string values
    ctx := context.Background()
    tree := art.NewTree[string](ctx)
    
    // Insert key-value pairs
    key1 := []byte("hello")
    tree.Insert(ctx, key1, "world")
    
    // Retrieve values
    value, err := tree.Get(ctx, key1)
    if err == nil {
        fmt.Println(value) // Outputs: world
    }
    
    // Delete entries
    oldValue, err := tree.Delete(ctx, key1)
    
    // Walk the tree in order
    tree.Walk(ctx, func(ctx context.Context, key []byte, value string) error {
        fmt.Printf("Key: %s, Value: %s\n", key, value)
        return nil
    })
}
```

## Future Enhancements

- [ ] P0 **Optimistic lock coupling**:
  - Currently, to make ART operations thread-safe, we use a coarse-grained lock, so all operations are executed sequentially.
  While this is the most straightforward approach, the trade-off is reduced throughput and increased latency under high concurrency.
  - The next synchronization approach we chose is optimistic lock coupling. Instead of preventing concurrent modification, we optimistically assume that there will be no concurrent modification and later use
  version counters to check if we need to restart the operation
- [ ] P1 **Swizzlable Pointers**: Enable persistence to disk by converting memory pointers to disk offsets
  - **Serialization**: Convert in-memory pointers to stable identifiers for storage
  - **Deserialization**: Restore pointers when loading from disk
