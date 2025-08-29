# Adaptive Radix Tree Implementation in Go

A high-performance, memory-efficient implementation of the Adaptive Radix Tree (ART) data structure with Go generics and concurrent access support.

## Overview

This library provides a space-optimized, cache-friendly implementation of the Adaptive Radix Tree with the following features:

- **Adaptive Node Sizing**: Uses four different kv types (4, 16, 48, and 256 children) to minimize memory footprint
- **Type Safety**: Full Go generics support for any value type
- **Concurrent Access**: Context-aware locking ensures thread safety during operations
- **Path Compression**: Efficiently stores common key prefixes to reduce memory usage
- **Compatible API**: Implements interfaces compatible with the popular [hashicorp/go-immutable-radix](https://github.com/hashicorp/go-immutable-radix) library

## What is an Adaptive Radix Tree?

An Adaptive Radix Tree (ART) is an optimized data structure that combines the advantages of radix trees and tries:

- **Space Efficiency**: Adaptively uses different kv sizes based on actual child count
- **Cache Locality**: Compact kv structure improves CPU cache utilization
- **Fast Operations**: O(k) complexity for lookups, insertions, and deletions (where k is key length)
- **Path Compression**: Stores common prefixes only once, reducing memory usage

## Node Types

The implementation uses four different kv types, each optimized for a specific number of children:

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

## API Reference

The tree implements the following interface:

```go
type ITree[V any] interface {
    // Insert adds or updates a key-value pair, returning previous value if any
    Insert(ctx context.Context, key InternalKey, value V) (V, error)
    
    // Delete removes a key and returns the previous value if found
    Delete(ctx context.Context, key InternalKey) (V, error)
    
    // Get retrieves a value by key
    Get(ctx context.Context, key InternalKey) (V, error)
    
    // Minimum returns the smallest key-value pair in the tree
    Minimum(ctx context.Context) (InternalKey, V, bool)
    
    // Maximum returns the largest key-value pair in the tree
    Maximum(ctx context.Context) (InternalKey, V, bool)
    
    // Walk traverses the tree in order
    Walk(ctx context.Context, fn WalkFn[V])
    
    // WalkBackwards traverses the tree in reverse order
    WalkBackwards(ctx context.Context, fn WalkFn[V])
}
```

## Performance

Benchmark results on Apple M1 Pro:

| Operation | Dataset Size | Ops/sec | Avg Time (ns/op) | Memory Usage (B/op) |
|-----------|--------------|---------|-----------------|-------------------|
| Insert    | 100,000      | 43      | 28,591,934      | 14,767,752        |
| Insert    | 250,000      | 18      | 71,634,674      | 37,092,440        |
| Insert    | 500,000      | 8       | 142,106,344     | 74,294,568        |
| Insert    | 1,000,000    | 4       | 278,329,469     | 148,703,752       |
| Get       | 100,000      | 135     | 8,818,765       | 0                 |
| Get       | 250,000      | 50      | 24,875,808      | 0                 |
| Get       | 500,000      | 24      | 49,266,505      | 0                 |
| Get       | 1,000,000    | 10      | 100,743,017     | 0                 |

## Use Cases

Adaptive Radix Trees excel in the following scenarios:

- **In-Memory Databases**: Efficient storage and retrieval of large datasets
- **Key-Value Stores**: Fast lookups with minimal memory overhead
- **String Dictionaries**: Compact storage of string keys with common prefixes
- **IP Routing Tables**: Efficient storage and lookup of network routes
- **Auto-Completion Systems**: Quick prefix-based searches
- **Time-Series Data**: Ordered access to time-based records

## Future Enhancements

- [ ] **Swizzlable Pointers**: Enable persistence to disk by converting memory pointers to disk offsets
  - **Serialization**: Convert in-memory pointers to stable identifiers for storage
  - **Deserialization**: Restore pointers when loading from disk

## References

- ["The Adaptive Radix Tree: ARTful Indexing for Main-Memory Databases"](https://db.in.tum.de/~leis/papers/ART.pdf) by Viktor Leis, et al.
- [Hashicorp's Immutable Radix Tree](https://github.com/hashicorp/go-immutable-radix)

## License

See the project's LICENSE file for license information.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.