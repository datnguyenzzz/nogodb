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

## Data Structure Comparison

The following table compares key characteristics of different data structures commonly used for key-value storage:

| Feature | Adaptive Radix Tree (ART) | Skip List | Hash Table |
|---------|---------------------------|-----------|------------|
| **Time Complexity** | | | |
| - Search | O(k) | O(log n) | O(1) average, O(n) worst |
| - Insert | O(k) | O(log n) | O(1) average, O(n) worst |
| - Delete | O(k) | O(log n) | O(1) average, O(n) worst |
| - Range Query | O(k + m) | O(log n + m) | Not supported |
| - Ordered Traversal | ✅ Natural | ✅ Natural | ❌ Requires sorting |
| **Memory Characteristics** | | | |
| - Memory Efficiency | ⭐⭐⭐⭐⭐ Excellent | ⭐⭐⭐ Good | ⭐⭐⭐ Good |
| - Cache Locality | ⭐⭐⭐⭐⭐ Excellent | ⭐⭐ Fair | ⭐⭐⭐ Good |
| - Memory Overhead | ~20-40 bytes/key | ~24-32 bytes/key | ~16-24 bytes/key |
| - Path Compression | ✅ Built-in | ❌ None | ❌ None |
| **Scalability** | | | |
| - Large Datasets | ⭐⭐⭐⭐⭐ Excellent | ⭐⭐⭐⭐ Good | ⭐⭐⭐⭐ Good |
| - String Keys | ⭐⭐⭐⭐⭐ Optimal | ⭐⭐⭐ Good | ⭐⭐⭐⭐ Good |
| - Prefix Sharing | ⭐⭐⭐⭐⭐ Excellent | ❌ None | ❌ None |
| **Concurrency** | | | |
| - Read Concurrency | ⭐⭐⭐ Fair | ⭐⭐⭐⭐ Good | ⭐⭐⭐⭐⭐ Excellent |
| - Write Concurrency | ⭐⭐ Limited | ⭐⭐⭐ Fair | ⭐⭐⭐⭐⭐ Excellent |
| - Lock Granularity | Coarse-grained | Fine-grained | Fine-grained |
| **Use Case Suitability** | | | |
| - In-Memory DB Index | ⭐⭐⭐⭐⭐ Optimal | ⭐⭐⭐⭐ Good | ⭐⭐⭐ Fair |
| - String Dictionaries | ⭐⭐⭐⭐⭐ Optimal | ⭐⭐⭐ Good | ⭐⭐⭐⭐ Good |
| - IP Routing Tables | ⭐⭐⭐⭐⭐ Optimal | ⭐⭐ Poor | ⭐⭐ Poor |
| - Auto-completion | ⭐⭐⭐⭐⭐ Optimal | ⭐⭐⭐ Good | ❌ Not suitable |
| - Cache Implementation | ⭐⭐⭐ Fair | ⭐⭐ Poor | ⭐⭐⭐⭐⭐ Optimal |
| - High-Freq Lookups | ⭐⭐⭐⭐ Good | ⭐⭐⭐ Fair | ⭐⭐⭐⭐⭐ Optimal |

### Key Insights

**Adaptive Radix Tree (ART)**

*Advantages:*
- **Memory Efficiency**: Superior for string keys with common prefixes due to path compression
- **Cache Performance**: Excellent spatial locality from compact, adaptive node structures
- **Ordered Access**: Natural lexicographic ordering enables efficient range queries
- **Deterministic Performance**: O(k) complexity independent of dataset size

*Disadvantages:*
- **Implementation Complexity**: More complex to implement and debug than alternatives
- **Limited Concurrency**: Coarse-grained locking reduces parallel write performance
- **Pointer Overhead**: Multiple indirections can hurt performance on small datasets
- **Worst-case Memory**: Can use more memory than hash tables for random, short keys

**Skip List**

*Advantages:*  
- **Simplicity**: Easier to implement and debug compared to ART
- **Probabilistic Balance**: Self-balancing without complex rotation logic
- **Fine-grained Locking**: Better concurrency than ART's coarse-grained approach
- **Consistent Performance**: Predictable O(log n) behavior across all operations

*Disadvantages:*
- **Memory Overhead**: Higher memory usage due to multiple forward pointers per node
- **Cache Misses**: Poor spatial locality due to scattered node allocation
- **No Path Compression**: Cannot optimize storage for keys with common prefixes
- **Probabilistic Nature**: Performance can vary based on random level generation

**Hash Table**

*Advantages:*
- **Lookup Speed**: Unmatched O(1) average case performance for point queries  
- **Concurrency**: Excellent parallelism with techniques like lock-free hashing
- **Implementation Maturity**: Well-understood with many optimized implementations
- **Memory Predictability**: Fixed overhead per entry with good load factor management

*Disadvantages:*
- **No Ordering**: Cannot support range queries or ordered traversal natively
- **Hash Collisions**: Performance degrades to O(n) in worst case with poor hash functions
- **Resize Costs**: Expensive rehashing operations when load factor thresholds exceeded
- **Memory Waste**: Fixed bucket allocation can waste memory with sparse key distributions

### Choosing the Right Structure

| Scenario | Recommended Structure | Rationale |
|----------|----------------------|-----------|
| String-heavy workloads with prefixes | **ART** | Path compression dramatically reduces memory usage |
| High-frequency point lookups | **Hash Table** | O(1) average performance beats all alternatives |
| Need for range/prefix queries | **ART or Skip List** | Hash tables don't support ordered operations |
| Write-heavy concurrent workloads | **Hash Table** | Superior concurrent write performance |
| Memory-constrained environments | **ART** | Best memory efficiency for structured keys |
| Simple implementation requirements | **Skip List** | Easier to implement and maintain |

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