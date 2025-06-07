# The Adaptive Radix Tree Implementation In Golang

This library provides an implementation of a radix tree with adaptive nodes and type-safe generics.
It is also compatible with the interfaces of the popular immutable radix tree library:
https://github.com/hashicorp/go-immutable-radix

Additionally, the tree implementation utilizes context-aware locking to ensure that exactly 1 thread can modify 
the tree during write operations, such as `Insert()` or `Delete()`.

## Supported functions 
```go
// WalkFn is used when walking the tree. Takes a key and value, returning if iteration should be terminated.
type WalkFn[V any] func(ctx context.Context, k InternalKey, v V) error

type ITree[V any] interface {
    // Insert is used to add or update a given key. The return provides the previous value and a bool indicating if any was set.
    Insert(ctx context.Context, key InternalKey, value V) (V, error)
    // Delete is used to delete a given key. Returns the old value if any, and a bool indicating if the key was set.
    Delete(ctx context.Context, key InternalKey) (V, error)
    // Get is used to lookup a specific key, returning the value and if it was found
    Get(ctx context.Context, key InternalKey) (V, error)
    // Minimum is used to return the minimum value in the tree
    Minimum(ctx context.Context) (InternalKey, V, bool)
    // Maximum is used to return the maximum value in the tree
    Maximum(ctx context.Context) (InternalKey, V, bool)
    // Walk is used to walk the tree
    Walk(ctx context.Context, fn WalkFn[V])
    // WalkBackwards is used to walk the tree in reverse order
    WalkBackwards(ctx context.Context, fn WalkFn[V])
    // TO BE ADDED
}
```

## Performance result 

Benchmark the library's performance for the Insert() and Get() functions using datasets containing 
100,000, 250,000, 500,000, and 1,000,000 randomly generated sentences of various length between [30, 50], 
created with the [go-faker-v4](https://pkg.go.dev/github.com/go-faker/faker/v4) library.

```text
goos: darwin
goarch: arm64
pkg: github.com/datnguyenzzz/nogodb/lib/go-adaptive-radix-tree
cpu: Apple M1 Pro
Testcase                               #          Average Time          Bytes per operation   Allocs per operation
BenchmarkInsert_100000-10     	      43	  28591934 ns/op	14767752 B/op	      340001 allocs/op
BenchmarkInsert_250000-10     	      18	  71634674 ns/op	37092440 B/op	      850001 allocs/op
BenchmarkInsert_500000-10     	       8	 142106344 ns/op	74294568 B/op	     1700001 allocs/op
BenchmarkInsert_1000000-10    	       4	 278329469 ns/op	148703752 B/op	     3400001 allocs/op
BenchmarkGet_100000-10        	     135	   8818765 ns/op	       0 B/op	           0 allocs/op
BenchmarkGet_250000-10        	      50	  24875808 ns/op	       0 B/op	           0 allocs/op
BenchmarkGet_500000-10        	      24	  49266505 ns/op	       0 B/op	           0 allocs/op
BenchmarkGet_1000000-10       	      10	 100743017 ns/op	       0 B/op	           0 allocs/op
```

## Next releases / TODO 

- [ ] Implement Swizzlable Pointers onto `Tree[V any]`
  - Currently, the struct Tree[V Any] only supports in-memory storage because it relies on pointers to nodes.
  Each node, in turn, contains multiple pointers to its children, forming the tree structure.
  Due to this design, we cannot persist this structure to disk.
  - While we could add additional fields to the struct, e.g
      ```go
      type Tree[V any] struct {
        root internal.INode[V] // memory address - pointer to the root node
        block_id uint32 // for unswizzling
        offset uint32 // for unswizzling
        lock gocontextawarelock.ICtxLock
      }
      ```
    this approach might introduce unnecessary resource overhead (it requires extra 8 bytes per Node). Therefore, we need to enhance the current object `Tree[V]` to Swizzlable Pointers 
  - Implements plan:
    - Serialization:
       When saving the data structure to disk, the pointers are "unswizzled," often by converting them into
       some form of identifier, such as an index or a unique ID, that is independent of memory addresses.
    - Deserialization:
       When loading the data structure back into memory, the identifiers are used to locate the corresponding
       data objects, and the pointers are "swizzled" by replacing the identifiers with the actual memory
       addresses of the loaded objects.
