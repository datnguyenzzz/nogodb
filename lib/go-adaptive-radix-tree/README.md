# The Adaptive Radix Tree Implementation In Golang

This library provides an implementation of a radix tree with adaptive nodes.
It is also compatible with the interfaces of the popular immutable radix tree library:
https://github.com/hashicorp/go-immutable-radix

## Supported functions 
```go
// WalkFn is used when walking the tree. Takes a key and value, returning if iteration should be terminated.
type WalkFn[V any] func(ctx context.Context, k Key, v V) error

type ITree[V any] interface {
// Insert is used to add or update a given key. The return provides the previous value and a bool indicating if any was set.
Insert(ctx context.Context, key Key, value V) (V, error)
// Delete is used to delete a given key. Returns the old value if any, and a bool indicating if the key was set.
Delete(ctx context.Context, key Key) (V, error)
// Get is used to lookup a specific key, returning the value and if it was found
Get(ctx context.Context, key Key) (V, error)
// Minimum is used to return the minimum value in the tree
Minimum(ctx context.Context) (Key, V, bool)
// Maximum is used to return the maximum value in the tree
Maximum(ctx context.Context) (Key, V, bool)
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