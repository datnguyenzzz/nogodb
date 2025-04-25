// Package go_adaptive_radix_tree is compatible with the famous radix tree library - https://github.com/hashicorp/go-immutable-radix
package go_adaptive_radix_tree

import "context"

type Key []byte

// WalkFn is used when walking the tree. Takes a key and value, returning if iteration should be terminated.
type WalkFn[V any] func(k Key, v V) bool

type INode[V any] interface {
	// Get is used to lookup a specific key, returning the value and if it was found
	Get(ctx context.Context, key Key) (V, error)
	// LongestPrefix is like Get, but instead of an exact match, it will return the longest prefix match.
	LongestPrefix(ctx context.Context, k Key) (Key, V, error)
	// Minimum is used to return the minimum value in the tree
	Minimum(ctx context.Context) (Key, V, bool)
	// Maximum is used to return the maximum value in the tree
	Maximum(ctx context.Context) (Key, V, bool)
	// Walk is used to walk the tree
	Walk(ctx context.Context, fn WalkFn[V])
	// WalkBackwards is used to walk the tree in reverse order
	WalkBackwards(ctx context.Context, fn WalkFn[V])
	// WalkPrefix is used to walk the tree under a prefix
	WalkPrefix(ctx context.Context, prefix Key, fn WalkFn[V])
}

type ITree[V any] interface {
	// Insert is used to add or update a given key. The return provides the previous value and a bool indicating if any was set.
	Insert(ctx context.Context, key Key, value V) (V, error)
	// Delete is used to delete a given key. Returns the old value if any, and a bool indicating if the key was set.
	Delete(ctx context.Context, key Key) (V, error)
	// Root returns the root node of the tree which can be used for richer query operations.
	Root(ctx context.Context) (*Node[V], error)
}
