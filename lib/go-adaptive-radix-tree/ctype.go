// Compatible with the famous radix tree library - https://github.com/hashicorp/go-immutable-radix

package go_adaptive_radix_tree

import "context"

type Key []byte

// WalkFn is used when walking the tree. Takes a key and value, returning if iteration should be terminated.
type WalkFn[T any] func(k Key, v T) bool

// Node

type Node[T any] struct {
	// ...
}

// INode shares the same interfaces with https://github.com/hashicorp/go-immutable-radix
type INode[T any] interface {
	// Get return an exact match
	Get(ctx context.Context, k Key) (T, bool)
	// LongestPrefix is like Get, but instead of an exact match, it will return the longest prefix match.
	LongestPrefix(ctx context.Context, k Key) (Key, T, error)
	// Minimum is used to return the minimum value in the tree
	Minimum(ctx context.Context) (Key, T, bool)
	// Maximum is used to return the maximum value in the tree
	Maximum(ctx context.Context) (Key, T, bool)
	// Walk is used to walk the tree
	Walk(ctx context.Context, fn WalkFn[T])
	// WalkBackwards is used to walk the tree in reverse order
	WalkBackwards(ctx context.Context, fn WalkFn[T])
	// WalkPrefix is used to walk the tree under a prefix
	WalkPrefix(ctx context.Context, prefix Key, fn WalkFn[T])
}

var _ INode[any] = (*Node[any])(nil)

// Tree

type Tree[T any] struct {
	// ...
}

// ITree shares the same interfaces with https://github.com/hashicorp/go-immutable-radix
type ITree[T any] interface {
	// Insert is used to add or update a given key. The return provides the previous value and a bool indicating if any was set.
	Insert(ctx context.Context, key Key, value T) (T, error)
	// Delete is used to delete a given key. Returns the old value if any, and a bool indicating if the key was set.
	Delete(ctx context.Context, key Key) (T, error)
	// Get is used to lookup a specific key, returning the value and if it was found
	Get(ctx context.Context, key Key) (T, error)
	// Root returns the root node of the tree which can be used for richer query operations.
	Root(ctx context.Context) (*Node[T], error)
}

var _ ITree[any] = (*Tree[any])(nil)
