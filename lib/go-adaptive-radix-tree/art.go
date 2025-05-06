package go_adaptive_radix_tree

import (
	"context"

	"github.com/datnguyenzzz/nogodb/lib/go-adaptive-radix-tree/internal"
	gocontextawarelock "github.com/datnguyenzzz/nogodb/lib/go-context-aware-lock"
)

// TODO Implement Swizzlable Pointers onto Tree[V any]
// Currently, the struct Tree[V Any] only supports in-memory storage because it relies on pointers to nodes.
// Each node, in turn, contains multiple pointers to its children, forming the tree structure.
// Due to this design, we cannot persist this structure to disk.
// While we could add additional fields to the struct, e.g
// type Tree[V any] struct {
//	root internal.INode[V] // memory address - pointer to the root node
//  block_id uint32 // for unswizzling
//  offset uint32 // for unswizzling
//	lock gocontextawarelock.ICtxLock
//}
// this approach might introduce unnecessary resource overhead (it requires extra 8 bytes per Node)
// Therefore, we need to
//    1. Enhance the current object Tree[V] to Swizzlable Pointers
//    2. Implements:
//      Serialization:
//         When saving the data structure to disk, the pointers are "unswizzled," often by converting them into
//         some form of identifier, such as an index or a unique ID, that is independent of memory addresses.
//      Deserialization:
//         When loading the data structure back into memory, the identifiers are used to locate the corresponding
//         data objects, and the pointers are "swizzled" by replacing the identifiers with the actual memory
//         addresses of the loaded objects.
//
// One major problem with implementing this (de)serialization process is that now we not only have to keep information
// about the memory address of pointers but also if they are already in memory and if not,
// what's the <Block,Offset> position they are stored.

// Tree is an implementation of a radix tree with adaptive nodes.
// It is also compatible with the interfaces of the popular radix tree library.
// https://github.com/hashicorp/go-immutable-radix
type Tree[V any] struct {
	root internal.INode[V] // pointer to the root node
	lock gocontextawarelock.ICtxLock
}

func NewTree[V any](ctx context.Context) *Tree[V] {
	return &Tree[V]{lock: gocontextawarelock.NewLocalLock()}
}

func (t *Tree[V]) Insert(ctx context.Context, key Key, value V) (V, error) {
	if err := t.lock.AcquireCtx(ctx); err != nil {
		return *new(V), err
	}
	defer t.lock.ReleaseCtx(ctx)

	ptr := &t.root
	return internal.InsertNode[V](ctx, ptr, key, value, 0)
}

func (t *Tree[V]) Delete(ctx context.Context, key Key) (V, error) {
	if err := t.lock.AcquireCtx(ctx); err != nil {
		return *new(V), err
	}
	defer t.lock.ReleaseCtx(ctx)

	ptr := &t.root
	v, isNodeRemovable, err := internal.RemoveNode[V](ctx, ptr, key, 0)
	if isNodeRemovable && err == nil {
		// it means the root node can be removed
		// since the all nodes in the tree have been deleted
		t.root = nil
	}
	return v, err
}

func (t *Tree[V]) Get(ctx context.Context, key Key) (V, error) {
	return internal.Get[V](ctx, t.root, key, 0)
}

func (t *Tree[V]) Minimum(ctx context.Context) (Key, V, bool) {
	//TODO implement me
	panic("implement me")
}

func (t *Tree[V]) Maximum(ctx context.Context) (Key, V, bool) {
	//TODO implement me
	panic("implement me")
}

func (t *Tree[V]) Walk(ctx context.Context, fn WalkFn[V]) {
	cb := func(ctx context.Context, k []byte, v V) {
		// Ignore error for now
		_ = fn(ctx, k, v)
	}
	internal.Walk[V](ctx, t.root, cb, internal.AscOrder)
}

func (t *Tree[V]) WalkBackwards(ctx context.Context, fn WalkFn[V]) {
	cb := func(ctx context.Context, k []byte, v V) {
		// Ignore error for now
		_ = fn(ctx, k, v)
	}
	internal.Walk[V](ctx, t.root, cb, internal.DescOrder)
}

var _ ITree[any] = (*Tree[any])(nil)
