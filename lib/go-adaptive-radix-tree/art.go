package go_adaptive_radix_tree

import (
	"context"

	"github.com/datnguyenzzz/nogodb/lib/go-adaptive-radix-tree/internal"
)

// Tree is an implementation of a radix tree with adaptive nodes.
// It is also compatible with the interfaces of the popular radix tree library.
// https://github.com/hashicorp/go-immutable-radix
type Tree[V any] struct {
	root internal.INode[V] // pointer to the root node
	lock *internal.CtxLock
}

func NewTree[V any](ctx context.Context) *Tree[V] {
	return &Tree[V]{lock: internal.NewLock()}
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
	//TODO implement me
	panic("implement me")
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
