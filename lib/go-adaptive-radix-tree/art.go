package go_adaptive_radix_tree

import (
	"context"
	"fmt"

	"github.com/datnguyenzzz/nogodb/lib/go-adaptive-radix-tree/internal"
	gocontextawarelock "github.com/datnguyenzzz/nogodb/lib/go-context-aware-lock"
)

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
	return errorCategorisation(internal.InsertNode[V](ctx, ptr, key, value, 0))
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
	return errorCategorisation(v, err)
}

func (t *Tree[V]) Get(ctx context.Context, key Key) (V, error) {
	if err := t.lock.AcquireCtx(ctx); err != nil {
		return *new(V), err
	}
	defer t.lock.ReleaseCtx(ctx)
	return errorCategorisation(internal.Get[V](ctx, t.root, key, 0))
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

func errorCategorisation[V any](v V, err error) (V, error) {
	var categorisedErr error
	switch err {
	case nil:
		categorisedErr = nil
	case internal.NoSuchKey:
		categorisedErr = fmt.Errorf("%w: %v", NonExist, err)
	default:
		categorisedErr = fmt.Errorf("%w: %v", Unrecognised, err)
	}

	return v, categorisedErr
}

var _ ITree[any] = (*Tree[any])(nil)
