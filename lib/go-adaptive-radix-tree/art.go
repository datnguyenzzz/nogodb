package go_adaptive_radix_tree

import (
	"context"
	"fmt"

	"github.com/datnguyenzzz/nogodb/lib/go-adaptive-radix-tree/internal"
)

// Tree is an implementation of a radix tree with adaptive nodes.
// It is also compatible with the interfaces of the popular radix tree library.
// https://github.com/hashicorp/go-immutable-radix
type Tree[V any] struct {
	root  internal.INode[V] // pointer to the root node
	vRoot internal.INode[V]
}

func NewTree[V any](ctx context.Context) *Tree[V] {
	return &Tree[V]{
		vRoot: internal.NewNode[V](internal.KindNode4),
	}
}

func (t *Tree[V]) Insert(ctx context.Context, key Key, value V) (V, error) {
	t.vRoot.GetLocker().Lock()
	return errorCategorisation(internal.InsertNode(ctx, &t.root, &t.vRoot, key, value, 0))
}

func (t *Tree[V]) Delete(ctx context.Context, key Key) (V, error) {
	if t.root == nil {
		return *new(V), NonExist
	}
	t.vRoot.GetLocker().Lock()
	v, isRootRemoved, err := internal.RemoveNode(ctx, &t.root, &t.vRoot, key, 0)
	if isRootRemoved {
		t.root = nil
	}
	return errorCategorisation(v, err)
}

func (t *Tree[V]) Get(ctx context.Context, key Key) (V, error) {
	return errorCategorisation(internal.Get(ctx, t.root, key, 0))
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

func (t *Tree[V]) Visualize(ctx context.Context) {
	internal.Visualize[V](ctx, t.root)
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
