package go_adaptive_radix_tree

import (
	"context"

	"github.com/datnguyenzzz/nogodb/lib/go-adaptive-radix-tree/internal"
)

type Tree[V any] struct {
	root internal.INode[V]
}

func (t *Tree[V]) Insert(ctx context.Context, key Key, value V) (V, error) {
	//TODO implement me
	panic("implement me")
}

func (t *Tree[V]) Delete(ctx context.Context, key Key) (V, error) {
	//TODO implement me
	panic("implement me")
}

func (t *Tree[V]) Get(ctx context.Context, key Key) (V, error) {
	//TODO implement me
	panic("implement me")
}

func (t *Tree[V]) LongestPrefix(ctx context.Context, k Key) (Key, V, error) {
	//TODO implement me
	panic("implement me")
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
	//TODO implement me
	panic("implement me")
}

func (t *Tree[V]) WalkBackwards(ctx context.Context, fn WalkFn[V]) {
	//TODO implement me
	panic("implement me")
}

func (t *Tree[V]) WalkPrefix(ctx context.Context, prefix Key, fn WalkFn[V]) {
	//TODO implement me
	panic("implement me")
}

var _ ITree[any] = (*Tree[any])(nil)
