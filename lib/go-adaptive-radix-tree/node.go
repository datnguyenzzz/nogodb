package go_adaptive_radix_tree

import "context"

type Node[T any] struct {
	// ...
}

func (n Node[T]) LongestPrefix(ctx context.Context, k Key) (Key, T, error) {
	//TODO implement me
	panic("implement me")
}

func (n Node[T]) Minimum(ctx context.Context) (Key, T, bool) {
	//TODO implement me
	panic("implement me")
}

func (n Node[T]) Maximum(ctx context.Context) (Key, T, bool) {
	//TODO implement me
	panic("implement me")
}

func (n Node[T]) Walk(ctx context.Context, fn WalkFn[T]) {
	//TODO implement me
	panic("implement me")
}

func (n Node[T]) WalkBackwards(ctx context.Context, fn WalkFn[T]) {
	//TODO implement me
	panic("implement me")
}

func (n Node[T]) WalkPrefix(ctx context.Context, prefix Key, fn WalkFn[T]) {
	//TODO implement me
	panic("implement me")
}

var _ INode[any] = (*Node[any])(nil)
