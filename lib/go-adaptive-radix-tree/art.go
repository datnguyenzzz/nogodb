package go_adaptive_radix_tree

import "context"

type Tree[T any] struct {
	// ...
}

func (t Tree[T]) Insert(ctx context.Context, key Key, value T) (T, error) {
	//TODO implement me
	panic("implement me")
}

func (t Tree[T]) Delete(ctx context.Context, key Key) (T, error) {
	//TODO implement me
	panic("implement me")
}

func (t Tree[T]) Get(ctx context.Context, key Key) (T, error) {
	//TODO implement me
	panic("implement me")
}

func (t Tree[T]) Root(ctx context.Context) (*Node[T], error) {
	//TODO implement me
	panic("implement me")
}

var _ ITree[any] = (*Tree[any])(nil)
