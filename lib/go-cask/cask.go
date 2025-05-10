package go_cask

import "context"

type DB[V any] struct {
	opts options
}

func NewDB[V any](opts ...EngineOpts[V]) *DB[V] {
	db := &DB[V]{}
	for _, o := range opts {
		o(db)
	}
	return db
}

func (D DB[V]) Open(ctx context.Context) (IEngine[V], error) {
	//TODO implement me
	panic("implement me")
}

func (D DB[V]) Close(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (D DB[V]) Get(ctx context.Context, k Key) (V, error) {
	//TODO implement me
	panic("implement me")
}

func (D DB[V]) Put(ctx context.Context, k Key, value V) error {
	//TODO implement me
	panic("implement me")
}

func (D DB[V]) Delete(ctx context.Context, k Key) error {
	//TODO implement me
	panic("implement me")
}

func (D DB[V]) Sync(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (D DB[V]) ListKeys(ctx context.Context) ([]Key, error) {
	//TODO implement me
	panic("implement me")
}

func (D DB[V]) Fold(ctx context.Context, fn FoldFn[V], maxAge int) error {
	//TODO implement me
	panic("implement me")
}

func (D DB[V]) Merge(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

var _ IEngine[any] = (*DB[any])(nil)
