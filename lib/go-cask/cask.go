package go_cask

import "context"

type DB[V any] struct {
	opts options
}

// NewDB init new instance of go-cask with given configuration, but WILL NOT open the file
// for neither reading nor writing yet
func NewDB[V any](opts ...EngineOpts[V]) *DB[V] {
	// init a DB instance with the default options
	db := &DB[V]{
		opts: options{
			generalOptions:    defaultGeneralOptions,
			expiryOptions:     defaultExpiryOptions,
			compactionOptions: defaultCompactionOptions,
			syncOptions:       defaultSyncOptions,
		},
	}
	for _, o := range opts {
		o(db)
	}
	return db
}

func (D DB[V]) Open(ctx context.Context) error {
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

var _ IDB[any] = (*DB[any])(nil)
