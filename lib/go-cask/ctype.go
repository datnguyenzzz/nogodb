package go_cask

import "context"

type Key []byte

type FoldFn[V any] = func(context.Context, Key, V) error

type IDB[V any] interface {
	// Open a new or existing go-cask datastore
	Open(ctx context.Context) error
	// Close a go-cask data store and flush any pending writes to disk.
	Close(context.Context) error
	// Get a value by key from a go-cask datastore.
	Get(ctx context.Context, k Key) (V, error)
	// Put a key and value in a go-cask datastore.
	Put(ctx context.Context, k Key, value V) error
	// Delete a key from a go-cask datastore.
	Delete(ctx context.Context, k Key) error
	// Sync any writes to sync to disk.
	Sync(ctx context.Context) error
	// ListKeys all keys in a go-cask datastore.
	ListKeys(ctx context.Context) ([]Key, error)
	// Fold over all keys in a go-cask datastore with limits on how out of date is allowed to be.
	Fold(ctx context.Context, fn FoldFn[V], maxAge int) error
	// Merge several data files within a go-cask datastore into a more compact form.
	Merge(ctx context.Context) error
}
