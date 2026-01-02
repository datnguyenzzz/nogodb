package go_wal

import (
	"time"

	go_fs "github.com/datnguyenzzz/nogodb/lib/go-fs"
)

type OptionFn func(*WAL)

type options struct {
	// pageSize specifies the maximum size of each page file in bytes.
	pageSize int64

	// sync is whether to synchronize writes through os buffer cache and down onto the actual disk.
	// Setting sync is required for durability of a single write operation, but also results in slower writes.
	// If false, and the machine crashes, then some recent writes may be lost.
	// Note that if it is just the process that crashes (machine does not) then no writes will be lost.
	sync bool

	// bytesPerSync specifies the number of bytes to write before calling sync.
	bytesPerSync uint32

	// syncInterval is the time duration in which explicit synchronization is performed.
	// If syncInterval is zero, no periodic synchronization is performed.
	syncInterval time.Duration

	location go_fs.Location
}

var defaultOptions = options{
	pageSize:     1 * 1024 * 1024 * 2024, // 1GB
	sync:         false,
	bytesPerSync: 0,
	syncInterval: 0,
	location:     go_fs.InMemory,
}

func WithLocation(location go_fs.Location) OptionFn {
	return func(w *WAL) {
		switch location {
		case go_fs.InMemory:
			w.storage = go_fs.NewInmemStorage()
		default:
			panic("not supported location")
		}
	}
}

func WithPageSize(pageSize int64) OptionFn {
	return func(wal *WAL) {
		wal.opts.pageSize = pageSize
	}
}

func WithSync(sync bool) OptionFn {
	return func(wal *WAL) {
		wal.opts.sync = sync
	}
}

func WithBytesPerSync(bytesPerSync uint32) OptionFn {
	return func(wal *WAL) {
		wal.opts.bytesPerSync = bytesPerSync
	}
}

func WithSyncInterval(interval time.Duration) OptionFn {
	return func(wal *WAL) {
		wal.opts.syncInterval = interval
	}
}
