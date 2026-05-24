package go_wal

import (
	"time"

	nogodb_common "github.com/datnguyenzzz/nogodb/lib/common"
	nogodb_fs "github.com/datnguyenzzz/nogodb/lib/go-fs"
)

type OptionFn func(*WAL)

type options struct {
	fs nogodb_fs.FS

	// bytesPerSync specifies the number of bytes to write before calling sync.
	bytesPerSync uint32

	// syncInterval is the time duration in which explicit synchronization is performed.
	// If syncInterval is zero, no periodic synchronization is performed.
	syncInterval time.Duration

	logger nogodb_common.Logger
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

func WithFS(fs nogodb_fs.FS) OptionFn {
	return func(w *WAL) {
		w.opts.fs = fs
	}
}

func WithLogger(l nogodb_common.Logger) OptionFn {
	return func(w *WAL) {
		w.opts.logger = l
	}
}
