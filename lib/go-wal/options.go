package go_wal

import (
	"fmt"
	"os"
	"strings"
	"time"
)

type OptionFn func(*WAL)

type options struct {
	// dirPath specifies the directory path where the WAL page files will be stored.
	dirPath string

	// pageSize specifies the maximum size of each page file in bytes.
	pageSize int64

	// fileExt specifies the file extension of the page files.
	fileExt string

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
}

var defaultOptions = options{
	dirPath:      os.TempDir(),
	pageSize:     512 * 1024 * 2024, // 512MB
	fileExt:      ".wal",
	sync:         false,
	bytesPerSync: 0,
	syncInterval: 0,
}

func WithDirPath(dirPath string) OptionFn {
	return func(wal *WAL) {
		wal.opts.dirPath = dirPath
	}
}

func WithFileExt(fileExt string) OptionFn {
	return func(wal *WAL) {
		// need a "." prefix
		if !strings.HasPrefix(fileExt, ".") {
			fileExt = fmt.Sprintf(".%s", fileExt)
		}
		wal.opts.fileExt = fileExt
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
