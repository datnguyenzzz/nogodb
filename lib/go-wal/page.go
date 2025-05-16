package go_wal

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
)

const (
	firstPageId      PageID = 0
	defaultBlockSize        = 32 * 1024 // 32KB
)

// writeBufferPool, readBufferPool maintains a pool of 32KB buffers, each serving as a dedicated buffer for individual blocks.
// This design helps reduce garbage collection (GC) pressure and minimizes memory allocations by reusing buffers,
// eliminating the need to create new buffers for every read and write operation, so the GC doesn't have to be kicked in
// to clean up the buffers after used. Since records are guaranteed to never exceed a data size of 32KB,
// the maximum buffer size is predictable.
var readBufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, defaultBlockSize)
	},
}

// An enhancement to address the issue of inefficient memory usage, where code that
// requires a small amount of memory may receive a large buffer from the pool, and vice versa.
// For example, in the case of writeBufferPool, the size of data writes can vary, making the
// allocation of a fixed 32KB buffer wasteful.
//
// A Non-optimised implementation for the writeBufferPool
//var writeBufferPool = sync.Pool{
//	New: func() interface{} {
//		return make([]byte, blockSize)
//	},
//}

func openPageByPath(path string, id PageID, mode PageAccessMode) (*Page, error) {
	var flag int
	switch mode {
	case PageAccessModeReadWrite:
		flag = os.O_CREATE | os.O_RDWR | os.O_TRUNC
	case PageAccessModeReadWriteSync:
		flag = os.O_CREATE | os.O_RDWR | os.O_TRUNC | os.O_SYNC
	case PageAccessModeReadOnly:
		flag = os.O_RDONLY
	default:
		return nil, fmt.Errorf("invalid page mode: %d", mode)
	}

	f, err := os.OpenFile(path, flag, 0644)
	if err != nil {
		return nil, err
	}

	offset, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	return &Page{
		Id:              id,
		F:               f,
		TotalBlockCount: uint32(offset / defaultBlockSize),
		LastBlockSize:   uint32(offset % defaultBlockSize),
	}, nil
}

func (s *Page) Close(ctx context.Context) error {
	return nil
}

// Write append an arbitrary slice of bytes to the currently open segment file.
func (s *Page) Write(ctx context.Context, data []byte) (*Record, error) {
	return nil, nil
}

// TODO Implement Write []byte --> buffer --> segment file
// TODO Implement Read segment file --> [32KB]byte --> buffer
