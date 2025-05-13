package go_wal

import (
	"context"
	"sync"
)

const (
	firstSegmentId SegmentID = 0
	blockSize                = 32 * 1024 // 32KB
)

// writeBufferPool, readBufferPool maintains a pool of 32KB buffers, each serving as a dedicated buffer for individual blocks.
// This design helps reduce garbage collection (GC) pressure and minimizes memory allocations by reusing buffers,
// eliminating the need to create new buffers for every read and write operation, so the GC doesn't have to be kicked in
// to clean up the buffers after used. Since records are guaranteed to never exceed a data size of 32KB,
// the maximum buffer size is predictable.
var readBufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, blockSize)
	},
}

// An enhancement to address the issue of inefficient memory usage, where code that
// requires a small amount of memory may receive a large buffer from the pool, and vice versa.
// For example, in the case of writeBufferPool, the size of data writes can vary, making the
// allocation of a fixed 32KB buffer wasteful.
//
// Non optimised implementation for the writeBufferPool
//var writeBufferPool = sync.Pool{
//	New: func() interface{} {
//		return make([]byte, blockSize)
//	},
//}

func openSegmentByPath(path string) (*Segment, error) {
	return nil, nil
}

func (s *Segment) Close(ctx context.Context) error {
	return nil
}

// TODO Implement Write []byte --> buffer --> segment file
// TODO Implement Read segment file --> [32KB]byte --> buffer
