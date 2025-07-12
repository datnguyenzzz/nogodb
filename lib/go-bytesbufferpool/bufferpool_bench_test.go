package go_bytesbufferpool

import (
	"sync"
	"testing"

	"github.com/datnguyenzzz/nogodb/lib/go-bytesbufferpool/predictable_size"
)

var (
	bufs = [][]byte{ // Total: 26MB
		makeDummyBuffer(1 * 1024 * 1024), // 1 MB
		makeDummyBuffer(4 * 1024 * 1024), // 4 MB
		makeDummyBuffer(2 * 1024 * 1024), // 2 MB
		makeDummyBuffer(8 * 1024 * 1024), // 8 MB
		makeDummyBuffer(8 * 1024 * 1024), // 8 MB
		makeDummyBuffer(2 * 1024 * 1024), // 2 MB
		makeDummyBuffer(4 * 1024 * 1024), // 4 MB
		makeDummyBuffer(1 * 1024 * 1024), // 1 MB
	}
)

func makeDummyBuffer(size int) []byte {
	buf := make([]byte, size)
	for i := 0; i < size; i++ {
		buf[i] = 0xff
	}
	return buf
}

// TODO:
//   Benchmark the Unpredictable size

func Benchmark_Generic_Buffer(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, b := range bufs {
			buf := make([]byte, len(b))
			buf = append(buf, b...)
		}
	}
}

func Benchmark_SyncPool_Buffer(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sPool := sync.Pool{
			New: func() interface{} {
				return []byte{}
			},
		}
		for _, b := range bufs {
			// get from the pool
			buf := sPool.Get().([]byte)
			if cap(buf) < len(b) {
				buf = make([]byte, len(b))
			}
			buf = append(buf, b...)
			// put back to the pool
			buf = buf[:0]
			sPool.Put(buf)
		}
	}
}

func Benchmark_Predictable_Size_Buffer(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool := predictable_size.NewPredictablePool()
		for _, b := range bufs {
			buf := pool.Get(len(b))
			buf = append(buf, b...)
			pool.Put(buf)
		}
	}
}
