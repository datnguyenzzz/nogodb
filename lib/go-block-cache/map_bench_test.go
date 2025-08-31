package go_block_cache

import (
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/dgraph-io/ristretto/v2"
)

const (
	valueSize = 1 << 8
)

// Ristretto V2

// Sync

func Benchmark_Ristretto_Cache_Add_Read(b *testing.B) {
	b.StopTimer()
	cache, err := ristretto.NewCache(&ristretto.Config[uint64, []byte]{
		NumCounters: 40_000, // 5x estimated nodes
		MaxCost:     2 * MiB,
		BufferItems: 64,
	})
	defer cache.Close()
	if err != nil {
		panic(err)
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_ = cache.Set(uint64(i), randomBytes(valueSize), valueSize)
		_, _ = cache.Get(uint64(i))
	}
	b.ReportAllocs()
	b.ReportMetric(float64(2*MiB-cache.RemainingCost()), "mem_footprint_in_bytes")
}

func Benchmark_Ristretto_Cache_Add(b *testing.B) {
	b.StopTimer()
	cache, err := ristretto.NewCache(&ristretto.Config[uint64, []byte]{
		NumCounters: 40_000, // 5x estimated nodes
		MaxCost:     2 * MiB,
		BufferItems: 64,
	})
	defer cache.Close()
	if err != nil {
		panic(err)
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_ = cache.Set(uint64(i), randomBytes(valueSize), valueSize)
	}
	b.ReportAllocs()
	b.ReportMetric(float64(2*MiB-cache.RemainingCost()), "mem_footprint_in_bytes")
}

func Benchmark_Ristretto_Cache_Read(b *testing.B) {
	b.StopTimer()
	cache, err := ristretto.NewCache(&ristretto.Config[uint64, []byte]{
		NumCounters: 40_000, // 5x estimated nodes
		MaxCost:     2 * MiB,
		BufferItems: 64,
	})
	defer cache.Close()
	if err != nil {
		panic(err)
	}

	for i := 0; i < b.N; i++ {
		_ = cache.Set(uint64(i), randomBytes(valueSize), valueSize)
	}
	cache.Wait()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cache.Get(uint64(i))
	}

	b.ReportAllocs()
	b.ReportMetric(float64(2*MiB-cache.RemainingCost()), "mem_footprint_in_bytes")
}

// ASync

func Benchmark_Ristretto_Cache_Add_Read_Async(b *testing.B) {
	b.StopTimer()
	cache, err := ristretto.NewCache(&ristretto.Config[uint64, []byte]{
		NumCounters: 40_000, // 5x estimated nodes
		MaxCost:     2 * MiB,
		BufferItems: 64,
	})
	defer cache.Close()
	if err != nil {
		panic(err)
	}

	b.StartTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i uint64
		for pb.Next() {
			_ = cache.Set(i, randomBytes(valueSize), valueSize)
			_, _ = cache.Get(i)

			i += 1
		}
	})
	b.ReportAllocs()
	b.ReportMetric(float64(2*MiB-cache.RemainingCost()), "mem_footprint_in_bytes")
}

func Benchmark_Ristretto_Cache_Add_Async(b *testing.B) {
	b.StopTimer()
	cache, err := ristretto.NewCache(&ristretto.Config[uint64, []byte]{
		NumCounters: 40_000, // 5x estimated nodes
		MaxCost:     2 * MiB,
		BufferItems: 64,
	})
	defer cache.Close()
	if err != nil {
		panic(err)
	}

	var fileNum int64
	b.StartTimer()
	b.RunParallel(func(pb *testing.PB) {
		atomic.AddInt64(&fileNum, 1)
		var i uint64
		for pb.Next() {
			_ = cache.Set(i, randomBytes(valueSize), valueSize)

			i += 1
		}
	})
	b.ReportAllocs()
	b.ReportMetric(float64(2*MiB-cache.RemainingCost()), "mem_footprint_in_bytes")
}

func Benchmark_Ristretto_Cache_Read_Async(b *testing.B) {
	b.StopTimer()
	cache, err := ristretto.NewCache(&ristretto.Config[uint64, []byte]{
		NumCounters: 40_000, // 5x estimated nodes
		MaxCost:     2 * MiB,
		BufferItems: 64,
	})
	defer cache.Close()
	if err != nil {
		panic(err)
	}

	for i := 0; i < b.N; i++ {
		_ = cache.Set(uint64(i), randomBytes(valueSize), valueSize)
	}

	cache.Wait()

	var counter uint64
	b.StartTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddUint64(&counter, 1) - 1
			_, _ = cache.Get(i)
		}
	})
	b.ReportAllocs()
	b.ReportMetric(float64(2*MiB-cache.RemainingCost()), "mem_footprint_in_bytes")
}

// NogoDB - block-cache

// Sync

func Benchmark_NogoDB_Cache_Add_Read(b *testing.B) {
	b.StopTimer()
	c := NewMap(
		WithCacheType(LRU),
		WithMaxSize(2*MiB),
	)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_ = c.Set(0, uint64(i), randomBytes(valueSize))
		_, _ = c.Get(0, uint64(i))
	}
	b.ReportAllocs()
	b.ReportMetric(float64(c.GetInUsed()), "mem_footprint_in_bytes")
}

func Benchmark_NogoDB_Cache_Add(b *testing.B) {
	b.StopTimer()
	c := NewMap(
		WithCacheType(LRU),
		WithMaxSize(2*MiB),
	)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_ = c.Set(0, uint64(i), randomBytes(valueSize))
	}
	b.ReportAllocs()
	b.ReportMetric(float64(c.GetInUsed()), "mem_footprint_in_bytes")
}

func Benchmark_NogoDB_Cache_Read(b *testing.B) {
	b.StopTimer()
	c := NewMap(
		WithCacheType(LRU),
		WithMaxSize(2*MiB),
	)

	for i := 0; i < b.N; i++ {
		_ = c.Set(0, uint64(i), randomBytes(valueSize))
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.Get(0, uint64(i))
	}

	b.ReportAllocs()
	b.ReportMetric(float64(c.GetInUsed()), "mem_footprint_in_bytes")
}

// ASync

func Benchmark_NogoDB_Cache_Add_Read_Async(b *testing.B) {
	b.StopTimer()
	c := NewMap(
		WithCacheType(LRU),
		WithMaxSize(2*MiB),
	)

	var fileNum int64
	b.StartTimer()
	b.RunParallel(func(pb *testing.PB) {
		atomic.AddInt64(&fileNum, 1)
		var i uint64
		for pb.Next() {
			_ = c.Set(0, i, randomBytes(valueSize))
			_, _ = c.Get(0, i)

			i += 1
		}
	})
	b.ReportAllocs()
	b.ReportMetric(float64(c.GetInUsed()), "mem_footprint_in_bytes")
}

func Benchmark_NogoDB_Cache_Add_Async(b *testing.B) {
	b.StopTimer()
	c := NewMap(
		WithCacheType(LRU),
		WithMaxSize(2*MiB),
	)

	var fileNum int64
	b.StartTimer()
	b.RunParallel(func(pb *testing.PB) {
		atomic.AddInt64(&fileNum, 1)
		var i uint64
		for pb.Next() {
			_ = c.Set(0, i, randomBytes(valueSize))
			i += 1
		}
	})
	b.ReportAllocs()
	b.ReportMetric(float64(c.GetInUsed()), "mem_footprint_in_bytes")
}

func Benchmark_NogoDB_Cache_Read_Async(b *testing.B) {
	b.StopTimer()
	c := NewMap(
		WithCacheType(LRU),
		WithMaxSize(2*MiB),
	)

	for i := 0; i < b.N; i++ {
		_ = c.Set(0, uint64(i), randomBytes(valueSize))
	}
	var counter uint64
	b.StartTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddUint64(&counter, 1) - 1
			_, _ = c.Get(0, i)
		}
	})
	b.ReportAllocs()
	b.ReportMetric(float64(c.GetInUsed()), "mem_footprint_in_bytes")
}

func randomBytes(sz int) []byte {
	res := make([]byte, sz)
	for i := 0; i < sz; i++ {
		res[i] = byte(rand.Intn(256))
	}
	return res
}
