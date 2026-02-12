package go_block_cache

import (
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/dgraph-io/ristretto/v2"
	"github.com/stretchr/testify/require"
)

const (
	valueSize = 1 << 8
)

// Ristretto V2

// Sync

func Benchmark_Ristretto_Cache_Add_Read(b *testing.B) {
	cache, err := ristretto.NewCache(&ristretto.Config[uint64, []byte]{
		NumCounters: 1_000_000_000, // 5x estimated nodes
		MaxCost:     10 * KiB,
		BufferItems: 64,
		Metrics:     true,
	})
	require.NoError(b, err)
	if err != nil {
		panic(err)
	}

	cnt := 0
	for b.Loop() {
		cnt += 1
		k, v := uint64(cnt), randomBytes(valueSize)
		_ = cache.Set(k, v, int64(len(v)))
		_, _ = cache.Get(k)
		_, _ = cache.Get(uint64(rand.Intn(cnt)) + 1)
	}
	b.ReportMetric(float64(10*KiB-cache.RemainingCost()), "mem_footprint_in_bytes")
	b.ReportMetric(cache.Metrics.Ratio(), "hit_ratio")
}

// ASync

func Benchmark_Ristretto_Cache_Add_Read_Async(b *testing.B) {
	cache, err := ristretto.NewCache(&ristretto.Config[uint64, []byte]{
		NumCounters: 1_000_000_000, // 5x estimated nodes
		MaxCost:     10 * KiB,
		BufferItems: 64,
		Metrics:     true,
	})
	require.NoError(b, err)
	if err != nil {
		panic(err)
	}
	cnt := int64(0)
	b.SetParallelism(10)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			atomic.AddInt64(&cnt, 1)
			k, v := uint64(cnt), randomBytes(valueSize)
			_ = cache.Set(k, v, int64(len(v)))
			_, _ = cache.Get(k)
			_, _ = cache.Get(uint64(rand.Int63n(cnt)) + 1)
		}
	})
	b.ReportMetric(float64(10*KiB-cache.RemainingCost()), "mem_footprint_in_bytes")
	b.ReportMetric(cache.Metrics.Ratio(), "hit_ratio")
}

// NogoDB - block-cache

// Sync

func Benchmark_NogoDB_LRU_Cache_Add_Read(b *testing.B) {
	c := NewMap(
		WithCacheType(LRU),
		WithMaxSize(10*KiB),
	)

	cnt := 0
	for b.Loop() {
		cnt += 1
		k, v := uint64(cnt), randomBytes(valueSize)
		_ = c.Set(0, k, v)
		_, _ = c.Get(0, k)
		_, _ = c.Get(0, uint64(rand.Intn(cnt))+1)
	}
	b.ReportMetric(float64(c.GetInUsed()), "mem_footprint_in_bytes")
	b.ReportMetric(float64(c.GetStats().statHit)/float64(c.GetStats().statHit+c.GetStats().statMiss), "hit_ratio")
}

// ASync

func Benchmark_NogoDB_LRU_Cache_Add_Read_Async(b *testing.B) {
	c := NewMap(
		WithCacheType(LRU),
		WithMaxSize(10*KiB),
	)

	var fileNum int64
	cnt := int64(0)
	b.SetParallelism(10)
	b.RunParallel(func(pb *testing.PB) {
		atomic.AddInt64(&fileNum, 1)
		for pb.Next() {
			atomic.AddInt64(&cnt, 1)
			k, v := uint64(cnt), randomBytes(valueSize)
			_ = c.Set(0, k, v)
			_, _ = c.Get(0, k)
			_, _ = c.Get(0, uint64(rand.Int63n(cnt))+1)
		}
	})
	b.ReportMetric(float64(c.GetInUsed()), "mem_footprint_in_bytes")
	b.ReportMetric(float64(c.GetStats().statHit)/float64(c.GetStats().statHit+c.GetStats().statMiss), "hit_ratio")
}

func randomBytes(sz int) []byte {
	res := make([]byte, sz)
	for i := 0; i < sz; i++ {
		res[i] = byte(rand.Intn(256))
	}
	return res
}
