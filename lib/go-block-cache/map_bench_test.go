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
		NumCounters: 40_000, // 5x estimated nodes
		MaxCost:     2 * MiB,
		BufferItems: 64,
	})
	require.NoError(b, err)
	defer cache.Close()
	if err != nil {
		panic(err)
	}

	for b.Loop() {
		k, v := uint64(rand.Intn(100_000)), randomBytes(valueSize)
		_ = cache.Set(k, v, valueSize)
		_, _ = cache.Get(k)
	}
	b.ReportMetric(float64(2*MiB-cache.RemainingCost()), "mem_footprint_in_bytes")
	b.ReportMetric(float64(cache.Metrics.Ratio()), "hit_ratio")
}

// ASync

func Benchmark_Ristretto_Cache_Add_Read_Async(b *testing.B) {
	cache, err := ristretto.NewCache(&ristretto.Config[uint64, []byte]{
		NumCounters: 40_000, // 5x estimated nodes
		MaxCost:     2 * MiB,
		BufferItems: 64,
	})
	require.NoError(b, err)
	defer cache.Close()
	if err != nil {
		panic(err)
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			k, v := uint64(rand.Intn(100_000)), randomBytes(valueSize)
			_ = cache.Set(k, v, valueSize)
			_, _ = cache.Get(k)
		}
	})
	b.ReportMetric(float64(2*MiB-cache.RemainingCost()), "mem_footprint_in_bytes")
	b.ReportMetric(float64(cache.Metrics.Ratio()), "hit_ratio")
}

// NogoDB - block-cache

// Sync

func Benchmark_NogoDB_LRU_Cache_Add_Read(b *testing.B) {
	c := NewMap(
		WithCacheType(LRU),
		WithMaxSize(2*MiB),
	)

	for b.Loop() {
		k, v := uint64(rand.Intn(100_000)), randomBytes(valueSize)
		_ = c.Set(0, k, v)
		_, _ = c.Get(0, k)
	}
	b.ReportMetric(float64(c.GetInUsed()), "mem_footprint_in_bytes")
	b.ReportMetric(float64(c.GetStats().statHit)/float64(c.GetStats().statHit+c.GetStats().statMiss), "hit_ratio")
}

// ASync

func Benchmark_NogoDB_LRU_Cache_Add_Read_Async(b *testing.B) {
	c := NewMap(
		WithCacheType(LRU),
		WithMaxSize(2*MiB),
	)

	var fileNum int64
	b.RunParallel(func(pb *testing.PB) {
		atomic.AddInt64(&fileNum, 1)
		for pb.Next() {
			k, v := uint64(rand.Intn(100_000)), randomBytes(valueSize)
			_ = c.Set(0, k, v)
			_, _ = c.Get(0, k)
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
