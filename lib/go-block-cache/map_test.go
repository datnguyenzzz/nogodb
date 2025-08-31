package go_block_cache

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

var (
	dummy10Bytes = []byte{01, 12, 23, 34, 45, 56, 67, 78, 89, 90}
	dummy1Byte   = []byte{10}
)

func Test_HashMap_Set_Then_Get_Sync(t *testing.T) {
	cache := NewMap(
		WithCacheType(LRU),
	)

	dummyFileNum, dummyKey := 1, 1

	cache.Set(uint64(dummyFileNum), uint64(dummyKey), dummy10Bytes)
	lazyValue, ok := cache.Get(uint64(dummyFileNum), uint64(dummyKey))
	assert.True(t, ok)
	assert.NotNil(t, lazyValue)
	val := []byte(lazyValue.Load())
	assert.Equal(t, dummy10Bytes, val)

	cache.Set(uint64(dummyFileNum), uint64(dummyKey), dummy1Byte)
	lazyValue, ok = cache.Get(uint64(dummyFileNum), uint64(dummyKey))
	assert.True(t, ok)
	assert.NotNil(t, lazyValue)
	val = lazyValue.Load()
	assert.Equal(t, dummy1Byte, val)

	lazyValue.Release()
	// verify stats
	stats := cache.GetStats()
	assert.Zero(t, stats.statNodes, "Stats nodes should be zero")
	assert.Zero(t, cache.GetInUsed(), "Stats size should be zero")
	fmt.Printf("STATS: %#v\n", stats)
}

func Test_HashMap_Capacity_Resizing(t *testing.T) {
	type params struct {
		fileNum, fileKey uint64
		value            []byte
	}

	cache := NewMap(
		WithCacheType(LRU),
		WithMaxSize(10),
	)

	dummyBytes := func(sz int) []byte {
		return make([]byte, sz)
	}

	sequences := []params{
		{0, 1, dummyBytes(1)},
		{0, 2, dummyBytes(2)},
		{1, 1, dummyBytes(3)},
		{2, 1, dummyBytes(1)},
		{2, 2, dummyBytes(1)},
		{2, 3, dummyBytes(1)},
		{2, 4, dummyBytes(1)},
		{2, 5, dummyBytes(1)},
	}
	for _, sequence := range sequences {
		ok := cache.Set(sequence.fileNum, sequence.fileKey, sequence.value)
		assert.True(t, ok)
	}
	stats := cache.GetStats()
	assert.Equal(t, int64(10), cache.GetInUsed())
	assert.Equal(t, int64(7), stats.statNodes) // [1 --> 8]
	// reduce the cache capacity, then remove the least recent updated node
	cache.SetCapacity(9)
	stats = cache.GetStats()
	assert.Equal(t, int64(8), cache.GetInUsed())
	assert.Equal(t, int64(6), stats.statNodes) // [2 --> 8]
	fmt.Printf("STATS: %#v\n", stats)

}

func Test_LazyValue_Release(t *testing.T) {
	cache := NewMap(
		WithCacheType(LRU),
	)
	ok := cache.Set(uint64(1), uint64(1), dummy10Bytes)
	assert.True(t, ok)
	times := 5
	// Get the lazy value of the given block
	lazyValues := make([]LazyValue, times)
	for i := 0; i < times; i++ {
		var ok bool
		lazyValues[i], ok = cache.Get(uint64(1), uint64(1))
		assert.True(t, ok)
	}
	// Release 1 of 5 lazy values
	lazyValues[0].Release()
	// The remains 4 lazy values must still accessible, if the cache still have spaces
	for i := 1; i < times; i++ {
		val := []byte(lazyValues[i].Load())
		assert.Equal(t, dummy10Bytes, val, fmt.Sprintf("lazy value should match"))
	}
}

func Test_Hashmap_Bulk_Set_Then_Get_And_Release_Async(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	type params struct {
		desc                          string
		nObjects, nHandles, cacheSize int
	}

	tests := []params{
		{"big cache - small load", 10, 5, 10 * MiB},
		{"big cache - medium load", 1000, 200, 10 * MiB},
		{"big cache - big load", 10000, 3000, 10 * MiB},
		{"small cache - small load", 10, 5, 50 * B},
		{"small cache - medium load", 1000, 200, 100 * B},
		{"small cache - big load", 10000, 3000, 100 * B},
	}

	for testID, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			cache := NewMap(
				WithCacheType(LRU),
			)
			// lazyValues saved lazy values, for the delayed release
			lazyValues := make([]unsafe.Pointer, tc.nHandles)
			var isDone int32

			wg := new(sync.WaitGroup)

			// Emulate Release a random lazy value, until the test is finished the loop
			go func() {
				r := rand.New(rand.NewSource(time.Now().UnixNano()))
				for atomic.LoadInt32(&isDone) == 0 {
					id := r.Intn(tc.nHandles)
					lazyValue := (*LazyValue)(atomic.LoadPointer(&lazyValues[id]))
					if lazyValue != nil {
						(*lazyValue).Release()
						atomic.StorePointer(&lazyValues[id], nil)
					}
					time.Sleep(time.Millisecond)
				}
			}()

			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			for i := 0; i < tc.nObjects; i++ {
				wg.Add(1)
				// Set then Get new key/value pair to the cache
				go func() {
					defer wg.Done()

					if t.Failed() {
						return
					}
					ok := cache.Set(uint64(testID), uint64(i), dummy10Bytes)
					if !assert.True(t, ok, fmt.Sprintf("%v-%v should be updated into the cache", testID, i)) {
						return
					}

					lazyValue, ok := cache.Get(uint64(testID), uint64(i))
					if !assert.True(t, ok, fmt.Sprintf("%v-%v record should exist", testID, i)) {
						return
					}
					if !assert.NotNil(t, lazyValue, fmt.Sprintf("%v-%v record should exist", testID, i)) {
						return
					}

					val := []byte(lazyValue.Load())
					if !assert.Equal(t, dummy10Bytes, val, fmt.Sprintf("%v-%v lazy value should match", testID, i)) {
						return
					}

					// store the lazyValue for the delayed releasing
					lvId := r.Intn(tc.nHandles)
					if !atomic.CompareAndSwapPointer(&lazyValues[lvId], nil, unsafe.Pointer(&lazyValue)) {
						lazyValue.Release()
					}
				}()
			}

			wg.Wait()
			atomic.StoreInt32(&isDone, 1)

			// release all lazy values that are pending
			for i, _ := range lazyValues {
				lazyValue := (*LazyValue)(atomic.LoadPointer(&lazyValues[i]))
				if lazyValue != nil {
					(*lazyValue).Release()
					atomic.StorePointer(&lazyValues[i], nil)
				}
			}

			// check the stats
			stats := cache.GetStats()
			assert.Zero(t, stats.statNodes, "Stats Nodes should be zero")
			assert.Zero(t, cache.GetInUsed(), "Cached size should be zero")
			fmt.Printf("STATS: %#v\n", stats)
		})
	}
}
