package go_block_cache

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

var (
	dummyBytes = []byte{01, 12, 23, 34, 45, 56, 67, 78, 89, 90}
)

func Test_HashMap_Set_Then_Get_Sync(t *testing.T) {
	cache := NewMap(
		WithCacheType(LRU),
	)

	dummyFileNum, dummyKey := 1, 1

	cache.Set(uint64(dummyFileNum), uint64(dummyKey), dummyBytes)
	lazyValue, ok := cache.Get(uint64(dummyFileNum), uint64(dummyKey))
	assert.True(t, ok)
	assert.NotNil(t, lazyValue)
	val := []byte(lazyValue.Load())
	assert.Equal(t, dummyBytes, val)
	// release it
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
}

func Test_Hashmap_Bulk_Set_Then_Get_And_Release_Async(t *testing.T) {
	type params struct {
		desc                                              string
		nObjects, nHandles, concurrent, repeat, cacheSize int
	}

	tests := []params{
		{"big cache - small load", 100, 10, 3, 3, 10 * MiB},
		{"big cache - medium load", 10000, 400, 50, 3, 10 * MiB},
		{"big cache - big load", 100000, 1000, 100, 10, 10 * MiB},
		{"small cache - small load", 100, 3, 3, 3, 50 * B},
		{"small cache - medium load", 10000, 400, 50, 3, 100 * B},
		{"small cache - big load", 100000, 1000, 100, 10, 100 * B},
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

			// Emulate Release a random lazyvalue, until the test is finished the loop
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

			for i := 0; i < tc.concurrent; i++ {
				wg.Add(1)
				// Set then Get new key/value pair to the cache
				go func() {
					defer wg.Done()

					r := rand.New(rand.NewSource(time.Now().UnixNano()))
					for j := 0; j < tc.nObjects*tc.repeat; j++ {
						if t.Failed() {
							return
						}
						key := r.Intn(tc.nObjects)
						ok := cache.Set(uint64(testID), uint64(key), dummyBytes)
						if !assert.True(t, ok, fmt.Sprintf("%v-%v should be updated into the cache", testID, key)) {
							return
						}
						// the cache size should never go higher the capacity of the cache
						assert.LessOrEqual(t, cache.GetInUsed(), int64(tc.cacheSize), "Cached size should not exceed the capacity")
						lazyValue, ok := cache.Get(uint64(testID), uint64(key))
						// record must be found, even in high concurrency manner
						if !assert.True(t, ok, fmt.Sprintf("%v-%v record should exist", testID, key)) {
							return
						}
						if !assert.NotNil(t, lazyValue, fmt.Sprintf("%v-%v record should exist", testID, key)) {
							return
						}
						val := []byte(lazyValue.Load())
						assert.Equal(t, dummyBytes, val, fmt.Sprintf("%v-%v lazy value should match", testID, key))
						// store the lazyValue
						lvId := r.Intn(tc.nHandles)
						if !atomic.CompareAndSwapPointer(&lazyValues[lvId], nil, unsafe.Pointer(&lazyValue)) {
							// the slot already fill, then release it
							lazyValue.Release()
						}
					}
				}()
			}

			wg.Wait()
			atomic.StoreInt32(&isDone, 1)
			// release all lazyvalues are left
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
