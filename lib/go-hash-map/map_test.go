package go_hash_map

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
	assert.Zero(t, stats.statSize, "Stats size should be zero")
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
	assert.Equal(t, int64(10), stats.statSize)
	assert.Equal(t, int64(7), stats.statNodes) // [1 --> 8]
	// cache the cache capacity
	cache.SetCapacity(9)
	stats = cache.GetStats()
	assert.Equal(t, int64(8), stats.statSize)
	assert.Equal(t, int64(6), stats.statNodes) // [2 --> 8]
}

func Test_Hashmap_Bulk_Set_Then_Get_Async(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())

	type params struct {
		desc                                              string
		nObjects, nHandles, concurrent, repeat, cacheSize int
	}

	tests := []params{
		{"big cache - small load", 1000, 1, 1, 1, 10 * MiB},
		{"big cache - medium load", 10000, 400, 50, 3, 10 * MiB},
		{"big cache - big load", 100000, 1000, 100, 10, 10 * MiB},
		{"small cache - small load", 1000, 1, 1, 1, 100 * B},
		{"small cache - medium load", 10000, 400, 50, 3, 100 * B},
		{"small cache - big load", 100000, 1000, 100, 10, 100 * B},
	}

	for testID, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			cache := NewMap(
				WithCacheType(LRU),
			)
			lazyValues := make([]unsafe.Pointer, tc.nHandles)
			var isDone int32

			wg := new(sync.WaitGroup)
			for i := 0; i < tc.concurrent; i++ {
				wg.Add(1)
				// Set then Get new key/value pair to the cache
				go func() {
					defer wg.Done()
					r := rand.New(rand.NewSource(time.Now().UnixNano()))
					for j := 0; j < tc.repeat; j++ {
						key := r.Intn(tc.nObjects)
						ok := cache.Set(uint64(testID), uint64(key), dummyBytes)
						assert.True(t, ok, "record should be updated into the cache")
						lazyValue, ok := cache.Get(uint64(testID), uint64(key))
						// record must be found, even in high concurrency manner
						assert.True(t, ok, "record should exist")
						assert.NotNil(t, lazyValue, "record should exist")
						val := []byte(lazyValue.Load())
						assert.Equal(t, dummyBytes, val, "record should matched")
						// store the lazyValue
						if !atomic.CompareAndSwapPointer(&lazyValues[r.Intn(tc.nHandles)], nil, unsafe.Pointer(&lazyValue)) {
							// the slot already fill, then release it
							lazyValue.Release()
						}
					}
				}()

				// Emulate Release a random lazyvalue, until the test is finished the loop
				go func() {
					r := rand.New(rand.NewSource(time.Now().UnixNano()))
					for atomic.LoadInt32(&isDone) == 1 {
						id := r.Intn(tc.nHandles)
						lazyValue := (*LazyValue)(atomic.LoadPointer(&lazyValues[id]))
						if lazyValue != nil && atomic.CompareAndSwapPointer(&lazyValues[id], unsafe.Pointer(lazyValue), nil) {
							(*lazyValue).Release()
						}
						time.Sleep(time.Millisecond)
					}
				}()
			}

			wg.Wait()
			atomic.StoreInt32(&isDone, 1)
			// release all lazyvalues are left
			for i, _ := range lazyValues {
				lazyValue := (*LazyValue)(atomic.LoadPointer(&lazyValues[i]))
				if lazyValue != nil && atomic.CompareAndSwapPointer(&lazyValues[i], unsafe.Pointer(lazyValue), nil) {
					(*lazyValue).Release()
				}
			}

			// check the stats
			stats := cache.GetStats()
			assert.Zero(t, stats.statNodes, "Stats nodes should be zero")
			assert.Zero(t, stats.statSize, "Stats size should be zero")
			fmt.Printf("STATS: %#v\n", stats)
		})
	}
}
