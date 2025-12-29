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
	//fmt.Printf("STATS: %#v\n", stats)
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
	// reduce the cache capacity, then evict the least recent updated node
	cache.SetCapacity(9)
	stats = cache.GetStats()
	assert.Equal(t, int64(8), cache.GetInUsed())
	assert.Equal(t, int64(6), stats.statNodes) // [2 --> 8]
	//fmt.Printf("STATS: %#v\n", stats)

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
	// Release 1st of 5 lazy values
	lazyValues[0].Release()
	// The remains 4 lazy values must still accessible, if the cache still have spaces
	for i := 1; i < times; i++ {
		val := []byte(lazyValues[i].Load())
		assert.Equal(t, dummy10Bytes, val, fmt.Sprintf("lazy value should match"))
	}

	// Release all 4 left lazy values
	for i := 1; i < times; i++ {
		lazyValues[i].Release()
	}
	_, ok = cache.Get(uint64(1), uint64(1))
	assert.False(t, ok)
}

func Test_LazyValue_Small_Cache(t *testing.T) {
	cache := NewMap(
		WithCacheType(LRU),
		WithMaxSize(10),
	)
	ok := cache.Set(uint64(1), uint64(1), dummy1Byte)
	assert.True(t, ok)
	prevLV, ok := cache.Get(uint64(1), uint64(1))
	assert.True(t, ok)
	val := []byte(prevLV.Load())
	assert.Equal(t, dummy1Byte, val, fmt.Sprintf("lazy value should match"))

	// Add New node to the cache, that big enough to evict the previous node
	ok = cache.Set(uint64(1), uint64(2), dummy10Bytes)
	assert.True(t, ok)
	assert.Equal(t, cache.GetInUsed(), int64(10))

	// The previous key/value must be evicted
	_, ok = cache.Get(uint64(1), uint64(1))
	assert.False(t, ok, "the old cache must be evicted")

	lazyValue, ok := cache.Get(uint64(1), uint64(2))
	assert.True(t, ok)
	val = lazyValue.Load()
	assert.Equal(t, dummy10Bytes, val, fmt.Sprintf("lazy value should match"))
}

func Test_Hashmap_Bulk_Set_Then_Get_And_Release_Async(t *testing.T) {
	type params struct {
		desc               string
		nObjects, nHandles int
		cacheSize          int64
	}

	randomBytes := func(sz int) []byte {
		res := make([]byte, sz)
		for i := 0; i < sz; i++ {
			res[i] = byte(rand.Intn(256))
		}
		return res
	}

	tests := []params{
		{"big cache - small load", 10, 5, 10 * MiB},
		{"big cache - medium load", 10000, 2000, 10 * MiB},
		{"big cache - big load", 100000, 30000, 10 * MiB},
		{"small cache - small load", 10, 5, 50 * B},
		{"small cache - medium load", 10000, 2000, 100 * B},
		{"small cache - big load", 100000, 30000, 100 * B},
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
					value := randomBytes(rand.Intn(20) + 1)
					ok := cache.Set(uint64(testID), uint64(i), value)
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

					odd := r.Intn(3)
					if odd%2 == 0 {
						val := []byte(lazyValue.Load())
						if !assert.Equal(t, value, val, fmt.Sprintf("%v-%v lazy value should match", testID, i)) {
							return
						}
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
			//fmt.Printf("STATS: %#v\n", stats)
		})
	}
}

func Test_HashMap_Hit_And_Miss(t *testing.T) {
	randomBytes := func(sz int) []byte {
		res := make([]byte, sz)
		for i := 0; i < sz; i++ {
			res[i] = byte(rand.Intn(256))
		}
		return res
	}

	cache := NewMap(WithCacheType(LRU))
	keySize := 10
	for i := 0; i < keySize; i++ {
		ok := cache.Set(uint64(0), uint64(i), randomBytes(keySize))
		assert.True(t, ok, fmt.Sprintf("%v-%v should be updated into the cache", uint64(0), i))
	}

	for i := 0; i < keySize; i++ {
		_, ok := cache.Get(uint64(0), uint64(i))
		assert.True(t, ok)
		_, ok = cache.Get(uint64(0), uint64(i+keySize))
		assert.False(t, ok)
	}
	//fmt.Printf("STATS: %#v\n", cache.GetStats())
}

func Test_HashMap_Delete(t *testing.T) {
	cache := NewMap(
		WithCacheType(LRU),
	)

	// Delete by using hashmap.Delete(...)
	cache.Set(0, 1, dummy10Bytes)
	lazyValue, ok := cache.Get(0, 1)
	assert.True(t, ok)
	assert.NotNil(t, lazyValue)
	val := []byte(lazyValue.Load())
	assert.Equal(t, dummy10Bytes, val)

	ok = cache.Delete(0, 2)
	assert.False(t, ok)

	ok = cache.Delete(0, 1)
	assert.True(t, ok)
	_, ok = cache.Get(0, 1)
	assert.False(t, ok)

	// Delete by using the lazyValue.Release()
	cache.Set(0, 1, dummy10Bytes)
	lazyValue, ok = cache.Get(0, 1)
	assert.True(t, ok)
	assert.NotNil(t, lazyValue)
	val = lazyValue.Load()
	assert.Equal(t, dummy10Bytes, val)

	lazyValue.Release()
	_, ok = cache.Get(0, 1)
	assert.False(t, ok)
}

func Test_HashMap_LRU_Eviction_Order(t *testing.T) {
	// Create a small cache that can only hold 20 bytes
	cache := NewMap(
		WithCacheType(LRU),
		WithMaxSize(20),
	)

	// Create test data: each entry is 5 bytes
	data := [][]byte{
		{1, 1, 1, 1, 1}, // key 1 - will be evicted first
		{2, 2, 2, 2, 2}, // key 2 - will be evicted second
		{3, 3, 3, 3, 3}, // key 3 - will be evicted third
		{4, 4, 4, 4, 4}, // key 4 - will remain
		{5, 5, 5, 5, 5}, // key 5 - will remain (triggers eviction)
	}

	// Add items in order: 1, 2, 3, 4
	for i := 0; i < 4; i++ {
		ok := cache.Set(0, uint64(i+1), data[i])
		assert.True(t, ok)
	}

	// Verify all 4 items are present (4 * 5 = 20 bytes, exactly at capacity)
	assert.Equal(t, int64(20), cache.GetInUsed())
	for i := 0; i < 4; i++ {
		_, ok := cache.Get(0, uint64(i+1))
		assert.True(t, ok, "Key %d should be present", i+1)
	}

	// Add 5th item - this should evict key 1 (least recently used)
	ok := cache.Set(0, 5, data[4])
	assert.True(t, ok)

	// Verify cache is still at capacity
	assert.Equal(t, int64(20), cache.GetInUsed())

	// Key 1 should be evicted (it was least recently used)
	_, ok = cache.Get(0, 1)
	assert.False(t, ok, "Key 1 should have been evicted")

	// Keys 2, 3, 4, 5 should still be present
	for i := 2; i <= 5; i++ {
		_, ok := cache.Get(0, uint64(i))
		assert.True(t, ok, "Key %d should still be present", i)
	}
}

func Test_HashMap_LRU_Access_Updates_Priority(t *testing.T) {
	// Create a cache that can hold 15 bytes
	cache := NewMap(
		WithCacheType(LRU),
		WithMaxSize(15),
	)

	// Create test data: each entry is 5 bytes
	data := [][]byte{
		{1, 1, 1, 1, 1}, // key 1
		{2, 2, 2, 2, 2}, // key 2
		{3, 3, 3, 3, 3}, // key 3
		{4, 4, 4, 4, 4}, // key 4 - will trigger eviction
	}

	// Add items 1, 2, 3 (fills cache to capacity)
	for i := 0; i < 3; i++ {
		ok := cache.Set(0, uint64(i+1), data[i])
		assert.True(t, ok)
	}

	// Access key 1 to make it most recently used
	// Order should now be: 2 (LRU), 3, 1 (MRU)
	_, ok := cache.Get(0, 1)
	assert.True(t, ok)

	// Add key 4 - this should evict key 2 (now the LRU)
	ok = cache.Set(0, 4, data[3])
	assert.True(t, ok)

	// Key 2 should be evicted because it became LRU after we accessed key 1
	_, ok = cache.Get(0, 2)
	assert.False(t, ok, "Key 2 should have been evicted")

	// Keys 1, 3, 4 should still be present
	for _, key := range []uint64{1, 3, 4} {
		_, ok := cache.Get(0, key)
		assert.True(t, ok, "Key %d should still be present", key)
	}
}

func Test_HashMap_LRU_Set_Updates_Priority(t *testing.T) {
	// Create a cache that can hold 15 bytes
	cache := NewMap(
		WithCacheType(LRU),
		WithMaxSize(15),
	)

	data1 := []byte{1, 1, 1, 1, 1} // 5 bytes
	data2 := []byte{2, 2, 2, 2, 2} // 5 bytes
	data3 := []byte{3, 3, 3, 3, 3} // 5 bytes
	data4 := []byte{4, 4, 4, 4, 4} // 5 bytes

	// Add items 1, 2, 3 (fills cache to capacity)
	cache.Set(0, 1, data1)
	cache.Set(0, 2, data2)
	cache.Set(0, 3, data3)

	// Update key 1 (this should make it most recently used)
	// Order should now be: 2 (LRU), 3, 1 (MRU)
	cache.Set(0, 1, data1)

	// Add key 4 - this should evict key 2 (LRU)
	ok := cache.Set(0, 4, data4)
	assert.True(t, ok)

	// Key 2 should be evicted
	_, ok = cache.Get(0, 2)
	assert.False(t, ok, "Key 2 should have been evicted")

	// Keys 1, 3, 4 should still be present
	for _, key := range []uint64{1, 3, 4} {
		_, ok := cache.Get(0, key)
		assert.True(t, ok, "Key %d should still be present", key)
	}
}

func Test_HashMap_LRU_Sequential_Eviction(t *testing.T) {
	// Create a cache that can hold exactly 10 bytes
	cache := NewMap(
		WithCacheType(LRU),
		WithMaxSize(10),
	)

	// Add 5 items of 2 bytes each (fills cache to capacity)
	for i := 1; i <= 5; i++ {
		data := []byte{byte(i), byte(i)}
		ok := cache.Set(0, uint64(i), data)
		assert.True(t, ok)
	}

	assert.Equal(t, int64(10), cache.GetInUsed())

	// Add items 6, 7, 8 - each should evict the oldest item
	for i := 6; i <= 8; i++ {
		data := []byte{byte(i), byte(i)}
		ok := cache.Set(0, uint64(i), data)
		assert.True(t, ok)

		// Check that the oldest item was evicted
		evictedKey := uint64(i - 5)
		_, ok = cache.Get(0, evictedKey)
		assert.False(t, ok, "Key %d should have been evicted when adding key %d", evictedKey, i)

		assert.Equal(t, int64(10), cache.GetInUsed())
	}

	// Keys 4, 5, 6, 7, 8 should remain
	for i := 4; i <= 8; i++ {
		_, ok := cache.Get(0, uint64(i))
		assert.True(t, ok, "Key %d should still be present", i)
	}
}

func Test_HashMap_LRU_Complex_Access_Pattern(t *testing.T) {
	// Create a cache that can hold 12 bytes
	cache := NewMap(
		WithCacheType(LRU),
		WithMaxSize(12),
	)

	// Each item is 3 bytes, so we can hold 4 items max
	data := [][]byte{
		{1, 1, 1}, // key 1
		{2, 2, 2}, // key 2
		{3, 3, 3}, // key 3
		{4, 4, 4}, // key 4
		{5, 5, 5}, // key 5
	}

	// Add keys 1, 2, 3, 4 (fills cache)
	for i := 0; i < 4; i++ {
		ok := cache.Set(0, uint64(i+1), data[i])
		assert.True(t, ok)
	}

	// Access pattern: 2, 1, 3 (making 4 the LRU)
	cache.Get(0, 2)
	cache.Get(0, 1)
	cache.Get(0, 3)
	// LRU order should now be: 4 (LRU), 2, 1, 3 (MRU)

	// Add key 5 - should evict key 4
	ok := cache.Set(0, 5, data[4])
	assert.True(t, ok)

	// Key 4 should be evicted
	_, ok = cache.Get(0, 4)
	assert.False(t, ok, "Key 4 should have been evicted")

	// Keys 1, 2, 3, 5 should be present
	for _, key := range []uint64{1, 2, 3, 5} {
		_, ok := cache.Get(0, key)
		assert.True(t, ok, "Key %d should still be present", key)
	}

	// Access key 2 again, then add another item
	cache.Get(0, 2)
	// LRU order: 1 (LRU), 3, 5, 2 (MRU)

	data6 := []byte{6, 6, 6}
	ok = cache.Set(0, 6, data6)
	assert.True(t, ok)

	// Key 1 should now be evicted
	_, ok = cache.Get(0, 1)
	assert.False(t, ok, "Key 1 should have been evicted")

	// Keys 2, 3, 5, 6 should be present
	for _, key := range []uint64{2, 3, 5, 6} {
		_, ok := cache.Get(0, key)
		assert.True(t, ok, "Key %d should still be present", key)
	}
}
