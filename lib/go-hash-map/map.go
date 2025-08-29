package go_hash_map

import (
	"reflect"
	"sync"
)

var (
	B   = 1
	KiB = 1024 * B
	MiB = 1024 * KiB
)

var (
	initialBucketSize = 1 << 4
	defaultCacheSize  = 2 * MiB
	defaultCacheType  = LRU
)

type Value interface{}

type CacheType byte

const (
	LRU CacheType = iota
	ClockPro
)

type stats struct {
	statNodes  int64
	statSize   int64
	statGrow   int32
	statShrink int32
	statHit    int64
	statMiss   int64
	statSet    int64
	statDel    int64
}

// hashMap represent a hash map
type hashMap struct {
	mu sync.RWMutex

	// options
	maxSize   int64
	cacheType CacheType

	cacher iCache
	stats  stats

	closed bool
	state  *state
}

func (h *hashMap) Set(fileNum, key uint64, value Value) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.closed {
		return false
	}
	hash := murmur32(fileNum, key)
	bucket := h.state.initBucket(int32(hash) % h.state.bucketSize)
	node := bucket.Get(fileNum, key)
	if node == nil {
		node = bucket.AddNewNode(fileNum, key, hash, h)
	}

	valSize := int64(reflect.TypeOf(value).Size())
	node.SetValue(value, valSize)

	h.stats.statSet++
	h.stats.statSize += valSize

	if value == nil {
		node.unref()
	}

	if h.cacher != nil {
		if ok := h.cacher.Promote(node); !ok {
			return false
		}
	}

	return true
}

func (h *hashMap) Get(fileNum, key uint64) (LazyValue, bool) {
	if h.closed {
		return nil, false
	}

	hash := murmur32(fileNum, key)
	bucket := h.state.initBucket(int32(hash) % h.state.bucketSize)
	node := bucket.Get(fileNum, key)
	if node == nil {
		h.stats.statMiss += 1
		return nil, false
	}

	h.stats.statHit += 1
	return node.ToLazyValue(), true
}

func (h *hashMap) Delete(fileNum, key uint64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return false
	}
	hash := murmur32(fileNum, key)
	bucket := h.state.initBucket(int32(hash) % h.state.bucketSize)
	node := bucket.Get(fileNum, key)
	if node == nil {
		return false
	}

	h.stats.statDel += 1
	node.unref()

	if h.cacher != nil {
		h.cacher.Ban(node)
	}

	return true
}

func (h *hashMap) Close() {
	//TODO implement me
	panic("implement me")
}

func NewMap(opts ...CacheOpt) IMap {
	state := &state{
		buckets:       make([]*bucket, initialBucketSize),
		bucketSize:    int32(initialBucketSize),
		growThreshold: int64(initialBucketSize * overflowThreshold),
	}
	for i, _ := range state.buckets {
		state.buckets[i].state = initialized
	}

	c := &hashMap{
		state:     state,
		maxSize:   int64(defaultCacheSize),
		cacheType: defaultCacheType,
	}

	for _, opt := range opts {
		opt(c)
	}

	switch c.cacheType {
	case LRU:
		c.cacher = newLRU()
	default:
		panic("invalid cache type")
	}

	c.cacher.SetCapacity(c.maxSize)

	return c
}

var _ IMap = (*hashMap)(nil)
