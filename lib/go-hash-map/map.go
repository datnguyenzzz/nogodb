package go_hash_map

import (
	"sync"
	"sync/atomic"
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

type Value []byte

type CacheType byte

const (
	LRU CacheType = iota
	ClockPro
)

type Stats struct {
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
	stats  Stats

	closed bool
	state  *state
}

func (h *hashMap) GetStats() Stats {
	return h.stats
}

func (h *hashMap) Set(fileNum, key uint64, value Value) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.closed {
		return false
	}
	bucket := h.state.initBucket(h.getBucketId(fileNum, key))
	node := bucket.Get(fileNum, key)
	if node == nil {
		hash := murmur32(fileNum, key)
		node = bucket.AddNewNode(fileNum, key, hash, h)
	}

	valSize := int64(cap(value))
	node.SetValue(value, valSize)

	atomic.AddInt64(&h.stats.statSet, 1)
	atomic.AddInt64(&h.stats.statSize, valSize)

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

	bucket := h.state.initBucket(h.getBucketId(fileNum, key))
	node := bucket.Get(fileNum, key)
	if node == nil {
		atomic.AddInt64(&h.stats.statMiss, 1)
		return nil, false
	}

	atomic.AddInt64(&h.stats.statHit, 1)
	return node.ToLazyValue(), true
}

func (h *hashMap) Delete(fileNum, key uint64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return false
	}
	bucket := h.state.initBucket(h.getBucketId(fileNum, key))
	node := bucket.Get(fileNum, key)
	if node == nil {
		return false
	}

	atomic.AddInt64(&h.stats.statDel, 1)
	node.unref()

	if h.cacher != nil {
		h.cacher.Ban(node)
	}

	return true
}

// removeKV remove a node from a hashmap
//
//	caller must do the lock
func (h *hashMap) removeKV(node *kv) bool {
	if h.closed {
		return false
	}
	atomic.AddInt64(&h.stats.statDel, 1)
	bucket := h.state.initBucket(h.getBucketId(node.fileNum, node.key))
	return bucket.DeleteNode(node.fileNum, node.key, node.hash, h)
}

func (h *hashMap) getBucketId(fileNum, key uint64) int32 {
	hash := murmur32(fileNum, key)
	return int32(hash % uint32(h.state.bucketSize))
}

func (h *hashMap) Close(force bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return
	}

	h.closed = true
	var allKVs []*kv
	for i, _ := range h.state.buckets {
		bucket := h.state.initBucket(int32(i))
		bucket.mu.Lock()
		allKVs = append(allKVs, bucket.nodes...)
		bucket.mu.Unlock()
	}

	for _, kv := range allKVs {
		if force {
			atomic.StoreInt32(&kv.ref, 0)
		}
		if h.cacher != nil {
			h.cacher.Evict(kv)
		}
	}
}

func (h *hashMap) SetCapacity(capacity int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	if h.cacher != nil {
		h.cacher.SetCapacity(capacity)
	}
}

func NewMap(opts ...CacheOpt) IMap {
	state := &state{
		buckets:       make([]*bucket, initialBucketSize),
		bucketSize:    int32(initialBucketSize),
		growThreshold: int64(initialBucketSize * overflowThreshold),
	}
	for i, _ := range state.buckets {
		state.buckets[i] = &bucket{state: initialized}
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
		c.cacher = newLRU(c.maxSize)
	default:
		panic("invalid cache type")
	}

	return c
}

var _ IMap = (*hashMap)(nil)
