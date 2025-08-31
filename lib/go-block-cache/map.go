package go_block_cache

import (
	"sync"
	"sync/atomic"
	"unsafe"
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

	cacher *lru
	stats  Stats

	closed bool
	// points to state. As in the state don't have mutex
	state unsafe.Pointer
}

func (h *hashMap) GetStats() Stats {
	return h.stats
}

func (h *hashMap) GetInUsed() int64 {
	return h.cacher.GetInUsed()
}

func (h *hashMap) Set(fileNum, key uint64, value Value) bool {
	h.mu.RLock()
	if h.closed {
		h.mu.RUnlock()
		return false
	}

	// defer until the bucket is initialised (aka migrate data from a frozen to a new bucket)
	for {
		state := (*state)(atomic.LoadPointer(&h.state))
		bucket := state.lazyLoadBucket(h.getBucketId(fileNum, key, state))
		isFrozen, node := bucket.Get(fileNum, key)
		if isFrozen {
			continue
		}

		if node == nil {
			hash := murmur32(fileNum, key)
			isFrozen, node = bucket.AddNewNode(fileNum, key, hash, h)
		}
		if isFrozen {
			continue
		}

		if value == nil || computeSize(value) == 0 {
			node.unRef()
			h.mu.RUnlock()
			return true
		}

		valSize := int64(computeSize(value))
		diffSize := valSize - node.size
		node.SetValue(value, valSize)
		atomic.StoreInt32(&node.ref, 0)
		atomic.AddInt64(&h.stats.statSet, 1)
		h.mu.RUnlock()

		if h.cacher != nil {
			if ok := h.cacher.Promote(node, diffSize); !ok {
				return false
			}
		}

		break
	}

	return true
}

func (h *hashMap) Get(fileNum, key uint64) (LazyValue, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.closed {
		return nil, false
	}
	var isFrozen bool
	var node *kv
	// defer until the bucket is initialised (aka migrate data from a frozen to a new bucket)
	for {
		state := (*state)(atomic.LoadPointer(&h.state))
		bucket := state.lazyLoadBucket(h.getBucketId(fileNum, key, state))
		isFrozen, node = bucket.Get(fileNum, key)
		if isFrozen {
			continue
		}
		if node == nil {
			atomic.AddInt64(&h.stats.statMiss, 1)
			return nil, false
		}

		atomic.AddInt64(&h.stats.statHit, 1)
		node.upRef()
		break
	}

	return node.ToLazyValue(), true
}

func (h *hashMap) Delete(fileNum, key uint64) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.closed {
		return false
	}
	var isFrozen bool
	var node *kv
	// defer until the bucket is initialised (aka migrate data from a frozen to a new bucket)
	for {
		state := (*state)(atomic.LoadPointer(&h.state))
		bucket := state.lazyLoadBucket(h.getBucketId(fileNum, key, state))
		isFrozen, node = bucket.Get(fileNum, key)
		if isFrozen {
			continue
		}
		if node == nil {
			return false
		}

		node.unRef()
		break
	}

	return true
}

// remove removes a node from a hashmap
//
//	Important: caller must ensure the Rlock of the hashmap
func (h *hashMap) remove(node *kv) bool {
	if h.closed {
		return false
	}
	// defer until the bucket is initialised
	var removed, isFrozen bool
	for {
		state := (*state)(atomic.LoadPointer(&h.state))
		bucket := state.lazyLoadBucket(h.getBucketId(node.fileNum, node.key, state))
		isFrozen, removed = bucket.DeleteNode(node.fileNum, node.key, node.hash, h)
		if isFrozen {
			continue
		}

		if removed {
			h.cacher.Evict(node)
			node = nil
			atomic.AddInt64(&h.stats.statDel, 1)
		}

		break
	}

	return removed
}

func (h *hashMap) getBucketId(fileNum, key uint64, state *state) uint32 {
	hash := murmur32(fileNum, key)
	return hash & state.bucketMark
}

func (h *hashMap) Close(force bool) {
	// mutex lock to ensure all the shared locks are cleared
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return
	}

	h.closed = true
	var allKVs []*kv
	state := (*state)(atomic.LoadPointer(&h.state))
	for i, _ := range state.buckets {
		bucket := state.lazyLoadBucket(uint32(i))
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

	atomic.StorePointer(&h.state, nil)
}

func (h *hashMap) SetCapacity(capacity int64) {
	if h.closed {
		return
	}
	if h.cacher != nil {
		h.cacher.SetCapacity(capacity)
	}
}

func NewMap(opts ...CacheOpt) IMap {
	state := &state{
		buckets:       make([]bucket, initialBucketSize),
		bucketMark:    uint32(initialBucketSize - 1),
		growThreshold: int64(initialBucketSize * overflowThreshold),
	}
	for i, _ := range state.buckets {
		state.buckets[i].state = initialized
	}

	c := &hashMap{
		state:     unsafe.Pointer(state),
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
