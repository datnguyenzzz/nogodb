package go_hash_map

import (
	"sort"
	"sync"
	"sync/atomic"
)

type bucketState byte

const (
	uninitialized bucketState = iota
	initialized
	frozen
)

// bucket: Dynamic-sized nonblocking hash tables
// https://dl.acm.org/doi/10.1145/2611462.2611495
type bucket struct {
	mu sync.Mutex

	// nodes are sorted by its key
	nodes []*kv
	state bucketState
}

func (b *bucket) Get(fileNum, key uint64) *kv {
	i := sort.Search(len(b.nodes), func(i int) bool {
		return b.nodes[i].fileNum == fileNum && b.nodes[i].key == key
	})
	if i == -1 || i == len(b.nodes) {
		return nil
	}

	n := b.nodes[i]
	atomic.AddInt32(&n.ref, 1)
	return n
}

func (b *bucket) AddNewNode(fileNum, key uint64, hash uint32, hm *hashMap) *kv {
	b.mu.Lock()
	if b.state == frozen {
		b.mu.Unlock()
		return nil
	}

	i := sort.Search(len(b.nodes), func(i int) bool {
		if b.nodes[i].key == key {
			return b.nodes[i].fileNum >= fileNum
		}

		return b.nodes[i].key > key
	})

	newNode := NewKV(fileNum, key, hash, hm)
	if i == len(b.nodes) {
		b.nodes = append(b.nodes, newNode)
	} else {
		b.nodes = append(b.nodes[:i+1], b.nodes[i:]...)
		b.nodes[i] = newNode
	}
	b.mu.Unlock()
	b.grow(hm)

	return newNode
}

func (b *bucket) Freeze() []*kv {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state = frozen
	return b.nodes
}

func (b *bucket) grow(hm *hashMap) {
	currState := hm.state
	grow := atomic.AddInt64(&hm.stats.statNodes, 1) >= currState.growThreshold
	if len(b.nodes) > overflowThreshold {
		grow = grow || atomic.AddInt32(&currState.overflow, 1) >= overflowGrowThreshold
	}

	if !grow {
		return
	}

	if !atomic.CompareAndSwapInt32(&currState.resizing, 0, 1) {
		return
	}

	newBucketSize := 2 * len(currState.buckets)
	newState := &state{
		// all buckets have a fresh start
		buckets:         make([]*bucket, newBucketSize),
		bucketSize:      int32(newBucketSize),
		prevState:       currState,
		growThreshold:   int64(newBucketSize * overflowThreshold),
		shrinkThreshold: int64(newBucketSize / 2),
	}

	hm.state = newState
	atomic.AddInt32(&hm.stats.statGrow, 1)

	go hm.state.initBuckets()
}
