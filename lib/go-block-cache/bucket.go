package go_block_cache

import (
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"
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

// seekGTE return minimum pos in the b.nodes which has fileNum/key greater than or equal
// than then given.
func (b *bucket) seekGTE(fileNum, key uint64) int {
	return sort.Search(len(b.nodes), func(i int) bool {
		return b.nodes[i].key > key || (b.nodes[i].key == key && b.nodes[i].fileNum >= fileNum)
	})
}

func (b *bucket) Get(fileNum, key uint64) (isFrozen bool, n *kv) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == frozen {
		return true, nil
	}

	if len(b.nodes) == 0 {
		return false, nil
	}

	pos := b.seekGTE(fileNum, key)
	if pos == len(b.nodes) {
		return false, nil
	}

	n = b.nodes[pos]
	if n.fileNum != fileNum || n.key != key {
		return false, nil
	}

	return false, n
}

func (b *bucket) AddNewNode(fileNum, key uint64, hash uint32, hm *hashMap) (isFrozen bool, newNode *kv) {
	b.mu.Lock()
	if b.state == frozen {
		b.mu.Unlock()
		return true, nil
	}

	pos := b.seekGTE(fileNum, key)
	newNode = NewKV(fileNum, key, hash, hm)
	if pos == len(b.nodes) {
		b.nodes = append(b.nodes, newNode)
	} else {
		b.nodes = append(b.nodes[:pos+1], b.nodes[pos:]...)
		b.nodes[pos] = newNode
	}
	b.mu.Unlock()

	b.grow(hm, (*state)(atomic.LoadPointer(&hm.state)))
	return false, newNode
}

func (b *bucket) DeleteNode(fileNum, key uint64, hash uint32, hm *hashMap) (isFrozen bool, deleted bool) {
	b.mu.Lock()

	if b.state == frozen {
		b.mu.Unlock()
		return true, false
	}

	pos := b.seekGTE(fileNum, key)
	if pos == len(b.nodes) {
		b.mu.Unlock()
		return false, false
	}

	n := b.nodes[pos]
	if n.fileNum != fileNum || n.key != key {
		b.mu.Unlock()
		return false, false
	}

	if atomic.LoadInt32(&n.ref) <= 0 {
		//fmt.Printf(">>> Deleting node %d-%d from bucket-%d\n", fileNum, key, unsafe.Pointer(b))
		deleted = true
		b.nodes = append(b.nodes[:pos], b.nodes[pos+1:]...)
	}

	b.mu.Unlock()

	if deleted {
		b.shrink(hm, (*state)(atomic.LoadPointer(&hm.state)))
	}
	return false, deleted
}

func (b *bucket) Freeze() []*kv {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state = frozen
	return b.nodes
}

func (b *bucket) grow(hm *hashMap, currState *state) {
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
		buckets:         make([]bucket, newBucketSize),
		bucketMark:      uint32(newBucketSize - 1),
		prevState:       unsafe.Pointer(currState),
		growThreshold:   int64(newBucketSize * overflowThreshold),
		shrinkThreshold: int64(newBucketSize / 2),
	}

	if !atomic.CompareAndSwapPointer(&hm.state, unsafe.Pointer(currState), unsafe.Pointer(newState)) {
		panic("Failed to swap the state when growing ")
	}
	atomic.AddInt32(&hm.stats.statGrow, 1)

	go newState.initBuckets()
}

func (b *bucket) shrink(hm *hashMap, currState *state) {
	shrink := atomic.AddInt64(&hm.stats.statNodes, -1) < currState.shrinkThreshold
	if len(b.nodes) >= overflowThreshold {
		atomic.AddInt32(&currState.overflow, -1)
	}

	if !shrink {
		return
	}

	if !atomic.CompareAndSwapInt32(&currState.resizing, 0, 1) {
		return
	}

	if len(currState.buckets) <= initialBucketSize {
		return
	}

	newBucketSize := len(currState.buckets) / 2
	newState := &state{
		// all buckets have a fresh start
		buckets:         make([]bucket, newBucketSize),
		bucketMark:      uint32(newBucketSize - 1),
		prevState:       unsafe.Pointer(currState),
		growThreshold:   int64(newBucketSize * overflowThreshold),
		shrinkThreshold: int64(newBucketSize / 2),
	}

	if !atomic.CompareAndSwapPointer(&hm.state, unsafe.Pointer(currState), unsafe.Pointer(newState)) {
		panic("Failed to swap the state when growing ")
	}
	atomic.AddInt32(&hm.stats.statShrink, 1)

	go newState.initBuckets()
}
