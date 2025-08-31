package go_block_cache

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

type handle struct {
	// point to *kv, that will be initialised when needed
	n unsafe.Pointer
}

// Release freed the associated kv cache
func (h *handle) Release() {
	nPtr := atomic.LoadPointer(&h.n)
	if nPtr == nil {
		return
	}

	if atomic.CompareAndSwapPointer(&h.n, nPtr, nil) {
		n := (*kv)(nPtr)

		if atomic.AddInt32(&n.ref, -1) <= 0 {
			n.hm.mu.RLock()
			_ = n.hm.evict(n)
			n.hm.mu.RUnlock()
		}
	}
}

func (h *handle) Load() Value {
	n := (*kv)(atomic.LoadPointer(&h.n))
	if n == nil {
		return nil
	}
	return n.value
}

var _ LazyValue = (*handle)(nil)

type kv struct {
	mu sync.Mutex
	hm *hashMap

	hash         uint32
	fileNum, key uint64
	value        Value
	size         int64

	// ref count number of instances still reference to the memory allocated for this kv
	// Since the hashMap will return the lazyValue, therefore, we might not
	// want to delete the allocated data in the memory, if there are some instances
	// still referring to it
	ref int32

	// log used to track when this kv pair got updated
	log unsafe.Pointer
}

func NewKV(fileNum, key uint64, hash uint32, hm *hashMap) *kv {
	return &kv{
		hm:      hm,
		fileNum: fileNum,
		key:     key,
		hash:    hash,
	}
}

func (n *kv) ToLazyValue() LazyValue {
	return &handle{n: unsafe.Pointer(n)}
}

func (n *kv) upRef() {
	atomic.AddInt32(&n.ref, 1)
}

func (n *kv) SetValue(value Value, size int64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.value = value
	n.size = size
}
