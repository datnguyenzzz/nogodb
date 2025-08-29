package go_hash_map

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
func (h handle) Release() {
	nPtr := atomic.LoadPointer(&h.n)
	if nPtr == nil {
		return
	}

	if atomic.CompareAndSwapPointer(&h.n, nPtr, nil) {
		n := (*kv)(nPtr)
		n.unref()
	}
}

func (h handle) Load() Value {
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
	ref int32

	// log used to track when this kv pair got updated
	log *log
}

func NewKV(fileNum, key uint64, hash uint32, hm *hashMap) *kv {
	return &kv{
		hm:      hm,
		fileNum: fileNum,
		key:     key,
		hash:    hash,
		ref:     1,
	}
}

func (n *kv) ToLazyValue() LazyValue {
	return &handle{n: unsafe.Pointer(n)}
}

func (n *kv) unref() {
	if atomic.AddInt32(&n.ref, -1) < 0 {
		// delete the kv from the hash map
		n.hm.mu.RLock()
		if !n.hm.closed {
			n.hm.Delete(n.fileNum, n.key)
		}
		n.hm.mu.RUnlock()
	}
}

func (n *kv) SetValue(value Value, size int64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.value = value
	n.size = size
}

func (n *kv) SetLog(log *log) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.log = log
}
