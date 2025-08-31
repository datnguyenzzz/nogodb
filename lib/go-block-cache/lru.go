package go_block_cache

import (
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"go.uber.org/zap"
)

type log struct {
	n          *kv
	prev, next *log
}

func (l *log) remove() {
	if l.prev == nil || l.next == nil {
		msg := fmt.Sprintf("remove a zombie node")
		zap.L().Error(msg)
		panic(msg)
	}
	l.prev.next = l.next
	l.next.prev = l.prev
	l.prev = nil
	l.next = nil
}

// insert linkage: l <--> another <--> l.next
func (l *log) insert(another *log) {
	tmp := l.next
	l.next = another
	another.prev = l
	another.next = tmp
	tmp.prev = another
}

type lru struct {
	inUse    int64
	capacity int64

	mu sync.Mutex

	// recent dummy node.
	//   dummy recent <--> 1st most recent  <--> 2nd most recent
	//   ^                                                     ^
	//   |                                                     |
	//   v                                                     v
	//   least recent <-->       ...       <--> K-th most recent
	recent *log
}

func newLRU(maxSize int64) *lru {
	dummy := new(log)
	dummy.next = dummy
	dummy.prev = dummy
	return &lru{
		capacity: maxSize,
		recent:   dummy,
	}
}

func (l *lru) SetCapacity(capacity int64) {
	l.mu.Lock()
	l.capacity = capacity
	evicted := l.balance()
	l.mu.Unlock()

	for _, n := range evicted {
		n.hm.mu.RLock()
		n.forceUnRef()
		n.hm.mu.RUnlock()
	}
}

func (l *lru) Promote(node *kv, diffSize int64) bool {
	l.mu.Lock()
	if node.log == nil {
		// the key/value pair is updated for the first time
		if node.size > l.capacity {
			return false
		}

		log := &log{n: node}
		l.recent.insert(log)
		node.log = unsafe.Pointer(log)
		atomic.AddInt64(&l.inUse, node.size)
	} else {
		log := (*log)(node.log)
		log.remove()
		l.recent.insert(log)
		atomic.AddInt64(&l.inUse, diffSize)
	}
	evicted := l.balance()
	l.mu.Unlock()

	for _, n := range evicted {
		n.hm.mu.RLock()
		n.forceUnRef()
		n.hm.mu.RUnlock()
	}

	return true
}

func (l *lru) Evict(node *kv) {
	l.mu.Lock()
	defer l.mu.Unlock()

	currLog := (*log)(node.log)
	if currLog == nil {
		return
	}
	l.RemoveLRULog(currLog)
}

// balance remove nodes to balance the maxSize.
//
//	Caller must ensure the lru is locked
func (l *lru) balance() (evicted []*kv) {
	for l.inUse > l.capacity {
		leastUpdate := l.recent.prev
		if leastUpdate == nil {
			panic("lru recent pointer is nil")
		}
		l.RemoveLRULog(leastUpdate)
		evicted = append(evicted, leastUpdate.n)
	}

	return evicted
}

func (l *lru) RemoveLRULog(lruLog *log) {
	lruLog.remove()
	lruLog.n.log = nil
	atomic.AddInt64(&l.inUse, -int64(computeSize(lruLog.n.value)))
}

func (l *lru) GetInUsed() int64 {
	return atomic.LoadInt64(&l.inUse)
}
