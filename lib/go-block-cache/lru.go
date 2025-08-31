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
	lz         LazyValue
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

	for _, log := range evicted {
		log.lz.Release()
	}
}

func (l *lru) Promote(node *kv) bool {
	l.mu.Lock()
	if node.log == nil {
		// the key/value pair is updated for the first time
		if node.size > l.capacity {
			return false
		}

		log := &log{n: node, lz: node.ToLazyValue()}
		l.recent.insert(log)
		node.log = unsafe.Pointer(log)
		l.inUse += node.size
	} else {
		log := (*log)(node.log)
		log.remove()
		l.recent.insert(log)
	}
	evicted := l.balance()
	l.mu.Unlock()
	for _, log := range evicted {
		log.lz.Release()
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
func (l *lru) balance() (evicted []*log) {
	for l.inUse > l.capacity {
		leastUpdate := l.recent.prev
		if leastUpdate == nil {
			panic("lru recent pointer is nil")
		}
		l.RemoveLRULog(leastUpdate)
		evicted = append(evicted, leastUpdate)
	}

	return evicted
}

func (l *lru) RemoveLRULog(lruLog *log) {
	lruLog.remove()
	lruLog.n.log = nil
	l.inUse -= int64(computeSize(lruLog.lz.Load()))
}

func (l *lru) GetInUsed() int64 {
	return atomic.LoadInt64(&l.inUse)
}
