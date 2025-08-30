package go_block_cache

import (
	"fmt"
	"sync"

	"go.uber.org/zap"
)

type log struct {
	kv LazyValue
	// ban do not allow promoting this key/value any longer
	ban        bool
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

	for _, kv := range evicted {
		kv.Release()
	}
}

func (l *lru) Promote(node *kv) bool {
	l.mu.Lock()
	if node.log == nil {
		// the key/value pair is updated for the first time
		if node.size > l.capacity {
			return false
		}

		log := &log{kv: node.ToLazyValue()}
		node.SetLog(log)
		l.inUse += node.size
	} else {
		log := node.log
		if !log.ban {
			log.remove()
		}
	}
	l.recent.insert(node.log)
	evicted := l.balance()

	l.mu.Unlock()
	for _, kv := range evicted {
		kv.Release()
	}

	return true
}

func (l *lru) Evict(node *kv) {
	l.mu.Lock()
	defer l.mu.Unlock()
	currLog := node.log
	if currLog == nil || currLog.ban {
		return
	}

	l.inUse -= node.size
	currLog.remove()
	node.SetLog(nil)
	node.unref()
}

func (l *lru) Ban(node *kv) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if node.log == nil {
		node.log = &log{kv: node.ToLazyValue(), ban: true}
	} else {
		currLog := node.log
		if !currLog.ban {
			currLog.remove()
			node.SetLog(nil)
			currLog.ban = true
			l.inUse -= node.size

			node.unref()
		}
	}
}

// balance evict nodes to balance the maxSize.
//
//	Caller must ensure the lru is locked
func (l *lru) balance() (evicted []LazyValue) {
	for l.inUse > l.capacity {
		leastUpdate := l.recent.prev
		if leastUpdate == nil {
			panic("lru recent pointer is nil")
		}
		leastUpdate.remove()
		l.inUse -= int64(cap(leastUpdate.kv.Load()))
		leastUpdate.kv = nil
		evicted = append(evicted, leastUpdate.kv)
	}

	return evicted
}

var _ iCache = (*lru)(nil)
