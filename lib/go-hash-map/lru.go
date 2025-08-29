package go_hash_map

import "sync"

type log struct {
	kv *kv
	// ban do not allow promoting this key/value any longer
	ban        bool
	prev, next *log
}

func (l *log) remove() {
	if l.prev == nil {
		panic("remove a zombie node")
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
	//   least recent <-->         ...      <--> K-th most recent
	recent *log
}

func newLRU() *lru {
	dummy := new(log)
	dummy.next = dummy
	dummy.prev = dummy
	return &lru{
		recent: dummy,
	}
}

func (l *lru) SetCapacity(capacity int64) {
	l.mu.Lock()
	l.capacity = capacity
	evicted := l.balance()
	l.mu.Unlock()

	for _, kv := range evicted {
		kv.unref()
	}
}

func (l *lru) Promote(node *kv) bool {
	var evicted []*kv
	l.mu.Lock()
	if node.log == nil {
		// the key/value pair is updated for the first time
		if node.size > l.capacity {
			return false
		}

		log := &log{kv: node}
		node.SetLog(log)
		l.inUse += node.size

		evicted = l.balance()
	} else {
		log := node.log
		if !log.ban {
			log.remove()
			l.recent.insert(log)
		}
	}

	l.mu.Unlock()
	for _, kv := range evicted {
		kv.unref()
	}

	return true
}

func (l *lru) Evict() {
	//TODO implement me
	panic("implement me")
}

func (l *lru) Ban() {
	//TODO implement me
	panic("implement me")
}

// balance evict nodes to balance the maxSize.
//
//	Caller must ensure the lru is locked
func (l *lru) balance() (evicted []*kv) {
	for l.inUse > l.capacity {
		leastUpdate := l.recent.prev
		if leastUpdate == nil {
			panic("lru recent pointer is nil")
		}
		leastUpdate.remove()
		l.inUse -= leastUpdate.kv.size
		evicted = append(evicted, leastUpdate.kv)
	}

	return evicted
}

var _ iCache = (*lru)(nil)
