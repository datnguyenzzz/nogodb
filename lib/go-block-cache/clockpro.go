package go_block_cache

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

type clockPro struct {
	mu sync.RWMutex

	maxSize int64 // max (hot + cold) memory that the shard can hold
	maxCold int64 // max cold memory that the shard can hold

	sizeHot  int64
	sizeCold int64
	sizeTest int64

	handHot  *log
	handCold *log
	handTest *log
}

// GetInUsed returns the current in-use size of the cache,
func (c *clockPro) GetInUsed() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	inUsed := c.sizeHot + c.sizeCold

	return inUsed
}

// Promote promotes the given node in the cache
// diffSize is the size difference between the new value and the old value of the node
func (c *clockPro) Promote(node *kv, diffSize int64, o op) bool {
	switch o {
	case opGet:
		if !node.usedBit.Load() {
			node.usedBit.Store(true)
		}
	case opSet:
		switch {
		case node.log == nil:
			// new node that is not yet in the cache,
			// move it to the cold node
			node.usedBit.Store(false)
			if !c.logAdd(node, cold) {
				return false
			}
			atomic.AddInt64(&c.sizeCold, diffSize)

		case (*log)(atomic.LoadPointer(&node.log)).clockType == test:
			// belongs to the test nodes
			// revive it from test to hot node
			if !c.logDel(node) {
				return false
			}

			node.usedBit.Store(false)
			if atomic.LoadInt64(&c.maxCold) < atomic.LoadInt64(&c.maxSize) {
				atomic.AddInt64(&c.maxCold, diffSize)
			}
			atomic.AddInt64(&c.sizeTest, -diffSize)

			if !c.logAdd(node, hot) {
				return false
			}
			atomic.AddInt64(&c.sizeHot, diffSize)

		default:
			// belongs to the hot or cold nodes
			if !node.usedBit.Load() {
				node.usedBit.Store(true)
			}
			l := (*log)(node.log)
			if l.clockType == hot {
				atomic.AddInt64(&c.sizeHot, diffSize)
			} else {
				// TODO (low): should we move to hot ?
				atomic.AddInt64(&c.sizeCold, diffSize)
			}

			c.evict()
		}
	default:
		panic("unsupported operation")
	}

	return true
}

func (c *clockPro) Evict(node *kv) {
	if node.log == nil {
		return
	}
	clockType := (*log)(atomic.LoadPointer(&node.log)).clockType
	switch clockType {
	case hot:
		atomic.AddInt64(&c.sizeHot, -node.size)
	case cold:
		atomic.AddInt64(&c.sizeCold, -node.size)
	case test:
		atomic.AddInt64(&c.sizeTest, -node.size)
	default:
		panic("unsupported clock type")
	}
	c.logDel(node)
}

func (c *clockPro) SetCapacity(capacity int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.maxSize = capacity
	c.evict()
}

// logAdd adds new log after the handHot
// and evicts data if needed
func (c *clockPro) logAdd(node *kv, ct clockType) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.evict()
	l := &log{n: node, clockType: ct}
	l.next = l
	l.prev = l
	if c.handHot == nil {
		c.handHot = l
		c.handCold = l
		c.handTest = l
	} else {
		c.handHot.insert(l)
	}

	if c.handCold == c.handHot {
		c.handCold = c.handCold.prev
	}

	node.log = unsafe.Pointer(l)

	return true
}

func (c *clockPro) logDel(node *kv) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	l := (*log)(node.log)
	node.log = nil

	switch l {
	case c.handHot:
		c.handHot = c.handHot.prev
	case c.handCold:
		c.handCold = c.handCold.prev
	case c.handTest:
		c.handTest = c.handTest.prev
	}

	l.remove()
	if c.handHot.next == nil || c.handHot.prev == nil {
		c.handHot = nil
		c.handCold = nil
		c.handTest = nil
	}

	return true
}

func (c *clockPro) evict() {
	for c.handCold != nil && c.sizeHot+c.sizeCold > c.maxSize {
		c.runHandCold()
	}
}

func (c *clockPro) runHandCold() {
	if c.handCold.clockType == cold {
		n := c.handCold.n
		ref := n.usedBit.Load()
		c.sizeCold -= n.size

		if ref {
			// move to hot
			n.usedBit.Store(false)
			c.handCold.clockType = hot
			c.sizeHot += n.size
		} else {
			// move to test and de-allocate the node's value
			// but still keep its meta
			//
			// Hacky: Keep the current node's size, so when the test node
			// get removed, we can still calculate the size correctly
			n.SetValue(nil, n.size)
			c.handCold.clockType = test
			c.sizeTest += n.size
			for c.handTest != nil && c.sizeTest > c.maxSize {
				c.runHandTest()
			}
		}
	}

	c.handCold = c.handCold.next

	for c.handHot != nil && c.maxCold+c.sizeHot > c.maxSize {
		c.runHandHot()
	}
}

func (c *clockPro) runHandHot() {
	if c.handHot == c.handTest && c.handTest != nil {
		c.runHandTest()
		if c.handHot == nil {
			return
		}
	}

	n := c.handHot.n
	if c.handHot.clockType == hot {
		ref := n.usedBit.Load()
		if ref {
			n.usedBit.Store(false)
		} else {
			// move to cold
			c.handHot.clockType = cold
			c.sizeHot -= n.size
			c.sizeCold += n.size
		}
	}

	c.handHot = c.handHot.next
}

func (c *clockPro) runHandTest() {
	if c.handTest == c.handCold && c.handCold != nil {
		c.runHandCold()
		if c.handTest == nil {
			return
		}
	}

	n := c.handTest.n
	if c.handTest.clockType == test {
		// remove the node entirely
		c.maxCold -= n.size
		c.maxCold = max(c.maxCold, 0)

		c.mu.Unlock()
		n.s.evict(n)
		c.mu.Lock()
	}

	c.handTest = c.handTest.next
}

func NewClockPro(maxSize int64) *clockPro {
	c := &clockPro{}
	c.SetCapacity(maxSize)
	return c
}

var _ ICacher = (*clockPro)(nil)
