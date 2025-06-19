package go_bytesbufferpool

import (
	"math/bits"
	"sync"
)

const (
	maximumPoolCnt = 24
)

// pools contains pools for slices of byte of various capacities.
//
//	pools[0] is for capacities from 0 upto 256
//	pools[1] is for capacities from 257 upto 512
//	pools[2] is for capacities from 513 upto 1024
//	...
//	pools[n] is for capacities from 2^(n+7)+1 to 2^(n+8)
//
// Limit the maximum capacity to 2^24, since there are no performance benefits
// in caching byte slices with bigger capacities.
var pools [maximumPoolCnt]sync.Pool

func Get(dataLen int) []byte {
	id, poolCap := getPoolIDAndCapacity(dataLen)
	if b := pools[id].Get(); b != nil {
		return b.([]byte)
	}

	// if the pool is empty, then allocate new poolCap bytes
	return make([]byte, 0, poolCap)
}

func Put(buf []byte) {
	capacity := cap(buf)
	id, poolCap := getPoolIDAndCapacity(capacity)
	if capacity > poolCap {
		// there is no available pool that can handle this size
		return
	}

	//reset the buffer and remains the capacity, and put into the pool
	buf = buf[:0]
	pools[id].Put(buf)
}

// getPoolIDAndCapacity predict the poolId from given data size
// and return the pool maximum capacity
func getPoolIDAndCapacity(size int) (int, int) {
	size--
	size = max(size, 0)
	size >>= 8
	id := bits.Len(uint(size))
	id = min(id, maximumPoolCnt-1)
	return id, 1 << (id + 8)
}
