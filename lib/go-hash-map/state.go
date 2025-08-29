package go_hash_map

import "sort"

const (
	overflowThreshold     = 1 << 5
	overflowGrowThreshold = 1 << 7
)

// state list of bucket, represent the current state of the hashmap
type state struct {
	buckets    []*bucket
	bucketSize int32

	prevState *state

	// resizing True if any bucket is changing its size
	resizing int32

	// overflow number of buckets that have size larger than overflowThreshold
	overflow        int32
	growThreshold   int64
	shrinkThreshold int64
}

func (s *state) initBucket(id int32) *bucket {
	bucket := s.buckets[id]

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	if bucket.state >= initialized {
		return bucket
	}

	prevState := s.prevState
	if prevState == nil {
		return bucket
	}

	if s.bucketSize > prevState.bucketSize {
		// grow
		nodes := prevState.initBucket(id % prevState.bucketSize).Freeze()
		for _, node := range nodes {
			if node.key%uint64(s.bucketSize) == uint64(id) {
				bucket.nodes = append(bucket.nodes, node)
			}
		}
	} else {
		// shrink
		nodes0 := prevState.initBucket(id).Freeze()
		nodes1 := prevState.initBucket(id + s.bucketSize).Freeze()

		bucket.nodes = make([]*kv, 0, len(nodes0)+len(nodes1))
		bucket.nodes = append(bucket.nodes, nodes0...)
		bucket.nodes = append(bucket.nodes, nodes1...)
		sort.Slice(bucket.nodes, func(i, j int) bool {
			return bucket.nodes[i].key < bucket.nodes[j].key
		})
	}

	bucket.state = initialized

	return bucket
}

func (s *state) initBuckets() {
	for i, _ := range s.buckets {
		s.initBucket(int32(i))
	}

	s.prevState = nil
}
