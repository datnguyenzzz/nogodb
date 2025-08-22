package row_block

import (
	"encoding/binary"

	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
)

// DataBlockIterator is an iterator over a single row-based block of data.
type DataBlockIterator struct {
	data          []byte
	offset        uint64
	trailerOffset uint64
	numRestarts   int32
	cmp           common.IComparer
	kv            *common.InternalKV
}

func NewDataBlockIterator(
	cmp common.IComparer,
	block []byte,
) *DataBlockIterator {
	// refer to the README to understand the data layout
	numRestarts := int32(binary.LittleEndian.Uint32(block[len(block)-4:]))
	i := &DataBlockIterator{
		cmp:           cmp,
		data:          block,
		numRestarts:   numRestarts,
		trailerOffset: uint64(len(block)) - uint64(4*(1+numRestarts)),
	}
	return i
}
