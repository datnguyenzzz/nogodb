package row_block

import (
	"encoding/binary"

	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
)

// DataBlockIterator is an iterator over a single row-based block of data.
type DataBlockIterator struct {
	// data represents entire data of the block
	data []byte
	// key represents key of the current entry
	key []byte
	// value represents value of the current entry
	value []byte
	// offsets
	offset        uint64
	nextOffset    uint64
	trailerOffset uint64
	// auxiliary
	numRestarts int32
	cmp         common.IComparer
	// TODO(high): Need exploring how to cache the data
}

func (i *DataBlockIterator) SeekGTE(key []byte) *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (i *DataBlockIterator) SeekLT(key []byte) *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (i *DataBlockIterator) First() *common.InternalKV {
	i.readEntry()
	iKV := &common.InternalKV{}
	iKV.K = *common.DeserializeKey(i.key)
	iKV.SetValue(i.value)
	return iKV
}

func (i *DataBlockIterator) Last() *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (i *DataBlockIterator) Next() *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (i *DataBlockIterator) Prev() *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (i *DataBlockIterator) Close() error {
	//TODO implement me
	panic("implement me")
}

// readEntry read key, value and nextOffset of the current entry where the iterator points at
func (i *DataBlockIterator) readEntry() {
	blkOffset := i.offset
	sharedLen, e := binary.Uvarint(i.data[blkOffset:])
	blkOffset += uint64(e)
	unsharedLen, e := binary.Uvarint(i.data[blkOffset:])
	blkOffset += uint64(e)
	valueLen, e := binary.Uvarint(i.data[blkOffset:])
	blkOffset += uint64(e)
	if len(i.key) == 0 {
		// the very first of the block
		i.key = i.data[blkOffset : blkOffset+unsharedLen]
	} else {
		i.key = append(i.key[:sharedLen], i.data[blkOffset:blkOffset+unsharedLen]...)
	}
	i.key = i.key[:len(i.key):len(i.key)]
	blkOffset += unsharedLen
	i.value = i.data[blkOffset : blkOffset+valueLen]
	i.value = i.value[:len(i.value):len(i.value)]
	blkOffset += valueLen
	i.nextOffset = blkOffset
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
		offset:        0,
	}
	return i
}

var _ common.InternalIterator = (*DataBlockIterator)(nil)
