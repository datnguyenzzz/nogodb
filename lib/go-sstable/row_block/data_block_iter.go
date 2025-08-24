package row_block

import (
	"encoding/binary"

	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
)

// DataBlockIterator is an iterator over a single row-based block of data.
type DataBlockIterator struct {
	data []byte
	// offset
	offset        uint64
	nextOffset    uint64
	trailerOffset uint64
	// auxiliary
	numRestarts int32
	cmp         common.IComparer
	// TODO(high): Need exploring how to cache the data
}

func (d DataBlockIterator) SeekGTE(key []byte) *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (d DataBlockIterator) SeekLT(key []byte) *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (d DataBlockIterator) First() *common.InternalKV {
	//TODO
	//  read current entry
	//  prepare nextOffset
	//  return iKV
	panic("implement me")
}

func (d DataBlockIterator) Last() *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (d DataBlockIterator) Next() *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (d DataBlockIterator) Prev() *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (d DataBlockIterator) Close() error {
	//TODO implement me
	panic("implement me")
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
