package row_block

import (
	"encoding/binary"

	"github.com/datnguyenzzz/nogodb/lib/go-bytesbufferpool/predictable_size"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	"go.uber.org/zap"
)

// BlockIterator is an iterator over a single row-based block.
// BlockIterator will still return even if the record has tombstone mark
type BlockIterator struct {
	bpool *predictable_size.PredictablePool
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
	// restarts
	numRestarts   int32
	restartPoints []int32
	// auxiliary
	cmp common.IComparer
	// TODO(high): Need exploring how to cache the data
}

func (i *BlockIterator) SeekGTE(key []byte) *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (i *BlockIterator) SeekLT(key []byte) *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (i *BlockIterator) First() *common.InternalKV {
	i.readEntry()
	return i.toKV()
}

func (i *BlockIterator) Last() *common.InternalKV {
	// move offset to the last restart point
	i.offset = uint64(i.restartPoints[len(i.restartPoints)-1])
	i.readEntry()
	for i.nextOffset != i.trailerOffset {
		i.offset = i.nextOffset
		i.readEntry()
	}
	return i.toKV()
}

func (i *BlockIterator) Next() *common.InternalKV {
	i.offset = i.nextOffset
	i.readEntry()
	iKV := &common.InternalKV{}
	iKV.K = *common.DeserializeKey(i.key)
	v := common.NewBlankInternalLazyValue(common.ValueFromBuffer)
	v.ReserveBuffer(i.bpool, len(i.value))
	if err := v.SetBufferValue(i.value); err != nil {
		zap.L().Error("failed to set value", zap.Error(err))
	}
	iKV.V = v
	return iKV
}

func (i *BlockIterator) Prev() *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (i *BlockIterator) Close() error {
	i.key = i.key[:0]
	i.value = i.value[:0]
	i.bpool.Put(i.data)
	return nil
}

func (i *BlockIterator) toKV() *common.InternalKV {
	iKV := &common.InternalKV{}
	iKV.K = *common.DeserializeKey(i.key)
	v := common.NewBlankInternalLazyValue(common.ValueFromBuffer)
	v.ReserveBuffer(i.bpool, len(i.value))
	if err := v.SetBufferValue(i.value); err != nil {
		zap.L().Error("failed to set value", zap.Error(err))
	}
	iKV.V = v
	return iKV
}

// readEntry read key, value and nextOffset of the current entry where the iterator points at
func (i *BlockIterator) readEntry() {
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

func NewBlockIterator(
	bpool *predictable_size.PredictablePool,
	cmp common.IComparer,
	block []byte,
) *BlockIterator {
	// refer to the README to understand the data layout
	numRestarts := int32(binary.LittleEndian.Uint32(block[len(block)-4:]))
	trailerOffset := uint64(len(block)) - uint64(4*numRestarts) - 4
	restartPoints := make([]int32, numRestarts)
	for i := 0; i < int(numRestarts); i++ {
		restartPoints[i] = int32(binary.LittleEndian.Uint32(block[trailerOffset+uint64(4*i):]))
	}

	i := &BlockIterator{
		bpool:         bpool,
		cmp:           cmp,
		data:          block,
		numRestarts:   numRestarts,
		trailerOffset: trailerOffset,
		restartPoints: restartPoints,
		offset:        0,
	}
	return i
}

var _ common.InternalIterator = (*BlockIterator)(nil)
