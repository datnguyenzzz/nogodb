package uinttype

import (
	"encoding/binary"
	"unsafe"

	colblock "github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
)

type UintDecoder[T colblock.UintType] struct {
	// ptr points to the offset, where the UintColumn's values encoded to after [width][baseValue]
	// [width][baseValue]|[values...]
	//.                  |<-- ptr
	ptr       unsafe.Pointer
	rows      uint32
	width     byte
	baseValue uint64
}

func (u *UintDecoder[T]) Get(row uint32) uint64 {
	switch u.width {
	case 0:
		return 0
	case 1:
		return u.baseValue + uint64(*(*uint8)(unsafe.Add(u.ptr, uintptr(row))))
	case 2:
		return u.baseValue + uint64(*(*uint16)(unsafe.Add(u.ptr, uintptr(row)<<1))) // 2 bytes per value
	case 4:
		return u.baseValue + uint64(*(*uint32)(unsafe.Add(u.ptr, uintptr(row)<<2))) // 4 bytes per value
	case 8:
		return u.baseValue + *(*uint64)(unsafe.Add(u.ptr, uintptr(row)<<3)) // 8 bytes per value
	default:
		panic("try decoding an UintDecoder but with a corrupted width")
	}
}

// NewUintDecoder returns a UintDecoder with the offset of the next block
func NewUintDecoder[T colblock.UintType](
	rows, offset uint32, data *common.InternalLazyValue,
) (*UintDecoder[T], uint32) {
	// Refer to the col_block/README.md for more detail about the layout
	buf := data.Value()
	width := buf[offset]
	offset += 1
	baseValue := binary.LittleEndian.Uint64(buf[offset:])
	offset += 8

	return &UintDecoder[T]{
		ptr:       unsafe.Pointer(&buf[offset]),
		rows:      rows,
		width:     buf[offset],
		baseValue: baseValue,
	}, offset + rows*uint32(width)
}

// assert UintEncoder implements the IColumnEncoder interface
var _ colblock.IColumnDecoder[uint64] = (*UintDecoder[uint64])(nil)
