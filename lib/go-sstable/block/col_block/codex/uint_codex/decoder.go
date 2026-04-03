package uintcodex

import (
	"encoding/binary"
	"unsafe"

	"github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block/codex"
)

type UintDecoder[T codex.UintType] struct {
	// ptr points to the offset, where the UintColumn's values encoded to after [width][baseValue]
	// [width][baseValue]|[values...]
	//.                  |<-- ptr
	ptr       unsafe.Pointer
	rows      uint32
	width     byte
	baseValue uint64
}

func (u *UintDecoder[T]) Get(row uint32) uint64 {
	if row >= u.rows {
		panic("outside of column block RawBytesDecoder")
	}

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

func (e *UintDecoder[T]) DataType() codex.DataType {
	return codex.UintDT
}

func (e *UintDecoder[T]) SeekGTE(key T) (rowIndex uint32, isEqual bool) {
	panic("UintDecoder can not support SeekGTE")
}

func (e *UintDecoder[T]) Slice(from, to uint32) T {
	panic("UintDecoder can not support Slice")
}

func (e *UintDecoder[T]) Rows() uint32 {
	return e.rows
}

// NewUintDecoder returns a UintDecoder with the offset of the next block
func NewUintDecoder[T codex.UintType](
	rows, offset uint32, buf []byte,
) (*UintDecoder[T], uint32) {
	// Refer to the col_block/README.md for more detail about the layout
	width := buf[offset]
	offset += 1
	baseValue := binary.LittleEndian.Uint64(buf[offset:])
	offset += 8

	return &UintDecoder[T]{
		ptr:       unsafe.Pointer(&buf[offset]),
		rows:      rows,
		width:     width,
		baseValue: baseValue,
	}, offset + rows*uint32(width)
}

var _ codex.IColumnDecoder[uint64] = (*UintDecoder[uint64])(nil)
