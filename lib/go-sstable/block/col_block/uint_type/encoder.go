package uinttype

import (
	"encoding/binary"
	"math/bits"

	colblock "github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block"
)

type UintEncoder[T colblock.UintType] struct {
	values []T
	rows   uint32
}

func (e *UintEncoder[T]) Init() {
	e.Reset()
}

// Reset reuses the existing encoder with its already allocated memory
func (e *UintEncoder[T]) Reset() {
	clear(e.values)
}

func (e *UintEncoder[T]) Append(v T) {
	e.rows += 1
	if cap(e.values) <= int(e.rows) {
		newSize := max(32, 1<<cap(e.values))
		for newSize <= int(e.rows) {
			if newSize < 1024 {
				newSize = newSize << 1
			} else {
				newSize += newSize / 4
			}
		}

		values := make([]T, len(e.values), newSize)
		copy(values, e.values)
		e.values = values
	}

	e.values = append(e.values, v)
}

// Size returns the size of the column, if the its row were encoded starting from an [offset]
func (e *UintEncoder[T]) Size(offset uint32) uint32 {
	minV, maxV := e.findMinMaxValue()
	reqB := byteWidth(maxV - minV)

	return 1 + 8 + e.rows*uint32(reqB)
}

// Finish serialises the encoded column into a [buf] from [offset], return the offset after written
func (e *UintEncoder[T]) Finish(offset uint32, buf []byte) uint32 {
	minV, maxV := e.findMinMaxValue()
	reqB := byteWidth(maxV - minV)
	// start encoding and serialising to the [buf]
	// refer to the col_block/README.md for more detail about the layout
	buf[offset] = reqB // 1B
	offset += 1
	offset = serialise(buf, offset, uint64(minV)) // 8B
	for _, v := range e.values {
		//reqB per block, If reqB = 8, then the delta encoding won't be much beneficial
		offset = serialise(buf, offset, v-minV)
	}

	return offset
}

// serialise puts the v into buf[offset:]
func serialise[T colblock.UintType](buf []byte, offset uint32, v T) (nextOffset uint32) {
	switch any(v).(type) {
	case uint8:
		buf[offset] = byte(v)
		nextOffset = offset + 1
	case uint16:
		binary.LittleEndian.PutUint16(buf[offset:], uint16(v))
		nextOffset = offset + 2
	case uint32:
		binary.LittleEndian.PutUint32(buf[offset:], uint32(v))
		nextOffset = offset + 4
	case uint64:
		binary.LittleEndian.PutUint64(buf[offset:], uint64(v))
		nextOffset = offset + 8
	default:
		panic("UintEncoder tries serialise un-supported type")
	}

	return nextOffset
}

func (e *UintEncoder[T]) findMinMaxValue() (T, T) {
	maxV, minV := T(0), ^T(0)
	for _, v := range e.values {
		maxV = max(maxV, v)
		minV = min(minV, v)
	}

	return minV, maxV
}

// byteWidth returns maximum needed bytes to present a num
// only returns [0, 1, 2, 4, 8]
func byteWidth[T colblock.UintType](num T) byte {
	bitWidth := bits.Len64(uint64(num))

	byteWidthTable := [65]uint8{
		// 0 bits => 0 bytes
		0,
		// 1..8 bits => 1 byte
		1, 1, 1, 1, 1, 1, 1, 1,
		// 9..16 bits => 2 bytes
		2, 2, 2, 2, 2, 2, 2, 2,
		// 17..32 bits => 4 bytes
		4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
		// 33..64 bits => 8 bytes
		8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
		8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
	}

	return byteWidthTable[bitWidth]
}

// assert UintEncoder implements the IColumnEncoder interface
var _ colblock.IColumnEncoder[uint64] = (*UintEncoder[uint64])(nil)
