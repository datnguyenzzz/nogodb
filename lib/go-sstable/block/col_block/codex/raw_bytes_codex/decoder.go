package rawbytescodex

import (
	"fmt"

	"github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block/codex"
	uintcodex "github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block/codex/uint_codex"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
)

type RawBytesDecoder struct {
	rows       uint32
	offsetsDec *uintcodex.UintDecoder[uint32]
	data       []byte
}

func (u *RawBytesDecoder) Get(row uint32) []byte {
	if row >= u.rows {
		panic("outside of column block RawBytesDecoder")
	}

	start := uint32(0)
	if row > 0 {
		start = u.offsetsDec.Get(row - 1)
	}

	end := u.offsetsDec.Get(row)

	return u.data[start:end]
}

func (u *RawBytesDecoder) Slice(from, to uint32) []byte {
	if from >= u.rows || to >= u.rows {
		panic(fmt.Sprintf("Slice [%d-%d] outside of column block RawBytesDecoder, %d", from, to, u.rows))
	}

	start := uint32(0)
	if from > 0 {
		start = u.offsetsDec.Get(from - 1)
	}

	end := u.offsetsDec.Get(to)

	return u.data[start:end]
}

func (e *RawBytesDecoder) DataType() codex.DataType {
	return codex.RawBytesDT
}

func (e *RawBytesDecoder) SeekGTE(key []byte) (rowIndex uint32, isEqual bool) {
	panic("RawBytesDecoder can not support SeekGTE")
}

func (e *RawBytesDecoder) Rows() uint32 {
	return e.rows
}

// NewRawBytesDecoder returns a RawBytesDecoder with the offset of the next block
func NewRawBytesDecoder(
	comparer common.IComparer, rows, offset uint32, data []byte,
) (codex.IColumnDecoder[[]byte], uint32) {
	iDec, offset := uintcodex.NewUintDecoder[uint32](comparer, rows, offset, data)
	dec, ok := iDec.(*uintcodex.UintDecoder[uint32])
	if !ok {
		panic("NewRawBytesDecoder failed to assert to uintcodex.UintDecoder[uint32]")
	}

	valuesLen := dec.Get(rows - 1)
	return &RawBytesDecoder{
		rows:       rows,
		offsetsDec: dec,
		data:       data[offset:],
	}, offset + uint32(valuesLen)
}

var _ codex.IColumnDecoder[[]byte] = (*RawBytesDecoder)(nil)
