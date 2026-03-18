package rawbytescodex

import (
	colblock "github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block"
	uintcodex "github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block/uint_codex"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
)

type RawBytesDecoder struct {
	rows       uint32
	offsetsDec *uintcodex.UintDecoder[uint16]
	data       []byte
}

func (u *RawBytesDecoder) Get(row uint32) []byte {
	if row >= u.rows {
		panic("outside of column block RawBytesDecoder")
	}

	start := uint64(0)
	if row > 0 {
		start = u.offsetsDec.Get(row - 1)
	}

	end := u.offsetsDec.Get(row)

	return u.data[start:end]
}

// NewRawBytesDecoder returns a RawBytesDecoder with the offset of the next block
func NewRawBytesDecoder(
	rows, offset uint32, data *common.InternalLazyValue,
) (*RawBytesDecoder, uint32) {
	dec, offset := uintcodex.NewUintDecoder[uint16](rows, offset, data)
	valuesLen := dec.Get(rows - 1)
	return &RawBytesDecoder{
		rows:       rows,
		offsetsDec: dec,
		data:       data.Value()[offset:],
	}, offset + uint32(valuesLen)
}

var _ colblock.IColumnDecoder[[]byte] = (*RawBytesDecoder)(nil)
