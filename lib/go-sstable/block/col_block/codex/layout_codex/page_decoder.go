package layoutcodex

import (
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block/codex"
)

type LayoutDecoder struct {
	data   []byte
	header *Header
}

func NewLayoutDecoder(data []byte) *LayoutDecoder {
	header, _ := DecodeHeader(0, data)
	return &LayoutDecoder{
		data:   data,
		header: header,
	}
}

func (d *LayoutDecoder) DataType(col uint16) codex.DataType {
	offset := headerOffset + columnHeadSize*col

	ptr := unsafe.Add(unsafe.Pointer(&d.data[0]), uintptr(offset))

	return codex.DataType(*(*byte)(ptr))
}

func (d *LayoutDecoder) PageOffset(col uint16) uint32 {
	offset := headerOffset + columnHeadSize*col + /*1B for data type*/ 1

	ptr := unsafe.Add(unsafe.Pointer(&d.data[0]), uintptr(offset))

	return binary.LittleEndian.Uint32(unsafe.Slice((*byte)(ptr), 4))
}

func Decode[T codex.EncodableDataType](
	d *LayoutDecoder,
	col uint16,
	instructor codex.DecoderInstructor[T],
) codex.IColumnDecoder[T] {
	if col > d.header.columns {
		panic(fmt.Sprintf("requested col: %d is greater than columns in header: %d", col, d.header.columns))
	}

	// find page start offset from the column header

	dt := d.DataType(col)

	dec, offset := instructor(d.header.rows, d.PageOffset(col), d.data)

	if dt != dec.DataType() {
		panic("data type is mismatched when decoding")
	}

	if offset != d.PageOffset(col+1) {
		panic("next offset doesn't match to the next column")
	}

	return dec
}
