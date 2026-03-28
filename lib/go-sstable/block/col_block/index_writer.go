package colblock

import (
	layoutcodex "github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block/codex/layout_codex"
	rawbytescodex "github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block/codex/raw_bytes_codex"
	uintcodex "github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block/codex/uint_codex"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common/block"
)

const (
	indexTotalColumns = 3
)

type IndexBlockWriter struct {
	// index key is the raw byte separator of 2 internal keys
	// prev_key ≤ index_key < current_key
	keyEncoder rawbytescodex.RawByteEncoder

	blockHandleEncoder struct {
		offset uintcodex.UintEncoder[uint64]
		length uintcodex.UintEncoder[uint64]
	}

	layoutEncoder layoutcodex.LayoutEncoder
	rows          uint32
}

func (i *IndexBlockWriter) Reset() {
	i.keyEncoder.Reset()

	i.blockHandleEncoder.offset.Reset()
	i.blockHandleEncoder.length.Reset()

	i.rows = 0
}

func (i *IndexBlockWriter) Init() {
	i.keyEncoder.Init()

	i.blockHandleEncoder.offset.Init()
	i.blockHandleEncoder.length.Init()

	i.layoutEncoder.Reset()

	i.rows = 0
}

func (i *IndexBlockWriter) AddKey(key []byte, handle block.BlockHandle) {
	i.rows += 1

	i.keyEncoder.Append(key)

	i.blockHandleEncoder.offset.Append(handle.Offset)
	i.blockHandleEncoder.length.Append(handle.Length)
}

func (i *IndexBlockWriter) Size() uint32 {
	offset := uint32(layoutcodex.HeaderOffset + layoutcodex.ColumnHeadSize*indexTotalColumns)
	offset = i.keyEncoder.Size(offset)
	offset = i.blockHandleEncoder.offset.Size(offset)
	offset = i.blockHandleEncoder.length.Size(offset)
	offset += 1

	return offset
}

// Finish the writing to the current page, and prepare data for flushing to the storage
//
// Caller of the function must keep track of the current accumlated size of the block
// or using  DataBlockWriter.Size() function to get the size before finishing
func (i *IndexBlockWriter) Finish(size int) []byte {
	header := layoutcodex.NewHeader(1, indexTotalColumns, i.rows)

	i.layoutEncoder.Init(size, header)

	i.layoutEncoder.Encode(i.rows, &i.keyEncoder)
	i.layoutEncoder.Encode(i.rows, &i.blockHandleEncoder.offset)
	i.layoutEncoder.Encode(i.rows, &i.blockHandleEncoder.length)

	return i.layoutEncoder.Data()
}
