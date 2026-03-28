package colblock

import (
	blockCommon "github.com/datnguyenzzz/nogodb/lib/go-sstable/block"
	layoutcodex "github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block/codex/layout_codex"
	rawbytescodex "github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block/codex/raw_bytes_codex"
	uintcodex "github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block/codex/uint_codex"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common/block"
)

const (
	indexTotalColumns = 3
)

// TODO(high): Support 2 layered index

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

func (i *IndexBlockWriter) Add(key *common.InternalKey, bh *block.BlockHandle) error {
	i.rows += 1

	// for index key, we only interested in the UserKey
	i.keyEncoder.Append(key.UserKey)

	i.blockHandleEncoder.offset.Append(bh.Offset)
	i.blockHandleEncoder.length.Append(bh.Length)
	return nil
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

func NewIndexBlockWriter() *IndexBlockWriter {
	i := &IndexBlockWriter{
		keyEncoder:    rawbytescodex.RawByteEncoder{},
		layoutEncoder: layoutcodex.LayoutEncoder{},
	}
	i.keyEncoder.Init()
	i.layoutEncoder.Reset()

	i.blockHandleEncoder = struct {
		offset uintcodex.UintEncoder[uint64]
		length uintcodex.UintEncoder[uint64]
	}{
		offset: uintcodex.UintEncoder[uint64]{},
		length: uintcodex.UintEncoder[uint64]{},
	}

	i.blockHandleEncoder.offset.Init()
	i.blockHandleEncoder.length.Init()

	return i
}

var _ blockCommon.IIndexWriter = (*IndexBlockWriter)(nil)
