package colblock

import (
	layoutcodex "github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block/codex/layout_codex"
	prefixbytescodex "github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block/codex/prefix_bytes_codex"
	rawbytescodex "github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block/codex/raw_bytes_codex"
	uintcodex "github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block/codex/uint_codex"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
)

const (
	dataBlockTotalColumns = 4
)

type DataBlockWriter struct {
	comparer common.IComparer

	keyEncoder struct {
		prefix prefixbytescodex.PrefixBytesEncoder
		suffix rawbytescodex.RawByteEncoder
	}

	columnEncoder struct {
		trailer uintcodex.UintEncoder[uint64]
		values  rawbytescodex.RawByteEncoder
	}

	layoutEncoder layoutcodex.LayoutEncoder

	currKey *common.InternalKey
	rows    uint32
}

func (d *DataBlockWriter) Reset() {
	d.keyEncoder.prefix.Reset()
	d.keyEncoder.suffix.Reset()

	d.columnEncoder.trailer.Reset()
	d.columnEncoder.values.Reset()

	d.layoutEncoder.Reset()
	d.rows = 0
	d.currKey = nil
}

// Add adds a key-value pair to the sstable.
func (d *DataBlockWriter) Add(key common.InternalKey, value []byte) {
	d.rows += 1
	d.currKey = &key

	prefixLen := d.comparer.Split(key.UserKey)
	d.keyEncoder.prefix.Append(key.UserKey[:prefixLen])
	d.keyEncoder.suffix.Append(key.UserKey[prefixLen:])

	d.columnEncoder.trailer.Append(uint64(key.Trailer))

	d.columnEncoder.values.Append(value)
}

func (d *DataBlockWriter) Rows() uint32 {
	return d.rows
}

func (d *DataBlockWriter) CurrKey() *common.InternalKey {
	return d.currKey
}

func (d *DataBlockWriter) Size() uint32 {
	// refer to the README to understand the layout
	offset := uint32(layoutcodex.HeaderOffset + layoutcodex.ColumnHeadSize*dataBlockTotalColumns)
	offset = d.keyEncoder.prefix.Size(offset)
	offset = d.keyEncoder.suffix.Size(offset)
	offset = d.columnEncoder.trailer.Size(offset)
	offset = d.columnEncoder.values.Size(offset)
	offset += 1 // 1 un-used padding byte

	return offset
}

// Finish the writing to the current page, and prepare data for flushing to the storage
//
// Caller of the function must keep track of the current accumlated size of the block
// or using  DataBlockWriter.Size() function to get the size before finishing
func (d *DataBlockWriter) Finish(rows uint32, size int) (finished []byte) {
	if rows < d.rows-1 {
		panic("DataBlockWriter only accepts to finish either all rows, or [all rows minus 1]")
	}

	h := layoutcodex.NewHeader(1, dataBlockTotalColumns, rows)

	d.layoutEncoder.Init(size, h)
	d.layoutEncoder.Encode(rows, &d.keyEncoder.prefix)
	d.layoutEncoder.Encode(rows, &d.keyEncoder.suffix)
	d.layoutEncoder.Encode(rows, &d.columnEncoder.trailer)
	d.layoutEncoder.Encode(rows, &d.columnEncoder.values)

	finished = d.layoutEncoder.Data()

	return finished
}

func NewDataBlockWriter(comparer common.IComparer) *DataBlockWriter {
	d := &DataBlockWriter{
		comparer: comparer,
	}

	d.keyEncoder = struct {
		prefix prefixbytescodex.PrefixBytesEncoder
		suffix rawbytescodex.RawByteEncoder
	}{
		prefix: prefixbytescodex.PrefixBytesEncoder{},
		suffix: rawbytescodex.RawByteEncoder{},
	}
	d.keyEncoder.prefix.Init()
	d.keyEncoder.suffix.Init()

	d.columnEncoder = struct {
		trailer uintcodex.UintEncoder[uint64]
		values  rawbytescodex.RawByteEncoder
	}{
		trailer: uintcodex.UintEncoder[uint64]{},
		values:  rawbytescodex.RawByteEncoder{},
	}
	d.columnEncoder.trailer.Init()
	d.columnEncoder.values.Init()

	d.layoutEncoder = layoutcodex.LayoutEncoder{}
	// we will init the layoutEncoder with the estimated size when the finish the block
	d.layoutEncoder.Reset()

	return d
}
