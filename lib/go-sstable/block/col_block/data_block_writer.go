package colblock

import (
	layoutcodex "github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block/codex/layout_codex"
	prefixbytescodex "github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block/codex/prefix_bytes_codex"
	rawbytescodex "github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block/codex/raw_bytes_codex"
	uintcodex "github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block/codex/uint_codex"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
)

const (
	totalColumns = 4
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

	prevKey []byte
	rows    uint32
}

func (d *DataBlockWriter) Reset() {
	d.keyEncoder.prefix.Reset()
	d.keyEncoder.suffix.Reset()

	d.columnEncoder.trailer.Reset()
	d.columnEncoder.values.Reset()

	d.layoutEncoder.Reset()
	d.rows = 0
	d.prevKey = nil
}

// Add adds a key-value pair to the sstable.
func (d *DataBlockWriter) Add(key common.InternalKey, value []byte) {
	d.rows += 1

	prefixLen := d.comparer.Split(key.UserKey)
	d.keyEncoder.prefix.Append(key.UserKey[:prefixLen])
	d.keyEncoder.suffix.Append(key.UserKey[prefixLen:])

	d.columnEncoder.trailer.Append(uint64(key.Trailer))

	d.columnEncoder.values.Append(value)
}

func (d *DataBlockWriter) Size() uint32 {
	// refer to the README to understand the layout
	// header
	offset := uint32(layoutcodex.HeaderOffset + layoutcodex.ColumnHeadSize*totalColumns)
	offset = d.keyEncoder.prefix.Size(offset)
	offset = d.keyEncoder.suffix.Size(offset)
	offset = d.columnEncoder.trailer.Size(offset)
	offset = d.columnEncoder.values.Size(offset)
	offset += 1 // 1 un-used padding byte

	return offset
}

// Finish the writing to the current page, and prepare data for flushing to the storage
// Caller of the function must keep track of the current accumlated size of the block
func (d *DataBlockWriter) Finish(size int) (finished []byte) {
	h := layoutcodex.NewHeader(1, totalColumns, d.rows)

	d.layoutEncoder.Init(size, h)
	d.layoutEncoder.Encode(d.rows, &d.keyEncoder.prefix)
	d.layoutEncoder.Encode(d.rows, &d.keyEncoder.suffix)
	d.layoutEncoder.Encode(d.rows, &d.columnEncoder.trailer)
	d.layoutEncoder.Encode(d.rows, &d.columnEncoder.values)

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
