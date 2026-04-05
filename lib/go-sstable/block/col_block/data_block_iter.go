package colblock

import (
	"fmt"

	"github.com/datnguyenzzz/nogodb/lib/go-bytesbufferpool/predictable_size"
	layoutcodex "github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block/codex/layout_codex"
	prefixbytescodex "github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block/codex/prefix_bytes_codex"
	rawbytescodex "github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block/codex/raw_bytes_codex"
	uintcodex "github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block/codex/uint_codex"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
)

// Note: All received keys are the full user key, eg internalKey.UserKey

type DataBlockIter struct {
	comparer common.IComparer

	keyDecoder struct {
		prefix  *prefixbytescodex.PrefixBytesDecoder
		suffix  *rawbytescodex.RawBytesDecoder
		trailer *uintcodex.UintDecoder[uint64]
	}

	prefixChangedAt *uintcodex.UintDecoder[uint32]
	values          *rawbytescodex.RawBytesDecoder

	currRow uint32 // used for iterating over the block
	closed  bool
}

func (i *DataBlockIter) SeekGTE(key []byte) *common.InternalKV {
	return nil
}

// SeekPrefixGTE moves the iterator to the first key/value pair whose key >= to the given key.
// that has the defined prefix for faster looking up
func (i *DataBlockIter) SeekPrefixGTE(prefix, key []byte) *common.InternalKV {
	return nil
}

// SeekLTE moves the iterator to the last key/value pair whose key ≤ to the given key.
func (i *DataBlockIter) SeekLTE(key []byte) *common.InternalKV {
	return nil
}

// First moves the iterator the first key/value pair.
func (i *DataBlockIter) First() *common.InternalKV {
	return nil
}

// Last moves the iterator the last key/value pair.
func (i *DataBlockIter) Last() *common.InternalKV {
	return nil
}

// Next moves the iterator to the next key/value pair
func (i *DataBlockIter) Next() *common.InternalKV {
	return nil
}

// Prev moves the iterator to the previous key/value pair.
func (i *DataBlockIter) Prev() *common.InternalKV {
	return nil
}

// Close closes the iterator and returns any accumulated error. Exhausting
// all the key/value pairs in a table is not considered to be an error.
func (i *DataBlockIter) Close() error {
	return nil
}

func (i *DataBlockIter) IsClosed() bool {
	return i.closed
}

func NewDataBlockIter(
	bp *predictable_size.PredictablePool,
	cp common.IComparer,
	data *common.InternalLazyValue,
) *DataBlockIter {
	d := &DataBlockIter{
		comparer: cp,
		keyDecoder: struct {
			prefix  *prefixbytescodex.PrefixBytesDecoder
			suffix  *rawbytescodex.RawBytesDecoder
			trailer *uintcodex.UintDecoder[uint64]
		}{},
	}
	// Refer to the README and data_block_writer to understand
	// the layout of the data block
	decoder := layoutcodex.NewLayoutDecoder(data.Value())
	var ok bool

	for i, columnName := range columnsOrder {
		switch columnName {
		case "prefix":
			d.keyDecoder.prefix, ok = layoutcodex.Decode(
				cp,
				decoder,
				uint16(i),
				prefixbytescodex.NewPrefixBytesDecoder,
			).(*prefixbytescodex.PrefixBytesDecoder)
			if !ok {
				panic("NewDataBlockIter, failed to assert to PrefixBytesDecoder")
			}
		case "suffix":
			d.keyDecoder.suffix, ok = layoutcodex.Decode(
				cp,
				decoder,
				uint16(i),
				rawbytescodex.NewRawBytesDecoder,
			).(*rawbytescodex.RawBytesDecoder)
			if !ok {
				panic("NewDataBlockIter, failed to assert to RawBytesDecoder")
			}
		case "trailer":
			d.keyDecoder.trailer, ok = layoutcodex.Decode(
				cp,
				decoder,
				uint16(i),
				uintcodex.NewUintDecoder[uint64],
			).(*uintcodex.UintDecoder[uint64])
			if !ok {
				panic("NewDataBlockIter, failed to assert to UintDecoder")
			}
		case "values":
			d.values, ok = layoutcodex.Decode(
				cp,
				decoder,
				uint16(i),
				rawbytescodex.NewRawBytesDecoder,
			).(*rawbytescodex.RawBytesDecoder)
			if !ok {
				panic("NewDataBlockIter, failed to assert to RawBytesDecoder")
			}
		case "prefixChangedAt":
			d.prefixChangedAt, ok = layoutcodex.Decode(
				cp,
				decoder,
				uint16(i),
				uintcodex.NewUintDecoder[uint32],
			).(*uintcodex.UintDecoder[uint32])
			if !ok {
				panic("NewDataBlockIter, failed to assert to UintDecoder")
			}
		default:
			panic(fmt.Sprintf("Unhandled column: %s", columnName))
		}
	}

	return d
}

var _ common.InternalIterator = (*DataBlockIter)(nil)
