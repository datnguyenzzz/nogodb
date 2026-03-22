package prefixbytescodex

import (
	"slices"

	"github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block/codex"
	rawbytescodex "github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block/codex/raw_bytes_codex"
)

type PrefixBytesDecoder struct {
	rows        uint32
	bundleSize  byte
	blockPrefix []byte

	rawBytesDec *rawbytescodex.RawBytesDecoder
}

func (u *PrefixBytesDecoder) Get(row uint32) []byte {
	if len(u.blockPrefix) == 0 {
		u.blockPrefix = u.rawBytesDec.Get(0)
	}

	bundlePrefixPos := GetBundlePrefixPos(row, u.bundleSize)
	bundlePrefix := u.rawBytesDec.Get(bundlePrefixPos)
	suffix := u.rawBytesDec.Get(GetPosFromRow(row, u.bundleSize))

	return slices.Concat(u.blockPrefix, bundlePrefix, suffix)
}

func (e *PrefixBytesDecoder) DataType() codex.DataType {
	return codex.PrefixCompressedBytesDT
}

func NewPrefixBytesDecoder(
	rows, offset uint32,
	data []byte,
) (*PrefixBytesDecoder, uint32) {
	dec := &PrefixBytesDecoder{rows: rows}

	dec.bundleSize = data[offset]

	rawSize := GetPosFromRow(rows-1, dec.bundleSize) + 1

	dec.rawBytesDec, offset = rawbytescodex.NewRawBytesDecoder(
		rawSize, offset, data[1:],
	)

	offset += 1 // skip 1 byte for the bundle size

	return dec, offset
}

var _ codex.IColumnDecoder[[]byte] = (*PrefixBytesDecoder)(nil)
