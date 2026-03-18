package prefixbytescodex

import (
	"slices"

	"github.com/datnguyenzzz/nogodb/lib/go-bytesbufferpool/predictable_size"
	colblock "github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block"
	rawbytescodex "github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block/raw_bytes_codex"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
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

func NewPrefixBytesDecoder(
	rows, offset uint32,
	data *common.InternalLazyValue,
	bp *predictable_size.PredictablePool,
) (*PrefixBytesDecoder, uint32) {
	dec := &PrefixBytesDecoder{rows: rows}

	dec.bundleSize = data.Value()[offset]

	rawBytes := common.NewBlankInternalLazyValue(common.ValueFromBuffer)
	rawBytes.ReserveBuffer(bp, len(data.Value())-1)
	rawBytes.SetBufferValue(data.Value()[1:])

	rawSize := GetPosFromRow(rows-1, dec.bundleSize) + 1

	dec.rawBytesDec, offset = rawbytescodex.NewRawBytesDecoder(
		rawSize, offset, &rawBytes,
	)

	offset += 1 // skip 1 byte for the bundle size

	return dec, offset
}

var _ colblock.IColumnDecoder[[]byte] = (*PrefixBytesDecoder)(nil)
