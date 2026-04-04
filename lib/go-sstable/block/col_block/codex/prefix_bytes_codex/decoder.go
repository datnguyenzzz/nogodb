package prefixbytescodex

import (
	"slices"

	"github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block/codex"
	rawbytescodex "github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block/codex/raw_bytes_codex"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
)

type PrefixBytesDecoder struct {
	comparer   common.IComparer
	rows       uint32
	bundleSize byte

	rawBytesDec *rawbytescodex.RawBytesDecoder
}

func (u *PrefixBytesDecoder) Get(row uint32) []byte {
	bundlePrefixPos := GetBundlePrefixPos(row, u.bundleSize)
	bundlePrefix := u.rawBytesDec.Get(bundlePrefixPos)
	suffix := u.rawBytesDec.Get(GetOffsetFromRow(row, u.bundleSize))

	return slices.Concat(u.rawBytesDec.Get(0), bundlePrefix, suffix)
}

func (e *PrefixBytesDecoder) DataType() codex.DataType {
	return codex.PrefixCompressedBytesDT
}

// SeekGTE moves the iterator to the first key/value pair whose key ≥ to the given key.
// Only PrefixBytesDecoder can support this function because the keys are sorted
func (u *PrefixBytesDecoder) SeekGTE(key []byte) (rowIndex uint32, isEqual bool) {
	blockPrefix := u.rawBytesDec.Get(0)

	if len(key) < len(blockPrefix) {
		return 0, false
	}

	if len(blockPrefix) > 0 {
		switch u.comparer.Compare(key[:len(blockPrefix)], blockPrefix) {
		case 1:
			return u.rows, false
		case -1:
			return 0, false
		}
	}

	key = key[len(blockPrefix):]

	// Binary search to find the right bundle
	lo, bundle, hi := uint32(0), uint32(0), GetBundleFromRow(u.rows-1, u.bundleSize)
	for lo <= hi {
		mid := (lo + hi) >> 1
		offset := GetBundleStartOffset(mid, u.bundleSize)
		firstKey := u.rawBytesDec.Slice(offset, offset+1)
		cp := u.comparer.Compare(firstKey, key)
		if cp <= 0 {
			bundle = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	bundlePrefix := u.rawBytesDec.Get(GetBundleStartOffset(bundle, u.bundleSize))

	if len(key) < len(bundlePrefix) || u.comparer.Compare(key[:len(bundlePrefix)], bundlePrefix) != 0 {
		// the founded is the first key of next bundle
		if bundle == GetBundleFromRow(u.rows-1, u.bundleSize) {
			// key is greater than all keys in the block
			return u.rows, false
		}

		offset := GetBundleStartOffset(bundle+1, u.bundleSize)
		firstKey := u.rawBytesDec.Slice(offset, offset+1)

		return GetRowFromOffset(offset+1, u.bundleSize), u.comparer.Compare(key, firstKey) == 0
	}

	// Binary search to find the index on the bundle
	key = key[len(bundlePrefix):]
	lo = GetBundleStartOffset(bundle, u.bundleSize) + 1
	hi = min(GetBundleStartOffset(bundle+1, u.bundleSize)-1, GetOffsetFromRow(u.rows-1, u.bundleSize))
	// fmt.Println("range:", lo, hi)
	cpResult := 2
	for lo <= hi {
		mid := (lo + hi) >> 1
		cp := u.comparer.Compare(u.rawBytesDec.Get(mid), key)
		if cp >= 0 {
			rowIndex = GetRowFromOffset(mid, u.bundleSize)
			cpResult = cp
			hi = mid - 1
		} else {
			lo = mid + 1
		}
	}

	if cpResult == 2 {
		// the key must be either not found if it is the last bundle
		// or the first key of the next bundle
		if bundle == GetBundleFromRow(u.rows-1, u.bundleSize) {
			// key is greater than all keys in the block
			return u.rows, false
		}

		offset := GetBundleStartOffset(bundle+1, u.bundleSize)
		firstKey := u.rawBytesDec.Get(offset + 1)

		return GetRowFromOffset(offset+1, u.bundleSize), u.comparer.Compare(key, firstKey) == 0
	}

	return rowIndex, cpResult == 0
}

func (u *PrefixBytesDecoder) Slice(from, to uint32) []byte {
	panic("Not yet implemented")
}

func (u *PrefixBytesDecoder) Rows() uint32 {
	return u.rows
}

func NewPrefixBytesDecoder(
	comparer common.IComparer,
	rows, offset uint32,
	data []byte,
) (*PrefixBytesDecoder, uint32) {
	dec := &PrefixBytesDecoder{rows: rows, comparer: comparer}

	dec.bundleSize = data[offset]

	rawSize := GetOffsetFromRow(rows-1, dec.bundleSize) + 1

	dec.rawBytesDec, offset = rawbytescodex.NewRawBytesDecoder(
		rawSize, offset+1, data, // skip 1 byte for bundle size
	)

	return dec, offset
}

var _ codex.IColumnDecoder[[]byte] = (*PrefixBytesDecoder)(nil)
