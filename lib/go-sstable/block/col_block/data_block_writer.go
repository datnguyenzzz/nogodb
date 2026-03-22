package colblock

import (
	prefixbytescodex "github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block/codex/prefix_bytes_codex"
	rawbytescodex "github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block/codex/raw_bytes_codex"
	uintcodex "github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block/codex/uint_codex"
)

type DataBlockWriter struct {
	encoder struct {
		key     prefixbytescodex.PrefixBytesEncoder
		trailer uintcodex.UintEncoder[uint64]
		values  rawbytescodex.RawByteEncoder
	}

	prevKey []byte
	rows    uint32
}
