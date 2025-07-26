package row_block

import (
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/block"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/compression"
)

func compressToPb(
	compressor compression.ICompression,
	checksumer common.IChecksum,
	rawData []byte,
) *block.PhysicalBlock {
	pb := &block.PhysicalBlock{}
	compressed := compressor.Compress(nil, rawData)
	checksum := checksumer.Checksum(compressed, byte(compressor.GetType()))
	pb.SetData(compressed)
	pb.SetTrailer(byte(compressor.GetType()), checksum)
	return pb
}
