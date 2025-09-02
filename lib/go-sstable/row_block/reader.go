package row_block

import (
	"encoding/binary"
	"fmt"

	"github.com/datnguyenzzz/nogodb/lib/go-bytesbufferpool/predictable_size"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	block_common "github.com/datnguyenzzz/nogodb/lib/go-sstable/common/block"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/compression"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/storage"
)

type IBlockReader interface {
	// Read perform read directly from the source without caching
	Read(bh *block_common.BlockHandle, kind block_common.BlockKind) (*common.InternalLazyValue, error)
	// ReadThroughCache perform read through cache method
	ReadThroughCache(bh *block_common.BlockHandle, kind block_common.BlockKind) (*common.InternalLazyValue, error)
	Init(bpool *predictable_size.PredictablePool, fr storage.ILayoutReader)
}

// RowBlockReader reads row-based blocks from a single file,
// handling block caching / read through cache, checksum validation
// and decompression.
type RowBlockReader struct {
	bpool         *predictable_size.PredictablePool
	storageReader storage.ILayoutReader
}

func (r *RowBlockReader) Init(bpool *predictable_size.PredictablePool, fr storage.ILayoutReader) {
	r.bpool = bpool
	r.storageReader = fr
}

func (r *RowBlockReader) ReadThroughCache(bh *block_common.BlockHandle, kind block_common.BlockKind) (*common.InternalLazyValue, error) {
	// TODO (high): The read function requires the buffer pool to be available to
	//  obtain the pre-allocated buffer for handling the read stream.
	//  An optimization is to have a caching mechanism to cache the value of
	//  the blockData , aka BlockCache (key: File ID + BlockHandle --> value: []byte)
	//  Research on how to implement an efficient Block's Cache
	//  ...
	//  Wire up with the go-cache
	panic("implement me")
}

func (r *RowBlockReader) Read(
	bh *block_common.BlockHandle,
	kind block_common.BlockKind,
) (*common.InternalLazyValue, error) {
	if r.bpool == nil {
		return nil, fmt.Errorf("blockData pool is nil")
	}

	compressedVal := &common.InternalLazyValue{}
	compressedVal.ReserveBuffer(r.bpool, int(bh.Length))
	if err := r.storageReader.ReadAt(compressedVal.Value(), bh.Offset); err != nil {
		compressedVal.Release()
		return nil, err
	}

	// Assume we would use CRC32 checksum method for every operation
	if !r.validateChecksum(common.CRC32Checksum, compressedVal.Value()) {
		compressedVal.Release()
		return nil, common.MismatchedChecksumError
	}

	// decompress block's data
	compressor, compressedLength := r.getCompressor(bh, compressedVal)
	compressedBytes := compressedVal.Value()[:compressedLength]
	decompressedLen, err := compressor.DecompressedLen(compressedBytes)
	if err != nil {
		compressedVal.Release()
		return nil, err
	}

	decompressedVal := &common.InternalLazyValue{}
	decompressedVal.ReserveBuffer(r.bpool, decompressedLen)

	err = compressor.Decompress(decompressedVal.Value(), compressedBytes)
	compressedVal.Release()

	if err != nil {
		decompressedVal.Release()
		return nil, err
	}

	return decompressedVal, nil
}

func (r *RowBlockReader) validateChecksum(checksumType common.ChecksumType, blockData []byte) bool {
	blockLengthWithoutTrailer := len(blockData) - block_common.TrailerLen
	foundChecksum := binary.LittleEndian.Uint32(blockData[blockLengthWithoutTrailer+1:])

	compressor := blockData[blockLengthWithoutTrailer]
	checksumer := common.NewChecksumer(checksumType)

	switch checksumType {
	case common.CRC32Checksum:
		expected := checksumer.Checksum(blockData[:blockLengthWithoutTrailer], compressor)
		if expected != foundChecksum {
			return false
		}
	default:
		return false
	}

	return true
}

// getCompressor return the compressor from the compressed block, and actual length
// of the compressed block. In the compressed data block, we store additional
// 5 bytes: 1-byte: [Compressor Type] + 4-bytes: [CRC checksum]
// Reference: lib/go-sstable/row_block/common.go compressToPb()
func (r *RowBlockReader) getCompressor(
	bh *block_common.BlockHandle,
	compressedVal *common.InternalLazyValue,
) (compressor compression.ICompression, compressedLength int) {
	compressedLength = int(bh.Length - block_common.TrailerLen)
	compressor = compression.NewCompressor(
		compression.CompressionType(compressedVal.Value()[compressedLength]),
	)

	return compressor, compressedLength
}

var _ IBlockReader = (*RowBlockReader)(nil)
