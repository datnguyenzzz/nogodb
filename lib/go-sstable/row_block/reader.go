package row_block

import (
	"encoding/binary"
	"fmt"

	"github.com/datnguyenzzz/nogodb/lib/go-bytesbufferpool/predictable_size"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/block"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/compression"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/storage"
)

// RowBlockReader reads row-based blocks from a single file, handling caching,
// checksum validation and decompression.
type RowBlockReader struct {
	storageReader storage.ILayoutReader
}

func (r *RowBlockReader) Init(fr storage.ILayoutReader) {
	r.storageReader = fr
}

func (r *RowBlockReader) Read(
	bpool *predictable_size.PredictablePool,
	bh *block.BlockHandle,
	kind block.BlockKind,
) (*block.Buffer, error) {

	// TODO (high): The read function requires the buffer pool to be available to
	//  obtain the pre-allocated buffer for handling the read stream.
	//  An optimization is to have a caching mechanism to cache the value of
	//  the blockData , aka BlockCache (key: File ID + BlockHandle --> value: []byte)
	//  Research on how to implement an efficient Block's Cache

	if bpool == nil {
		return nil, fmt.Errorf("blockData pool is nil")
	}

	// TODO
	//  1. Get the pre-allocated []bytes from the pool
	//  2. Read the data from the blockData handle
	//  3. Validate checksum. Note assume we only use exactly 1 checksum method for whole project
	//  4. Read compressor method --> Decompress the data

	blockData := bpool.Get(int(bh.Length))
	blockData = blockData[:bh.Length]
	if err := r.storageReader.ReadAt(blockData, bh.Offset); err != nil {
		bpool.Put(blockData)
		return nil, err
	}

	// Assume we would use CRC32 checksum method for every operation
	if !r.validateChecksum(common.CRC32Checksum, blockData) {
		bpool.Put(blockData)
		return nil, common.MismatchedChecksumError
	}

	// decompress block's data

	compressedLength := bh.Length - block.TrailerLen
	compressor := compression.NewCompressor(
		compression.CompressionType(blockData[compressedLength]),
	)

	decompressedLen, err := compressor.DecompressedLen(blockData[:compressedLength])
	if err != nil {
		bpool.Put(blockData)
		return nil, err
	}

	decompressed := bpool.Get(decompressedLen)
	decompressed = decompressed[:decompressedLen]
	err = compressor.Decompress(decompressed, blockData[:compressedLength])
	bpool.Put(blockData)

	if err != nil {
		bpool.Put(decompressed)
		return nil, err
	}

	return block.MakeBufferRaw(decompressed), nil
}

func (r *RowBlockReader) validateChecksum(checksumType common.ChecksumType, blockData []byte) bool {
	blockLengthWithoutTrailer := len(blockData) - block.TrailerLen
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
