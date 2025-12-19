package row_block

import (
	"encoding/binary"
	"errors"
	"fmt"

	go_block_cache "github.com/datnguyenzzz/nogodb/lib/go-block-cache"
	"github.com/datnguyenzzz/nogodb/lib/go-bytesbufferpool/predictable_size"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	block_common "github.com/datnguyenzzz/nogodb/lib/go-sstable/common/block"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/compression"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/options"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/storage"
	"go.uber.org/zap"
)

var (
	cacheMiss           = errors.New("cache miss")
	failedUpdateToCache = errors.New("failed to update value to cache")
)

// blockCacheWrapper a block cache with key = [file_num + offset], value = data[offset:offset + length]
type blockCacheWrapper struct {
	fileNum uint64
	c       go_block_cache.IBlockCache
}

type lazyValueWrapper struct {
	lazyValue go_block_cache.LazyValue
}

func (l *lazyValueWrapper) Load() []byte {
	return l.lazyValue.Load()
}

func (l *lazyValueWrapper) Release() {
	l.lazyValue.Release()
}

//go:generate mockery --name=IBlockCacheWrapper --case=underscore --disable-version-string
type IBlockCacheWrapper interface {
	Get(bh *block_common.BlockHandle) (*common.InternalLazyValue, error)
	Set(bh *block_common.BlockHandle, val *common.InternalLazyValue) error
	Close()
}

func (w *blockCacheWrapper) Get(bh *block_common.BlockHandle) (*common.InternalLazyValue, error) {
	lazyValue, exist := w.c.Get(w.fileNum, bh.Offset)
	if !exist {
		return nil, cacheMiss
	}
	val := common.NewBlankInternalLazyValue(common.ValueFromCache)
	if err := val.SetCacheFetcher(&lazyValueWrapper{lazyValue}); err != nil {
		return nil, err
	}

	return &val, nil
}

func (w *blockCacheWrapper) Set(bh *block_common.BlockHandle, val *common.InternalLazyValue) error {
	ok := w.c.Set(w.fileNum, bh.Offset, val.Value())
	if !ok {
		return failedUpdateToCache
	}
	return nil
}

func (w *blockCacheWrapper) Close() {
	w.c.Close()
}

//go:generate mockery --name=IBlockReader --case=underscore --disable-version-string
type IBlockReader interface {
	// Read perform read directly from the source without caching
	Read(bh *block_common.BlockHandle, kind block_common.BlockKind) (*common.InternalLazyValue, error)
	// ReadThroughCache perform read through cache method
	ReadThroughCache(bh *block_common.BlockHandle, kind block_common.BlockKind) (*common.InternalLazyValue, error)
	Init(bpool *predictable_size.PredictablePool, fr storage.ILayoutReader, cacheOpts *options.CacheOptions)
	Release()
}

// RowBlockReader reads row-based blocks from a single file,
// handling block caching / read through cache, checksum validation
// and decompression.
type RowBlockReader struct {
	bpool         *predictable_size.PredictablePool
	storageReader storage.ILayoutReader
	blockCache    IBlockCacheWrapper
}

func (r *RowBlockReader) Init(
	bpool *predictable_size.PredictablePool,
	fr storage.ILayoutReader,
	cacheOpts *options.CacheOptions,
) {
	r.bpool = bpool
	r.storageReader = fr
	if r.blockCache == nil && cacheOpts != nil {
		c := go_block_cache.NewMap(
			go_block_cache.WithCacheType(cacheOpts.CacheMethod),
			go_block_cache.WithMaxSize(cacheOpts.MaxSize),
		)
		r.blockCache = &blockCacheWrapper{
			fileNum: uint64(cacheOpts.FileNum),
			c:       c,
		}
	}
}

func (r *RowBlockReader) Release() {
	if r.blockCache != nil {
		r.blockCache.Close()
	}
	_ = r.storageReader.Close()
	r.blockCache = nil
}

func (r *RowBlockReader) ReadThroughCache(
	bh *block_common.BlockHandle,
	kind block_common.BlockKind,
) (*common.InternalLazyValue, error) {
	if r.blockCache == nil {
		zap.L().Warn("ReadThroughCache, Block cache is not enabled, fall back!")
		return r.readFromStorage(bh, kind)
	}

	cachedVal, err := r.blockCache.Get(bh)
	if err == nil {
		return cachedVal, nil
	}

	fromStorageVal, err := r.readFromStorage(bh, kind)
	if err != nil {
		return nil, err
	}
	if err := r.blockCache.Set(bh, fromStorageVal); err != nil {
		return nil, err
	}

	return fromStorageVal, nil
}

func (r *RowBlockReader) Read(
	bh *block_common.BlockHandle,
	kind block_common.BlockKind,
) (*common.InternalLazyValue, error) {
	return r.readFromStorage(bh, kind)
}

func (r *RowBlockReader) readFromStorage(
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
