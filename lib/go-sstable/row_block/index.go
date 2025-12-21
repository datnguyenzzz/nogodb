package row_block

import (
	"encoding/binary"

	"github.com/datnguyenzzz/nogodb/lib/go-bytesbufferpool/predictable_size"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	block2 "github.com/datnguyenzzz/nogodb/lib/go-sstable/common/block"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/compression"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/options"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/storage"
	"go.uber.org/zap"
)

const (
	estimateNumberOfTopLevelBlocks = 128
)

type firstLevelIndex struct {
	key           *common.InternalKey
	entries       int
	finishedBlock []byte
}

// indexWriter The `i'th` value is the encoded block handle of the `i'th` data block.
// The `i'th` key is a string `>=` last key in that data block and `<` the first key
// in the successive data block.
//
// It consists of a sequence of lower-level
// index blocks with block handles for data blocks followed by a single top-level
// index block with block handles for the lower-level index blocks.
type indexWriter struct {
	firstLevelBlock   *rowBlockBuf
	secondLevelBlock  *rowBlockBuf
	comparer          common.IComparer
	compressor        compression.ICompression
	checksumer        common.IChecksum
	flushDecider      common.IFlushDecider
	storageWriter     storage.ILayoutWriter
	firstLevelIndices []*firstLevelIndex
	metaIndexBlock    *rowBlockBuf
	bytesBufferPool   *predictable_size.PredictablePool
}

func (w *indexWriter) createKey(prevKey, key *common.InternalKey) *common.InternalKey {
	var sep *common.InternalKey
	if key.UserKey == nil && key.Trailer == 0 {
		sep = prevKey.Successor(w.comparer)
	} else {
		sep = prevKey.Separator(w.comparer, key)
	}

	return sep
}

func (w *indexWriter) add(key *common.InternalKey, bh *block2.BlockHandle) error {
	if bh.Length == 0 {
		return nil
	}
	if err := w.mightFlushToMem(key); err != nil {
		return err
	}
	encoded := make([]byte, block2.MaxBlockHandleBytes)
	n := bh.EncodeInto(encoded)
	encoded = encoded[:n]
	return w.firstLevelBlock.WriteEntry(*key, encoded)
}

func (w *indexWriter) mightFlushToMem(key *common.InternalKey) error {
	estimatedBHSize := binary.MaxVarintLen64 * 2
	if !w.firstLevelBlock.ShouldFlush(key.Size(), estimatedBHSize, w.flushDecider) {
		return nil
	}

	// Start flushing the index block to the top level of the index block
	// As this function will only be triggered as a part of the DataBlock.Flush()
	// Therefore we can always assure that they are no other concurrent attempts

	// Instead of flushing directly to the storage, we store the top level indices
	// into the memory, we will only write the index blocks (which is classified as meta block)
	// once the table is finished. We do it because based on the design, the index
	// blocks are only written after all of data blocks are flushed, and the table is
	// about closing
	// TODO (low): Open questions:
	//   1. How to recover the indices if the machine crash ?
	w.flushFirstLevelIndexToMem(key)
	return nil
}

func (w *indexWriter) flushFirstLevelIndexToMem(key *common.InternalKey) {
	idx := &firstLevelIndex{
		key:     key,
		entries: w.firstLevelBlock.EntryCount(),
	}
	uncompressed := w.bytesBufferPool.Get(w.firstLevelBlock.EstimateSize())
	uncompressed = uncompressed[:w.firstLevelBlock.EstimateSize()]
	w.firstLevelBlock.Finish(uncompressed)
	idx.finishedBlock = uncompressed
	w.firstLevelIndices = append(w.firstLevelIndices, idx)
	w.bytesBufferPool.Put(uncompressed)
}

// buildIndex build the 2-level index for the SST
func (w *indexWriter) buildIndex() error {
	// flush all of pending/un-finished 1-level indices to mem
	w.flushFirstLevelIndexToMem(w.firstLevelBlock.CurKey())
	for _, idx := range w.firstLevelIndices {
		// 1. Write the compressed first level index to the storage
		pb := compressToPb(w.compressor, w.checksumer, idx.finishedBlock)
		bh, err := w.storageWriter.WritePhysicalBlock(*pb)
		if err != nil {
			return err
		}
		// 2. Write the encoded value of the 1-level index block handle
		// into buffer
		encodedBH := make([]byte, block2.MaxBlockHandleBytes)
		n := bh.EncodeInto(encodedBH)
		if err := w.secondLevelBlock.WriteEntry(*idx.key, encodedBH[:n]); err != nil {
			return err
		}
	}

	// 3. Build and Write the 2-level index buffer to the storage
	uncompressed := w.bytesBufferPool.Get(w.secondLevelBlock.EstimateSize())
	uncompressed = uncompressed[:w.secondLevelBlock.EstimateSize()]
	w.secondLevelBlock.Finish(uncompressed)
	pb := compressToPb(w.compressor, w.checksumer, uncompressed)
	bh, err := w.storageWriter.WritePhysicalBlock(*pb)
	if err == nil {
		// save the block location of the 2-level index to the index
		// key - 1 byte indicate block kind , value - varint encoded of the block handle
		encodedBH := make([]byte, block2.MaxBlockHandleBytes)
		n := bh.EncodeInto(encodedBH)
		err := w.metaIndexBlock.WriteEntry(
			common.MakeMetaIndexKey(block2.BlockKindIndex),
			encodedBH[:n],
		)
		if err != nil {
			zap.L().Error("failed to write the 2-level index block to the meta index", zap.Error(err))
		}
	}

	w.bytesBufferPool.Put(uncompressed)
	return err
}

func (w *indexWriter) Release() {
	w.firstLevelBlock.CleanUpForReuse()
	w.firstLevelBlock.Release()
	clear(w.firstLevelIndices)
}

func newIndexWriter(
	comparer common.IComparer,
	compressor compression.ICompression,
	checksumer common.IChecksum,
	flushDecider common.IFlushDecider,
	storageWriter storage.ILayoutWriter,
	metaIndexBlock *rowBlockBuf,
	bufferPool *predictable_size.PredictablePool,
	opts options.BlockWriteOpt,
) *indexWriter {
	return &indexWriter{
		// The index block also use the row oriented layout.
		// And its restart interval is 1, aka every entry is a restart point.
		firstLevelBlock:   newBlock(1, bufferPool, opts.BlockSize),
		secondLevelBlock:  newBlock(1, bufferPool, opts.BlockSize),
		comparer:          comparer,
		compressor:        compressor,
		checksumer:        checksumer,
		flushDecider:      flushDecider,
		storageWriter:     storageWriter,
		firstLevelIndices: make([]*firstLevelIndex, 0, estimateNumberOfTopLevelBlocks),
		metaIndexBlock:    metaIndexBlock,
		bytesBufferPool:   bufferPool,
	}
}
