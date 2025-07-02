package row_block

import (
	"encoding/binary"

	go_bytesbufferpool "github.com/datnguyenzzz/nogodb/lib/go-bytesbufferpool"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common/compression"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/storage"
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
	storageWriter     storage.IWriter
	firstLevelIndices []*firstLevelIndex
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

func (w *indexWriter) add(key *common.InternalKey, bh *common.BlockHandle) error {
	if bh.Length == 0 {
		return nil
	}
	if err := w.mightFlush(key); err != nil {
		return err
	}
	var encoded []byte
	n := bh.EncodeInto(encoded)
	encoded = encoded[:n]
	return w.firstLevelBlock.WriteEntry(*key, encoded)
}

func (w *indexWriter) mightFlush(key *common.InternalKey) error {
	estimatedBHSize := binary.MaxVarintLen64 * 2
	if !w.firstLevelBlock.ShouldFlush(key.Size(), estimatedBHSize, w.flushDecider) {
		return nil
	}

	// Start flushing the index block to the top level of the index block
	// As this function will only be triggered as a part of the DataBlock.Flush()
	// Therefore we can always assure that they are no other concurrent attempts

	// Store the top level indices into the memory, we will only write the index blocks
	// (which is classified as meta block) once the table is finished.
	// TODO: Open questions:
	//   1. How to recover the indices if the machine crash ?
	w.buildFirstLevelIndices(key)
	return nil
}

func (w *indexWriter) buildFirstLevelIndices(key *common.InternalKey) {
	idx := &firstLevelIndex{
		key:     key,
		entries: w.firstLevelBlock.EntryCount(),
	}
	uncompressed := go_bytesbufferpool.Get(w.firstLevelBlock.EstimateSize())
	w.firstLevelBlock.Finish(uncompressed)
	idx.finishedBlock = uncompressed
	w.firstLevelIndices = append(w.firstLevelIndices, idx)
	go_bytesbufferpool.Put(uncompressed)
}

func (w *indexWriter) buildIndex() error {
	// build all of pending/un-finished 1-level indices
	w.buildFirstLevelIndices(w.firstLevelBlock.CurKey())
	for _, idx := range w.firstLevelIndices {
		// 1. Write the compressed first level index to the storage
		pb := w.compressToPhysicalBlock(idx.finishedBlock)
		bh, err := w.storageWriter.WritePhysicalBlock(*pb)
		if err != nil {
			return err
		}
		// 2. Write the encoded value of the 1-level index block handle
		// into buffer
		var encodedBH []byte
		_ = bh.EncodeInto(encodedBH)
		if err := w.secondLevelBlock.WriteEntry(*idx.key, encodedBH); err != nil {
			return err
		}
	}

	// 3. Find and Write the 2-level index buffer to the storage
	uncompressed := go_bytesbufferpool.Get(w.secondLevelBlock.EstimateSize())
	w.secondLevelBlock.Finish(uncompressed)
	pb := w.compressToPhysicalBlock(uncompressed)
	_, err := w.storageWriter.WritePhysicalBlock(*pb)
	go_bytesbufferpool.Put(uncompressed)
	return err
}

func (w *indexWriter) compressToPhysicalBlock(uncompressed []byte) *common.PhysicalBlock {
	pb := &common.PhysicalBlock{}
	compressed := w.compressor.Compress(nil, uncompressed)
	checksum := w.checksumer.Checksum(compressed, byte(w.compressor.GetType()))
	pb.SetData(compressed)
	pb.SetTrailer(byte(w.compressor.GetType()), checksum)
	return pb
}

func newIndexWriter(
	comparer common.IComparer,
	compressor compression.ICompression,
	checksumer common.IChecksum,
	flushDecider common.IFlushDecider,
	storageWriter storage.IWriter,
) *indexWriter {
	return &indexWriter{
		// The index block also use the row oriented layout.
		// And its restart interval is 1, aka every entry is a restart point.
		firstLevelBlock:   newBlock(1),
		comparer:          comparer,
		compressor:        compressor,
		checksumer:        checksumer,
		flushDecider:      flushDecider,
		storageWriter:     storageWriter,
		firstLevelIndices: make([]*firstLevelIndex, 0, estimateNumberOfTopLevelBlocks),
	}
}
