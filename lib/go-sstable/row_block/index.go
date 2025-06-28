package row_block

import (
	"encoding/binary"

	go_bytesbufferpool "github.com/datnguyenzzz/nogodb/lib/go-bytesbufferpool"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
)

const (
	estimateNumberOfTopLevelBlocks = 128
)

type topLevelIndex struct {
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
	indexBlock      *rowBlockBuf
	comparer        common.IComparer
	flushDecider    common.IFlushDecider
	topLevelIndices []*topLevelIndex
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
	return w.indexBlock.WriteEntry(*key, encoded)
}

func (w *indexWriter) mightFlush(key *common.InternalKey) error {
	estimatedBHSize := binary.MaxVarintLen64 * 2
	if !w.indexBlock.ShouldFlush(key.Size(), estimatedBHSize, w.flushDecider) {
		return nil
	}

	// Start flushing the index block to the top level of the index block
	// As this function will only be triggered as a part of the DataBlock.Flush()
	// Therefore we can always assure that they are no other concurrent attempts

	// Store the top level indices into the memory, we will only write the index blocks
	// (which is classified as meta block) once the table is finished.
	// TODO: Open questions:
	//   1. How to recover the indices if the machine crash ?
	idx := &topLevelIndex{
		entries: w.indexBlock.EntryCount(),
	}
	uncompressed := go_bytesbufferpool.Get(w.indexBlock.EstimateSize())
	w.indexBlock.Finish(uncompressed)
	idx.finishedBlock = uncompressed
	w.topLevelIndices = append(w.topLevelIndices, idx)
	go_bytesbufferpool.Put(uncompressed)
	return nil
}

func newIndexWriter(comparer common.IComparer, flushDecider common.IFlushDecider) *indexWriter {
	return &indexWriter{
		// The index block also use the row oriented layout.
		// And its restart interval is 1, aka every entry is a restart point.
		indexBlock:      newBlock(1),
		comparer:        comparer,
		flushDecider:    flushDecider,
		topLevelIndices: make([]*topLevelIndex, 0, estimateNumberOfTopLevelBlocks),
	}
}
