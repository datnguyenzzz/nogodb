package colblock

import (
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/block"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	commonBlock "github.com/datnguyenzzz/nogodb/lib/go-sstable/common/block"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/compression"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/storage"
)

type IndexWriter struct {
	firstLevelIndex *IndexBlockWriter
	uncompressed    []byte

	secondLevelIndex *IndexBlockWriter

	// storage
	storageWriter storage.ILayoutWriter

	// utilities
	flushDecider common.IFlushDecider
	comparer     common.IComparer
	compressor   compression.ICompression
	checksumer   common.IChecksum

	// indexBuffer holds the all compressed block of the completed first level index
	// and they will be flushed to the storage at once when the SSTable is closed.
	indexBuffer []struct {
		key        *common.InternalKey
		rows       int
		compressed *commonBlock.PhysicalBlock
	}

	prevKey *common.InternalKey
}

func NewIndexWriter(
	storageWriter storage.ILayoutWriter,
	flushDecider common.IFlushDecider,
	comparer common.IComparer,
	compressor compression.ICompression,
	checksumer common.IChecksum,
) *IndexWriter {
	return &IndexWriter{
		firstLevelIndex:  NewIndexBlockWriter(),
		secondLevelIndex: NewIndexBlockWriter(),

		storageWriter: storageWriter,
		flushDecider:  flushDecider,
		comparer:      comparer,
		compressor:    compressor,
		checksumer:    checksumer,
	}
}

func (iw *IndexWriter) Add(key *common.InternalKey, bh *commonBlock.BlockHandle) error {
	sizeBefore := iw.firstLevelIndex.Size()

	iw.firstLevelIndex.Add(key.UserKey, bh)

	if iw.flushDecider.ShouldFlush(int(sizeBefore), int(iw.firstLevelIndex.Size())) {
		// flush the first index block to the buffered memory
		// then re-adding the current KV because the first level index block
		// will be reset after flushing
		iw.flushToMemWithoutLastKey(int(sizeBefore))
		iw.firstLevelIndex.Add(key.UserKey, bh)
	}

	iw.prevKey = key
	return nil
}

func (iw *IndexWriter) BuildIndex() error {
	panic("not implemented")
}

// flushToMemWithoutLastKey flushes the current first level index block to the memory
// without the last key, and reset the first level index block for the next entries
func (iw *IndexWriter) flushToMemWithoutLastKey(size int) {
	rows := int(iw.firstLevelIndex.Rows())
	idx := struct {
		key        *common.InternalKey
		rows       int
		compressed *commonBlock.PhysicalBlock
	}{
		key:        iw.prevKey,
		rows:       rows - 1,
		compressed: nil,
	}

	block.GrowSize(&iw.uncompressed, size)
	iw.uncompressed = iw.firstLevelIndex.Finish(uint32(rows-1), size)
	idx.compressed = block.CompressToPb(iw.compressor, iw.checksumer, iw.uncompressed)
	iw.indexBuffer = append(iw.indexBuffer, idx)

	// reset the first level index block for the next entries
	iw.firstLevelIndex.Reset()
	if cap(iw.uncompressed) > maxBlockRetainedSize {
		iw.uncompressed = nil
	}
}

var _ block.IIndexWriter = (*IndexWriter)(nil)
