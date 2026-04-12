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
	if iw.comparer.Compare(iw.prevKey.UserKey, key.UserKey) >= 0 {
		panic("IndexWriter key must be in a strictly increasing order")
	}

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

func (iw *IndexWriter) BuildIndex() (*commonBlock.BlockHandle, error) {
	// flush all pendings 1st level index
	iw.flushAll()

	for _, idx := range iw.indexBuffer {
		bh, err := iw.storageWriter.WritePhysicalBlock(*idx.compressed)
		if err != nil {
			return nil, err
		}

		iw.secondLevelIndex.Add(idx.key.UserKey, &bh)
	}

	// build the 2nd level index and write to the storage
	rows := uint32(iw.secondLevelIndex.Rows())
	size := int(iw.secondLevelIndex.Size())
	block.GrowSize(&iw.uncompressed, size)
	iw.uncompressed = iw.secondLevelIndex.Finish(rows, size)
	pb := block.CompressToPb(iw.compressor, iw.checksumer, iw.uncompressed)
	bh, err := iw.storageWriter.WritePhysicalBlock(*pb)
	if err != nil {
		return nil, err
	}

	return &bh, nil
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

func (iw *IndexWriter) flushAll() {
	rows := int(iw.firstLevelIndex.Rows())
	size := int(iw.firstLevelIndex.Size())
	idx := struct {
		key        *common.InternalKey
		rows       int
		compressed *commonBlock.PhysicalBlock
	}{
		key:        iw.prevKey,
		rows:       rows,
		compressed: nil,
	}

	block.GrowSize(&iw.uncompressed, size)
	iw.uncompressed = iw.firstLevelIndex.Finish(uint32(rows), size)
	idx.compressed = block.CompressToPb(iw.compressor, iw.checksumer, iw.uncompressed)
	iw.indexBuffer = append(iw.indexBuffer, idx)

	// reset the first level index block for the next entries
	iw.firstLevelIndex.Reset()
	if cap(iw.uncompressed) > maxBlockRetainedSize {
		iw.uncompressed = nil
	}
}

var _ block.IIndexWriter = (*IndexWriter)(nil)
