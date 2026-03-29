package colblock

import (
	"fmt"

	go_fs "github.com/datnguyenzzz/nogodb/lib/go-fs"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/block"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/compression"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/filter"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/options"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/queue"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/storage"
)

const (
	flushQueueLen        = 100_000
	maxBlockRetainedSize = 256 << 10
)

type ColBlockWriter struct {
	opt          options.BlockWriteOpt
	tableVersion common.TableVersion
	uncompressed []byte

	// storage
	storageWriter storage.ILayoutWriter

	// flush queue
	taskQueue queue.IQueue

	// filter
	filterWriter filter.IWriter

	// utilities
	flushDecider common.IFlushDecider
	comparer     common.IComparer
	compressors  compression.ICompression
	checksumer   common.IChecksum

	// data and indexes
	dataBlock  *DataBlockWriter
	indexBlock *IndexBlockWriter
}

// Add adds a key-value pair to the sstable.
func (c *ColBlockWriter) Add(key common.InternalKey, value []byte) error {
	if err := c.validate(key); err != nil {
		return err
	}

	sizeBefore := c.dataBlock.Size()
	keyBefore := c.dataBlock.CurrKey()
	c.dataBlock.Add(key, value)
	// we will check and flush the data block without the last key
	if c.flushDecider.ShouldFlush(int(sizeBefore), int(c.dataBlock.Size())) {
		if err := c.doFlushWithoutLastKey(int(sizeBefore), keyBefore); err != nil {
			return err
		}

		// after flushing, the data block will be reset, so we need re-adding the current kv
		c.dataBlock.Add(key, value)
	}

	if c.filterWriter != nil {
		// write the actual key, exclude the MVCC suffix
		prefix := c.comparer.Split(key.UserKey)
		c.filterWriter.Add(key.UserKey[:prefix])
	}

	return nil
}

// Close finishes writing the table and closes the underlying file that the table was written to.
func (c *ColBlockWriter) Close() error {
	return nil
}

func (c *ColBlockWriter) validate(key common.InternalKey) error {
	if c.dataBlock.Rows() == 0 {
		return nil
	}

	prevKey := *c.dataBlock.CurrKey()
	cmp := c.comparer.Compare(key.UserKey, prevKey.UserKey)
	if cmp < 0 || (cmp == 0 && prevKey.Trailer < key.Trailer) {
		return fmt.Errorf("%w: keys must be added in strictly increasing order", common.ClientInvalidRequestError)
	}

	return nil
}

// doFlushWithoutLastKey flush the data block except the last key
func (c *ColBlockWriter) doFlushWithoutLastKey(size int, indexKey *common.InternalKey) error {
	currRows := c.dataBlock.Rows()

	block.GrowSize(&c.uncompressed, size)

	c.uncompressed = c.dataBlock.Finish(currRows-1, size)

	task := block.SpawnNewTask()
	task.StorageWriter = c.storageWriter
	task.Physical = block.CompressToPb(
		c.compressors,
		c.checksumer,
		c.uncompressed,
	)
	task.IndexKey = indexKey
	task.IndexWriter = c.indexBlock

	c.taskQueue.Put(task)

	// reset the data block for the new writes
	c.dataBlock.Reset()
	if cap(c.uncompressed) > maxBlockRetainedSize {
		c.uncompressed = nil
	}

	return nil
}

func NewColBlockWriter(
	w go_fs.Writable,
	opts options.BlockWriteOpt,
	ver common.TableVersion,
) *ColBlockWriter {
	comparer := common.NewComparer()
	return &ColBlockWriter{
		opt:          opts,
		tableVersion: ver,

		storageWriter: storage.NewLayoutWriter(w),

		taskQueue: queue.NewQueue(flushQueueLen, false),

		filterWriter: filter.NewFilterWriter(filter.BloomFilter),

		flushDecider: common.NewFlushDecider(opts.BlockSize, opts.BlockSizeThreshold),
		comparer:     comparer,
		compressors:  compression.NewCompressor(opts.DefaultCompression),
		checksumer:   common.NewChecksumer(common.CRC32Checksum),

		dataBlock:  NewDataBlockWriter(comparer),
		indexBlock: NewIndexBlockWriter(),
	}
}

var _ common.InternalWriter = (*ColBlockWriter)(nil)
