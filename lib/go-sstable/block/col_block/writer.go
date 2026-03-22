package colblock

import (
	"github.com/datnguyenzzz/nogodb/lib/go-bytesbufferpool/predictable_size"
	go_fs "github.com/datnguyenzzz/nogodb/lib/go-fs"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/compression"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/filter"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/options"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/queue"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/storage"
)

const (
	flushQueueLen = 100_000
)

type ColBlockWriter struct {
	opt             options.BlockWriteOpt
	bytesBufferPool *predictable_size.PredictablePool
	tableVersion    common.TableVersion

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
	return nil
}

// Close finishes writing the table and closes the underlying file that the table was written to.
func (c *ColBlockWriter) Close() error {
	return nil
}

func NewColBlockWriter(
	w go_fs.Writable,
	opts options.BlockWriteOpt,
	ver common.TableVersion,
) *ColBlockWriter {
	return &ColBlockWriter{
		opt:             opts,
		bytesBufferPool: predictable_size.NewPredictablePool(),
		tableVersion:    ver,

		storageWriter: storage.NewLayoutWriter(w),

		taskQueue: queue.NewQueue(flushQueueLen, false),

		filterWriter: filter.NewFilterWriter(filter.BloomFilter),

		flushDecider: common.NewFlushDecider(opts.BlockSize, opts.BlockSizeThreshold),
		comparer:     common.NewComparer(),
		compressors:  compression.NewCompressor(opts.DefaultCompression),
		checksumer:   common.NewChecksumer(common.CRC32Checksum),

		dataBlock:  nil, // TODO: fill me
		indexBlock: nil, // TODO: fill me
	}
}

var _ common.InternalWriter = (*ColBlockWriter)(nil)
