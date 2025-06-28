package row_block

import (
	"fmt"

	go_bytesbufferpool "github.com/datnguyenzzz/nogodb/lib/go-bytesbufferpool"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common/compression"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/filter"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/options"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/queue"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/storage"
)

type compressorPerBlock map[common.BlockKind]compression.ICompression

// RowBlockWriter is an implementation of common.RawWriter, which writes SSTables with row-oriented blocks
type RowBlockWriter struct {
	opts          options.BlockWriteOpt
	storageWriter storage.IWriter
	dataBlock     *rowBlockBuf
	indexWriter   *indexWriter
	flushDecider  common.IFlushDecider
	comparer      common.IComparer
	filterWriter  filter.IWriter
	compressors   compressorPerBlock
	checksumer    common.IChecksum
	taskQueue     queue.IQueue
}

func (rw *RowBlockWriter) Error() error {
	//TODO implement me
	panic("implement me")
}

func (rw *RowBlockWriter) Add(key common.InternalKey, value []byte) error {
	return rw.add(key, value)
}

func (rw *RowBlockWriter) Close() error {
	//TODO implement me
	panic("implement me")
}

func (rw *RowBlockWriter) add(key common.InternalKey, value []byte) error {
	if err := rw.validateKey(key); err != nil {
		return err
	}

	if err := rw.mightFlush(key, len(value)); err != nil {
		return err
	}

	if rw.filterWriter != nil {
		rw.filterWriter.Add(key.UserKey)
	}

	if err := rw.dataBlock.WriteEntry(key, value); err != nil {
		return err
	}

	return nil
}

// validateKey ensure the key is added in the asc order.
func (rw *RowBlockWriter) validateKey(key common.InternalKey) error {
	if rw.dataBlock.EntryCount() == 0 {
		return nil
	}
	lastKey := *rw.dataBlock.CurKey()
	cmp := rw.comparer.Compare(key.UserKey, lastKey.UserKey)
	if cmp < 0 || (cmp == 0 && lastKey.Trailer <= key.Trailer) {
		return fmt.Errorf("%w: keys must be added in strictly increasing order", common.ClientInvalidRequestError)
	}

	return nil
}

// mightFlush validate if required or not, if yes then flush (and compression) the data to the stable storage
func (rw *RowBlockWriter) mightFlush(key common.InternalKey, dataLen int) error {
	// Skip if the data block is not ready to flush
	if !rw.dataBlock.ShouldFlush(key.Size(), dataLen, rw.flushDecider) {
		return nil
	}

	// 1. Finish the data block, write the serialised data into the buffer
	uncompressed := go_bytesbufferpool.Get(rw.dataBlock.EstimateSize())
	rw.dataBlock.Finish(uncompressed)

	// 2. Get the task from the pool and compute the physical format
	// of the data block to prepare the needed input for the task
	task := spawnNewTask()
	task.storageWriter = rw.storageWriter
	task.physical = &common.PhysicalBlock{}
	compressor := rw.compressors[common.BlockKindData]
	compressed := compressor.Compress(nil, uncompressed)
	checksum := rw.checksumer.Checksum(compressed, byte(compressor.GetType()))
	task.physical.SetData(compressed)
	task.physical.SetTrailer(byte(compressor.GetType()), checksum)
	// inputs for index writer
	prevKey := rw.dataBlock.CurKey()
	task.indexKey = rw.indexWriter.createKey(prevKey, &key)
	task.indexWriter = rw.indexWriter

	// 3. Put the task into queue that is running on another go-routine
	// for execution
	rw.taskQueue.Put(task)

	// 4. Put the uncompressed buffer back to the bytes buffer pool
	go_bytesbufferpool.Put(uncompressed)
	return nil
}

func NewRowBlockWriter(writable storage.Writable, opts options.BlockWriteOpt) *RowBlockWriter {
	c := compressorPerBlock{}
	for blockKind, _ := range common.BlockKindStrings {
		if _, ok := opts.Compression[blockKind]; !ok {
			c[blockKind] = compression.NewCompressor(opts.DefaultCompression)
			continue
		}

		c[blockKind] = compression.NewCompressor(opts.Compression[blockKind])
	}
	comparer := common.NewComparer()
	flushDecider := common.NewFlushDecider(opts.BlockSize, opts.BlockSizeThreshold)
	return &RowBlockWriter{
		opts:          opts,
		storageWriter: storage.NewWriter(writable),
		dataBlock:     newBlock(opts.BlockRestartInterval),
		indexWriter:   newIndexWriter(comparer, flushDecider),
		comparer:      comparer,
		filterWriter:  filter.NewFilterWriter(filter.BloomFilter), // Use bloom filter as a default method
		flushDecider:  flushDecider,
		compressors:   c,
		checksumer:    common.NewChecksumer(common.CRC32Checksum), // Use crc32 as a default checksum method
		taskQueue:     queue.NewQueue(0, false),
	}
}

var _ common.RawWriter = (*RowBlockWriter)(nil)
