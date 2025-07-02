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

func (rw *RowBlockWriter) Add(key common.InternalKey, value []byte) error {
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

func (rw *RowBlockWriter) Close() error {
	defer func() {
		rw.storageWriter.Abort()
	}()

	// Flush the last (current) data block to the storage
	if rw.dataBlock.EntryCount() > 0 {
		if err := rw.doFlush(common.InternalKey{}); err != nil {
			return err
		}
	}

	// Wait to finish all current pending data_block_flush task
	// Then close the queue
	if err := rw.taskQueue.Close(); err != nil {
		return err
	}

	// Build and Flush filter block
	if rw.filterWriter != nil {
		var rawData []byte
		rw.filterWriter.Build(&rawData)
		// compress and checksum
		pb := &common.PhysicalBlock{}
		compressor := rw.compressors[common.BlockKindFilter]
		compressed := compressor.Compress(nil, rawData)
		checksum := rw.checksumer.Checksum(compressed, byte(compressor.GetType()))
		pb.SetData(compressed)
		pb.SetTrailer(byte(compressor.GetType()), checksum)
		// write to the stable storage
		if _, err := rw.storageWriter.WritePhysicalBlock(*pb); err != nil {
			return err
		}
	}
	// Build and Flush index block
	if err := rw.indexWriter.buildIndex(); err != nil {
		return err
	}

	// TODO: research on what we need to do here ?
	//   - Write footer
	//   - Reset all buffers

	panic("implement me")
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

	return rw.doFlush(key)
}

func (rw *RowBlockWriter) doFlush(key common.InternalKey) error {
	prevKey := rw.dataBlock.CurKey()
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
	crc32Checksum := common.NewChecksumer(common.CRC32Checksum)
	comparer := common.NewComparer()
	flushDecider := common.NewFlushDecider(opts.BlockSize, opts.BlockSizeThreshold)
	storageWriter := storage.NewWriter(writable)
	return &RowBlockWriter{
		opts:          opts,
		storageWriter: storageWriter,
		dataBlock:     newBlock(opts.BlockRestartInterval),
		indexWriter: newIndexWriter(
			comparer,
			c[common.BlockKindIndex],
			crc32Checksum,
			flushDecider,
			storageWriter,
		),
		comparer:     comparer,
		filterWriter: filter.NewFilterWriter(filter.BloomFilter), // Use bloom filter as a default method
		flushDecider: flushDecider,
		compressors:  c,
		checksumer:   crc32Checksum,
		taskQueue:    queue.NewQueue(0, false),
	}
}

var _ common.RawWriter = (*RowBlockWriter)(nil)
