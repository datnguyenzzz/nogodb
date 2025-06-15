package row_block

import (
	"fmt"

	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common/compression"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/filter"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/options"
)

type compressorPerBlock map[common.BlockKind]compression.ICompression

// RowBlockWriter is an implementation of common.RawWriter, which writes SSTables with row-oriented blocks
type RowBlockWriter struct {
	dataBlock             *dataBlockBuf
	dataBlockFlushDecider common.IFlushDecider
	comparer              common.IComparer
	filterWriter          filter.IWriter
	compressors           compressorPerBlock
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

	if err := rw.doFlush(key, len(value)); err != nil {
		return err
	}

	if rw.filterWriter != nil {
		rw.filterWriter.Add(key.UserKey)
	}

	if err := rw.dataBlock.WriteToBuf(key, value); err != nil {
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

// doFlush validate if required or not, if yes then flush the data to the stable storage
func (rw *RowBlockWriter) doFlush(key common.InternalKey, dataLen int) error {
	// Skip if the data block is not ready to flush
	if !rw.dataBlock.ShouldFlush(key.Size(), dataLen, rw.dataBlockFlushDecider) {
		return nil
	}

	uncompressed := rw.dataBlock.Finish()
	compressor := rw.compressors[common.BlockKindData]
	compressed := compressor.Compress(nil, uncompressed)

	// TODO Checksum the compressed with its method
	panic("finish implementing me")
}

func NewRowBlockWriter(writable common.Writable, opts options.BlockWriteOpt) *RowBlockWriter {
	c := compressorPerBlock{}
	for blockKind, _ := range common.BlockKindStrings {
		if _, ok := opts.Compression[blockKind]; !ok {
			c[blockKind] = compression.NewCompressor(opts.DefaultCompression)
			continue
		}

		c[blockKind] = compression.NewCompressor(opts.Compression[blockKind])
	}
	return &RowBlockWriter{
		dataBlock:             newDataBlock(opts.BlockRestartInterval),
		comparer:              common.NewComparer(),
		filterWriter:          filter.NewFilterWriter(filter.BloomFilter), // Use bloom filter as a default method
		dataBlockFlushDecider: common.NewFlushDecider(opts.BlockSize, opts.BlockSizeThreshold),
		compressors:           c,
	}
}

var _ common.RawWriter = (*RowBlockWriter)(nil)
