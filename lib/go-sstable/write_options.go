package go_sstable

import "github.com/datnguyenzzz/nogodb/lib/go-sstable/base"

type WriteOptFn func(w *Writer)

var DefaultWriteOpt = &base.WriteOpt{
	BlockRestartInterval: 16,
	BlockSize:            4 * 1024,
	BlockSizeThreshold:   0.9,
	Compression:          base.SnappyCompression,
	TableFormat:          base.RowBlockedBaseTableFormat,
}

func WithBlockRestartInterval(interval int) WriteOptFn {
	return func(w *Writer) {
		w.opts.BlockRestartInterval = interval
	}
}

func WithBlockSize(blockSize int) WriteOptFn {
	return func(w *Writer) {
		w.opts.BlockSize = blockSize
	}
}

func WithBlockSizeThreshold(blockSizeThreshold float32) WriteOptFn {
	return func(w *Writer) {
		w.opts.BlockSizeThreshold = blockSizeThreshold
	}
}

func WithCompression(compression base.Compression) WriteOptFn {
	return func(w *Writer) {
		w.opts.Compression = compression
	}
}

func WithTableFormat(tableFormat base.TableFormat) WriteOptFn {
	return func(w *Writer) {
		w.opts.TableFormat = tableFormat
	}
}
