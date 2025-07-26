package go_sstable

import (
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/block"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/compression"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/options"
)

type WriteOptFn func(w *Writer)

var DefaultWriteOpt = &options.BlockWriteOpt{
	BlockRestartInterval: 16,
	BlockSize:            4 * 1024,
	BlockSizeThreshold:   0.9,
	Compression: map[block.BlockKind]compression.CompressionType{
		block.BlockKindData:   compression.SnappyCompression,
		block.BlockKindIndex:  compression.SnappyCompression,
		block.BlockKindFilter: compression.SnappyCompression,
	},
	TableFormat: common.RowBlockedBaseTableFormat,
}

func WithBlockRestartInterval(interval int) WriteOptFn {
	return func(w *Writer) {
		w.datablockOpts.BlockRestartInterval = interval
	}
}

func WithBlockSize(blockSize int) WriteOptFn {
	return func(w *Writer) {
		w.datablockOpts.BlockSize = blockSize
	}
}

func WithBlockSizeThreshold(blockSizeThreshold float32) WriteOptFn {
	return func(w *Writer) {
		w.datablockOpts.BlockSizeThreshold = blockSizeThreshold
	}
}

//func WithCompression(compression compression.CompressionType) WriteOptFn {
//	return func(w *Writer) {
//		w.datablockOpts.Compression = compression
//	}
//}

func WithTableFormat(tableFormat common.TableFormat) WriteOptFn {
	return func(w *Writer) {
		w.datablockOpts.TableFormat = tableFormat
	}
}
