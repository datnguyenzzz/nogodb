package go_sstable

import "github.com/datnguyenzzz/nogodb/lib/go-sstable/internal"

type WriteOptFn func(w *Writer)

type writeOpt struct {
	// blockRestartInterval is the number of keys between restart points for delta encoding of keys.
	//
	// The default value is 16.
	blockRestartInterval int

	// blockSize is the target uncompressed size in bytes of each table block.
	//
	// The default value is 4KB.
	blockSize int

	// blockSizeThreshold finishes a block if the block size is larger than the
	// specified percentage of the target block size and adding the next entry
	// would cause the block to be larger than the target block size.
	//
	// The default value is 0.9.
	blockSizeThreshold float32

	// compression defines the per-block compression to use.
	compression internal.Compression

	// tableFormat specifies the format version for writing sstables.
	tableFormat TableFormat
}

var defaultWriteOpt = &writeOpt{
	blockRestartInterval: 16,
	blockSize:            4 * 1024,
	blockSizeThreshold:   0.9,
	compression:          internal.SnappyCompression,
	tableFormat:          RowBlockedBaseTableFormat,
}

func WithBlockRestartInterval(interval int) WriteOptFn {
	return func(w *Writer) {
		w.opts.blockRestartInterval = interval
	}
}

func WithBlockSize(blockSize int) WriteOptFn {
	return func(w *Writer) {
		w.opts.blockSize = blockSize
	}
}

func WithBlockSizeThreshold(blockSizeThreshold float32) WriteOptFn {
	return func(w *Writer) {
		w.opts.blockSizeThreshold = blockSizeThreshold
	}
}

func WithCompression(compression internal.Compression) WriteOptFn {
	return func(w *Writer) {
		w.opts.compression = compression
	}
}

func WithTableFormat(tableFormat TableFormat) WriteOptFn {
	return func(w *Writer) {
		w.opts.tableFormat = tableFormat
	}
}
