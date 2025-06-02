package base

type WriteOpt struct {
	// BlockRestartInterval is the number of keys between restart points for delta encoding of keys.
	//
	// The default value is 16.
	BlockRestartInterval int

	// BlockSize is the target uncompressed size in bytes of each table block.
	//
	// The default value is 4KB.
	BlockSize int

	// BlockSizeThreshold finishes a block if the block size is larger than the
	// specified percentage of the target block size and adding the next entry
	// would cause the block to be larger than the target block size.
	//
	// The default value is 0.9.
	BlockSizeThreshold float32

	// Compression defines the per-block compression to use.
	Compression Compression

	// TableFormat specifies the format version for writing sstables.
	TableFormat TableFormat
}
