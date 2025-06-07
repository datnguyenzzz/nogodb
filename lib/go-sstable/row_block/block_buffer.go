package row_block

// A blockBuffer holds all the state required to compress and write a data block to disk.
//
// When the RowBlockWriter client adds keys to the SStable, it writes directly into a blockBuffer's blockWriter
// until the block is full. Once a blockBuffer's block is full, the blockBuffer will be passed
// to other goroutines for compression and file I/O.
type blockBuffer struct {
	// memBuf is the destination buffer for compression for storing a copy of the data
	memBuf []byte
}
