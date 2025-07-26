package row_block

import "github.com/datnguyenzzz/nogodb/lib/go-sstable/storage"

// RowBlockReader reads row-based blocks from a single file, handling caching,
// checksum validation and decompression.
type RowBlockReader struct {
	storageReader storage.ILayoutReader
}

func (r *RowBlockReader) Init(fr storage.ILayoutReader) {
	r.storageReader = fr
}

func (r *RowBlockReader) Read() error {
	return nil
}
