package row_block

import "github.com/datnguyenzzz/nogodb/lib/go-sstable/storage"

// RowBlockReader reads blocks from a single file, handling caching, checksum
// validation and decompression.
type RowBlockReader struct {
	storageReader storage.ILayoutReader
}

func NewRowBlockReader(fr storage.ILayoutReader) *RowBlockReader {
	return &RowBlockReader{
		storageReader: fr,
	}
}
