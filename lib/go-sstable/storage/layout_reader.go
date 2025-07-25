package storage

import (
	go_fs "github.com/datnguyenzzz/nogodb/lib/go-fs"
)

type layoutReader struct {
	fsReader go_fs.Readable
}

// ILayoutReader is used to perform reads that are related and might benefit from
// optimizations like read-ahead.
type ILayoutReader interface {
	// ReadAt reads len(p) bytes into p starting at offset off.
	//
	// Does not return partial results; if off + len(p) is past the end of the
	// object, an error is returned.
	ReadAt(p []byte, off uint64) error

	Close() error
}

// Implementations \\

func (l layoutReader) ReadAt(p []byte, off uint64) error {
	//TODO implement me
	panic("implement me")
}

func (l layoutReader) Close() error {
	//TODO implement me
	panic("implement me")
}

func NewLayoutReader(fsReader go_fs.Readable) ILayoutReader {
	// TODO(high): Not every read requests need a read-ahead improvement
	//  need to support caller to have an option to opt-in / opt-out the read ahead optimization
	return &layoutReader{
		fsReader: fsReader,
	}
}

var _ ILayoutReader = (*layoutReader)(nil)
