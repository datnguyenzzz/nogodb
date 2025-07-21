package storage

import (
	"context"

	go_fs "github.com/datnguyenzzz/nogodb/lib/go-fs"
)

type layoutReader struct {
	fsReader go_fs.Readable
}

type ILayoutReader interface {
	// ReadAt reads len(p) bytes into p starting at offset off.
	//
	// Does not return partial results; if off + len(p) is past the end of the
	// object, an error is returned.
	ReadAt(ctx context.Context, p []byte, off int64) error

	Close() error
}

// Implementations \\

func (l layoutReader) ReadAt(ctx context.Context, p []byte, off int64) error {
	//TODO implement me
	panic("implement me")
}

func (l layoutReader) Close() error {
	//TODO implement me
	panic("implement me")
}

func NewLayoutReader(fsReader go_fs.Readable) ILayoutReader {
	return &layoutReader{
		fsReader: fsReader,
	}
}

var _ ILayoutReader = (*layoutReader)(nil)
