package storage

import go_fs "github.com/datnguyenzzz/nogodb/lib/go-fs"

type layoutReader struct {
	fsReader go_fs.Readable
}

type ILayoutReader interface{}

func NewLayoutReader(fsReader go_fs.Readable) ILayoutReader {
	return &layoutReader{
		fsReader: fsReader,
	}
}

var _ ILayoutReader = (*layoutReader)(nil)
