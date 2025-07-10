package storage

import (
	go_fs "github.com/datnguyenzzz/nogodb/lib/go-fs"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
)

type layoutWriter struct {
	objWritable go_fs.Writable
}

type ILayoutWriter interface {
	WritePhysicalBlock(b common.PhysicalBlock) (common.BlockHandle, error)
	WriteRawBytes(b []byte) error
	// Abort aborts writing the table, aborting the underlying writable too.
	Abort()
	Finish() error
}

var _ ILayoutWriter = (*layoutWriter)(nil)

// -- Implementations -- \\

func (w *layoutWriter) WritePhysicalBlock(pb common.PhysicalBlock) (common.BlockHandle, error) {
	//TODO implement me
	panic("implement me")
}

func (w *layoutWriter) WriteRawBytes(b []byte) error {
	//TODO implement me
	panic("implement me")
}

func (w *layoutWriter) Abort() {
	w.objWritable.Abort()
}

func (w *layoutWriter) Finish() error {
	return w.objWritable.Finish()
}

func NewLayoutWriter(w go_fs.Writable) ILayoutWriter {
	return &layoutWriter{
		objWritable: w,
	}
}
