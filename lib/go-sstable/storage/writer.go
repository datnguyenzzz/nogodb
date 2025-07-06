package storage

import "github.com/datnguyenzzz/nogodb/lib/go-sstable/common"

type writer struct {
	objWritable Writable
}

type IWriter interface {
	WritePhysicalBlock(b common.PhysicalBlock) (common.BlockHandle, error)
	WriteRawBytes(b []byte) error
	// Abort aborts writing the table, aborting the underlying writable too.
	Abort()
	Finish() error
}

// -- Implementations -- \\

func (w *writer) WritePhysicalBlock(pb common.PhysicalBlock) (common.BlockHandle, error) {
	//TODO implement me
	panic("implement me")
}

func (w *writer) WriteRawBytes(b []byte) error {
	panic("implement me")
}

func (w *writer) Abort() {
	w.objWritable.Abort()
}

func (w *writer) Finish() error {
	return w.objWritable.Finish()
}

func NewWriter(w Writable) IWriter {
	return &writer{
		objWritable: w,
	}
}
