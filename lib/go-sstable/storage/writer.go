package storage

import "github.com/datnguyenzzz/nogodb/lib/go-sstable/common"

type writer struct {
	objWritable Writable
}

type IWriter interface {
	WritePhysicalBlock(b common.PhysicalBlock) (common.BlockHandle, error)
	// Abort aborts writing the table, aborting the underlying writable too.
	Abort()
}

// -- Implementations -- \\

func (w *writer) WritePhysicalBlock(pb common.PhysicalBlock) (common.BlockHandle, error) {
	//TODO implement me
	panic("implement me")
}

func (w *writer) Abort() {
	// TODO implement me
	panic("implement me")
}

func NewWriter(w Writable) IWriter {
	return &writer{
		objWritable: w,
	}
}
