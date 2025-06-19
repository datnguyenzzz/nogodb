package storage

import "github.com/datnguyenzzz/nogodb/lib/go-sstable/common"

type writer struct {
	objWritable Writable
}

type IWriter interface {
	WritePrecompressedPhysicalBlock(b common.PhysicalBlock) (common.BlockHandle, error)
}

// -- Implementations -- \\

func (w writer) WritePrecompressedPhysicalBlock(pb common.PhysicalBlock) (common.BlockHandle, error) {
	//TODO implement me
	panic("implement me")
}

func NewWriter(w Writable) IWriter {
	return &writer{
		objWritable: w,
	}
}
