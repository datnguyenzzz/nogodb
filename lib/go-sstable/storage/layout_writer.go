package storage

import (
	go_fs "github.com/datnguyenzzz/nogodb/lib/go-fs"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common/block"
)

type layoutWriter struct {
	fsWritable go_fs.Writable
	// offset tracks the current offset of the entire physical table
	offset uint64
}

type ILayoutWriter interface {
	// WritePhysicalBlock write a physicalBlock to the file system, and return BlockHandle
	// which contains the offset before writing, and data length
	WritePhysicalBlock(b block.PhysicalBlock) (block.BlockHandle, error)
	WriteRawBytes(b []byte) (block.BlockHandle, error)
	// Abort aborts writing the table, aborting the underlying writable too.
	Abort()
	Finish() error
}

var _ ILayoutWriter = (*layoutWriter)(nil)

// -- Implementations -- \\

func (w *layoutWriter) WritePhysicalBlock(pb block.PhysicalBlock) (block.BlockHandle, error) {
	if err := w.fsWritable.Write(pb.Data); err != nil {
		return block.BlockHandle{}, err
	}
	if err := w.fsWritable.Write(pb.Trailer[:]); err != nil {
		return block.BlockHandle{}, err
	}

	bh := block.BlockHandle{
		Offset: w.offset,
		Length: pb.Size(),
	}

	w.offset += pb.Size()

	return bh, nil
}

func (w *layoutWriter) WriteRawBytes(b []byte) (block.BlockHandle, error) {
	if err := w.fsWritable.Write(b); err != nil {
		return block.BlockHandle{}, err
	}

	bh := block.BlockHandle{
		Offset: w.offset,
		Length: uint64(len(b)),
	}

	w.offset += uint64(len(b))
	return bh, nil
}

func (w *layoutWriter) Abort() {
	w.fsWritable.Abort()
}

func (w *layoutWriter) Finish() error {
	return w.fsWritable.Finish()
}

func NewLayoutWriter(w go_fs.Writable) ILayoutWriter {
	return &layoutWriter{
		fsWritable: w,
	}
}
