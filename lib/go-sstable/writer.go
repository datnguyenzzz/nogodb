package go_sstable

import (
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/base"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/row_block"
)

type Writer struct {
	opts *base.WriteOpt
	rw   base.RawWriter
}

func (w *Writer) Set(key, value []byte) error {
	//TODO implement me
	panic("implement me")
}

func (w *Writer) Close() error {
	//TODO implement me
	panic("implement me")
}

func NewWriter(writable base.Writable, opts ...WriteOptFn) *Writer {
	w := &Writer{
		opts: DefaultWriteOpt,
	}

	for _, o := range opts {
		o(w)
	}

	// Only support row-based format for now
	if w.opts.TableFormat != base.RowBlockedBaseTableFormat {
		panic("invalid table format")
	}

	w.rw = row_block.NewRowBlockWriter(writable, *w.opts)

	return w
}

var _ IWriter = (*Writer)(nil)
