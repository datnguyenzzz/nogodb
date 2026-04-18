package go_sstable

import (
	go_fs "github.com/datnguyenzzz/nogodb/lib/go-fs"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/block/col_block"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/block/row_block"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/options"
)

type Writer struct {
	datablockOpts *options.BlockWriteOpt
	rw            common.InternalWriter
}

func (w *Writer) Set(key, value []byte) error {
	return w.rw.Add(common.MakeKey(key, 0, common.KeyKindSet), value)
}

func (w *Writer) Delete(key []byte) error {
	return w.rw.Add(common.MakeKey(key, 0, common.KeyKindDelete), nil)
}

// Close finishes writing the table and closes the underlying file that the
// table was written to.
func (w *Writer) Close() error {
	return w.rw.Close()
}

func NewWriter(writable go_fs.Writable, tableVersion common.TableVersion, opts ...WriteOptFn) *Writer {
	w := &Writer{
		datablockOpts: DefaultWriteOpt,
	}

	for _, o := range opts {
		o(w)
	}

	if tableVersion == common.TableV1 {
		w.rw = row_block.NewRowBlockWriter(writable, *w.datablockOpts, tableVersion)
	} else {
		w.rw = col_block.NewColBlockWriter(writable, *w.datablockOpts, tableVersion)
	}

	return w
}

var _ IWriter = (*Writer)(nil)
