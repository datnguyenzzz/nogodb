package go_sstable

import (
	go_fs "github.com/datnguyenzzz/nogodb/lib/go-fs"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/options"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/row_block"
)

// NewSingularIterator returns an iterator for the singular keys in the SSTable
func NewSingularIterator(r go_fs.Readable, opts *options.IteratorOpts) IIterator {
	return row_block.NewSecondLevelIterator(r, opts)
}
