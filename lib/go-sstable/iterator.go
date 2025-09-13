package go_sstable

import (
	go_fs "github.com/datnguyenzzz/nogodb/lib/go-fs"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/iterators"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/options"
)

// Entry point for initializing every supported iterator

// NewSingularIterator returns an iterator for the singular keys in the SSTable
func NewSingularIterator(r go_fs.Readable, opts *options.IteratorOpts) (IIterator, error) {
	return iterators.NewIterator(r, opts)
}
