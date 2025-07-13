package go_sstable

import (
	go_fs "github.com/datnguyenzzz/nogodb/lib/go-fs"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
)

type Iterator struct {
	fsReadable go_fs.Readable
}

func (i Iterator) SeekGTE(key []byte) *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (i Iterator) SeekLT(key []byte) *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (i Iterator) First() *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (i Iterator) Last() *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (i Iterator) Next() *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (i Iterator) Prev() *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (i Iterator) Close() error {
	//TODO implement me
	panic("implement me")
}

// NewSingularIterator returns an iterator for the singular keys in the SSTable
func NewSingularIterator(r go_fs.Readable) *Iterator {
	// TODO: Research what is needed here:
	panic("implement me")
}

var _ IIterator = (*Iterator)(nil)
