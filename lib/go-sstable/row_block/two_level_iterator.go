package row_block

import (
	go_fs "github.com/datnguyenzzz/nogodb/lib/go-fs"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/storage"
)

type TwoLevelIterator struct {
	storageReader storage.ILayoutReader
}

func (t *TwoLevelIterator) SeekGTE(key []byte) *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (t *TwoLevelIterator) SeekLT(key []byte) *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (t *TwoLevelIterator) First() *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (t *TwoLevelIterator) Last() *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (t *TwoLevelIterator) Next() *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (t *TwoLevelIterator) Prev() *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (t *TwoLevelIterator) Close() error {
	//TODO implement me
	panic("implement me")
}

// NewTwoLevelIterator reads the 2nd-level index block and creates and
// initializes a two-level iterator over a sstable's data block
func NewTwoLevelIterator(r go_fs.Readable) *TwoLevelIterator {
	// TODO need to research what is needed here
	return &TwoLevelIterator{
		storageReader: storage.NewLayoutReader(r),
	}
}

var _ common.InternalIterator = (*TwoLevelIterator)(nil)
