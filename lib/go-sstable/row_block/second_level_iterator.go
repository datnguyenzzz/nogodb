package row_block

import (
	"sync"

	go_fs "github.com/datnguyenzzz/nogodb/lib/go-fs"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/options"
)

// SecondLevelIterator reads the 2nd-level index block and creates and
// initializes an iterator over the 1st-level index, by which subsequently
// iterate over the datablock within the requested boundary [lower_bound, upper_bound]
type SecondLevelIterator struct {
}

var secondLevelIteratorPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return &SecondLevelIterator{}
	},
}

func (t *SecondLevelIterator) SeekGTE(key []byte) *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (t *SecondLevelIterator) SeekLT(key []byte) *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (t *SecondLevelIterator) First() *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (t *SecondLevelIterator) Last() *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (t *SecondLevelIterator) Next() *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (t *SecondLevelIterator) Prev() *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (t *SecondLevelIterator) Close() error {
	//TODO implement me
	panic("implement me")
}

func NewSecondLevelIterator(r go_fs.Readable, opts *options.IteratorOpts) *SecondLevelIterator {
	iter := secondLevelIteratorPool.Get().(*SecondLevelIterator)

	// TODO
	//  1. Init block reader
	//  2. Read metaindex blockHandle from footer
	//  3. Read 2nd level index / filter block from metaindex block

	return iter
}

var _ common.InternalIterator = (*SecondLevelIterator)(nil)
