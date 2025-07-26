package row_block

import (
	"sync"

	go_fs "github.com/datnguyenzzz/nogodb/lib/go-fs"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/block"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/options"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/storage"
)

// SecondLevelIterator reads the 2nd-level index block and creates and
// initializes an iterator over the 1st-level index, by which subsequently
// iterate over the datablock within the requested boundary [lower_bound, upper_bound]
type SecondLevelIterator struct {
	rowBlockReader *RowBlockReader
	metaIndexBH    *block.BlockHandle
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

func NewSecondLevelIterator(fr go_fs.Readable, opts *options.IteratorOpts) (*SecondLevelIterator, error) {
	iter := secondLevelIteratorPool.Get().(*SecondLevelIterator)

	// TODO
	//  3. Read 2nd level index / filter block from metaindex block

	reader := storage.NewLayoutReader(fr)
	defer func() {
		_ = reader.Close()
	}()

	fullSize := fr.Size()
	footer, err := ReadFooter(reader, fullSize)
	if err != nil {
		return nil, err
	}

	if iter.rowBlockReader == nil {
		iter.rowBlockReader = &RowBlockReader{}
	}

	iter.rowBlockReader.Init(reader)
	iter.metaIndexBH = &footer.metaIndexBH

	return iter, nil
}

var _ common.InternalIterator = (*SecondLevelIterator)(nil)
