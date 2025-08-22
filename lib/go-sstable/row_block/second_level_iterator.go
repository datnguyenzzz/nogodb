package row_block

import (
	"sync"

	"github.com/datnguyenzzz/nogodb/lib/go-bytesbufferpool/predictable_size"
	go_fs "github.com/datnguyenzzz/nogodb/lib/go-fs"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/block"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/options"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/storage"
	"go.uber.org/zap"
)

// SecondLevelIterator reads the 2nd-level index block and creates and
// initializes an iterator over the 1st-level index, by which subsequently
// iterate over the datablock within the requested boundary [lower_bound, upper_bound]
type SecondLevelIterator struct {
	bpool          *predictable_size.PredictablePool
	rowBlockReader *RowBlockReader
}

var secondLevelIteratorPool = sync.Pool{
	New: func() interface{} {
		return &SecondLevelIterator{
			bpool: predictable_size.NewPredictablePool(),
		}
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

	// Read and decode the meta index block
	metaIndexBuf, err := iter.rowBlockReader.Read(iter.bpool, &footer.metaIndexBH, block.BlockKindMetaIntex)
	if err != nil {
		zap.L().Error("failed to read metaIndexBlock", zap.Error(err))
		return nil, err
	}
	// TODO (dat.ngthanh): Use the row blocker iter to read the block buffer DataBlockIterator
	blkIter := NewDataBlockIterator(common.NewComparer(), metaIndexBuf.ToByte())

	return iter, nil
}

var _ common.InternalIterator = (*SecondLevelIterator)(nil)
