package iterators

import (
	"fmt"
	"sync"

	"github.com/datnguyenzzz/nogodb/lib/go-bytesbufferpool/predictable_size"
	go_fs "github.com/datnguyenzzz/nogodb/lib/go-fs"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	block_common "github.com/datnguyenzzz/nogodb/lib/go-sstable/common/block"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/options"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/row_block"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/storage"
	"go.uber.org/zap"
)

// DataBlockIterator reads the 2nd-level index block and creates and
// initializes an iterator over the 1st-level index, by which subsequently
// iterate over the datablock within the requested boundary [lower_bound, upper_bound]
type DataBlockIterator struct {
	bpool *predictable_size.PredictablePool
	// filterBH
	filterBH *block_common.BlockHandle

	// secondLevelIndexIter iterator through the 2nd level index block
	secondLevelIndexBH   *block_common.BlockHandle
	secondLevelIndexIter common.InternalIterator
	blockReader          row_block.IBlockReader
}

var dataBlockIteratorPool = sync.Pool{
	New: func() interface{} {
		return &DataBlockIterator{
			bpool: predictable_size.NewPredictablePool(),
		}
	},
}

func (i *DataBlockIterator) SeekGTE(key []byte) *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (i *DataBlockIterator) SeekLT(key []byte) *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (i *DataBlockIterator) First() *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (i *DataBlockIterator) Last() *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (i *DataBlockIterator) Next() *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (i *DataBlockIterator) Prev() *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (i *DataBlockIterator) Close() error {
	err := i.secondLevelIndexIter.Close()
	dataBlockIteratorPool.Put(i)
	return err
}

func (i *DataBlockIterator) readMetaIndexBlock(footer *row_block.Footer) error {
	// TODO (medium - dat.ngthanh): Should we cache the metaIndex block ?
	// Read and decode the meta index block
	metaIndexBuf, err := i.blockReader.Read(footer.GetMetaIndex(), block_common.BlockKindMetaIntex)
	if err != nil {
		zap.L().Error("failed to read metaIndexBlock", zap.Error(err))
		return err
	}
	blkIter := row_block.NewBlockIterator(i.bpool, common.NewComparer(), metaIndexBuf.ToByte())
	for iter := blkIter.First(); iter != nil; iter = blkIter.Next() {
		val := iter.Value()
		bh := &block_common.BlockHandle{}
		if sz := bh.DecodeFrom(val); sz != len(val) {
			zap.L().Error("failed to decode block, corrupted size", zap.Any("block", i))
			return fmt.Errorf("failed to decode block, corrupted size. %w", common.InternalServerError)
		}

		switch iter.K.ReadMetaIndexKey() {
		case block_common.BlockKindIndex:
			i.secondLevelIndexBH = bh
		case block_common.BlockKindFilter:
			i.filterBH = bh
		default:
		}
	}

	return nil
}

func (i *DataBlockIterator) init2ndLevelIndexBlockIterator() error {
	// TODO (medium - dat.ngthanh): Should we cache the 2nd level index block ?
	secondLevelIndexBuf, err := i.blockReader.Read(i.secondLevelIndexBH, block_common.BlockKindIndex)
	if err != nil {
		zap.L().Error("failed to read secondLevelIndexBlock", zap.Error(err))
		return err
	}
	i.secondLevelIndexIter = row_block.NewBlockIterator(i.bpool, common.NewComparer(), secondLevelIndexBuf.ToByte())
	return nil
}

func NewDataBlockIterator(fr go_fs.Readable, opts *options.IteratorOpts) (*DataBlockIterator, error) {
	iter := dataBlockIteratorPool.Get().(*DataBlockIterator)
	var err error
	var footer *row_block.Footer
	var layoutReader storage.ILayoutReader
	defer func() {
		if err != nil {
			_ = layoutReader.Close()
			_ = iter.Close()
		}
	}()

	layoutReader = storage.NewLayoutReader(fr)
	fullSize := fr.Size()
	footer, err = row_block.ReadFooter(layoutReader, fullSize)
	if err != nil {
		return nil, err
	}

	if iter.blockReader == nil {
		iter.blockReader = &row_block.RowBlockReader{}
	}
	iter.blockReader.Init(iter.bpool, layoutReader)

	// read meta index
	if err = iter.readMetaIndexBlock(footer); err != nil {
		return nil, err
	}

	// init 2nd level index iterator
	if err = iter.init2ndLevelIndexBlockIterator(); err != nil {
		return nil, err
	}

	return iter, nil
}

var _ common.InternalIterator = (*DataBlockIterator)(nil)
