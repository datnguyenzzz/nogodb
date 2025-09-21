package iterators

import (
	"errors"
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

// DataIterator reads the 2nd-level index block and creates and
// initializes an iterator over the 1st-level index, by which subsequently
// iterate over the datablock within the requested boundary [lower_bound, upper_bound]
type DataIterator struct {
	cmp         common.IComparer
	blockReader row_block.IBlockReader

	bpool *predictable_size.PredictablePool
	// filter
	filterBH *block_common.BlockHandle
	filter   *common.InternalLazyValue

	// block handlers
	secondLevelIndexBH *block_common.BlockHandle

	// indexes
	secondLevelIndex *common.InternalKV
	firstLevelIndex  *common.InternalKV

	// Iterators
	secondLevelIndexIter common.InternalIterator
	firstLevelIndexIter  common.InternalIterator
	dataBlockIter        common.InternalIterator
}

var dataBlockIteratorPool = sync.Pool{
	New: func() interface{} {
		return &DataIterator{
			bpool: predictable_size.NewPredictablePool(),
		}
	},
}

func (i *DataIterator) SeekPrefixGTE(prefix, key []byte) *common.InternalIterator {
	//TODO implement me
	panic("implement me")
}

// SeekGTE
// TODO(high) - Untested
// TODO(med) - The code looks repetitive, need to refactor this
func (i *DataIterator) SeekGTE(key []byte) *common.InternalKV {
	// Important notes:
	//  - Ensure the data (lazyValue) is released properly once the block is no longer used

	// 1. Seek LTE of the 2nd level index to get index key ≤ search key
	if err := i.seekGTE2ndLevelIndex(key); err != nil {
		return nil
	}

	firstLvlIndexBH := &block_common.BlockHandle{}
	secondLvlIndexVal := i.secondLevelIndex.V.Value()
	if n := firstLvlIndexBH.DecodeFrom(secondLvlIndexVal); n != len(secondLvlIndexVal) {
		zap.L().Error("failed to fully decode the 2nd-level index")
		return nil
	}
	firstLvlIndexBlock, err := i.blockReader.ReadThroughCache(firstLvlIndexBH, block_common.BlockKindIndex)
	if err != nil {
		zap.L().Error("failed to read the first-level index block", zap.Error(err))
		return nil
	}
	i.firstLevelIndexIter = row_block.NewBlockIterator(i.bpool, i.cmp, firstLvlIndexBlock)

	// 2. Seek LTE of the 1st level index to get index key ≤ search key
	if err := i.seek1stLevelIndex(key); err != nil {
		return nil
	}
	dataBlockBh := &block_common.BlockHandle{}
	firstLvlIndexVal := i.firstLevelIndex.V.Value()
	if n := dataBlockBh.DecodeFrom(firstLvlIndexVal); n != len(firstLvlIndexVal) {
		zap.L().Error("failed to fully decode the first-level index")
		return nil
	}
	dataBlock, err := i.blockReader.ReadThroughCache(dataBlockBh, block_common.BlockKindData)
	if err != nil {
		zap.L().Error("failed to read the data block", zap.Error(err))
		return nil
	}

	i.dataBlockIter = row_block.NewBlockIterator(i.bpool, i.cmp, dataBlock)

	// 3. seek data block to get key ≥ search key
	return i.dataBlockIter.SeekGTE(key)
}

func (i *DataIterator) seekGTE2ndLevelIndex(key []byte) error {
	newIndex := i.secondLevelIndexIter.SeekLTE(key)
	if newIndex == nil {
		return fmt.Errorf("failed to seek 2nd-level index")
	}

	if newIndex == i.secondLevelIndex {
		return nil
	}

	i.secondLevelIndex = newIndex
	if err := i.firstLevelIndexIter.Close(); err != nil {
		zap.L().Error("failed to close the current first-level index", zap.Error(err))
		return err
	}
	if err := i.dataBlockIter.Close(); err != nil {
		zap.L().Error("failed to close the current data block", zap.Error(err))
		return err
	}

	i.dataBlockIter = nil
	i.firstLevelIndexIter = nil
	i.firstLevelIndex = nil
	return nil
}

func (i *DataIterator) seek1stLevelIndex(key []byte) error {
	newIndex := i.firstLevelIndexIter.SeekLTE(key)
	if newIndex == nil {
		return fmt.Errorf("failed to seek 1st-level index")
	}

	if newIndex == i.firstLevelIndex {
		return nil
	}

	i.firstLevelIndex = newIndex
	if err := i.dataBlockIter.Close(); err != nil {
		zap.L().Error("failed to close the current data block", zap.Error(err))
		return err
	}

	i.dataBlockIter = nil
	return nil
}

func (i *DataIterator) SeekLTE(key []byte) *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (i *DataIterator) First() *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (i *DataIterator) Last() *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (i *DataIterator) Next() *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (i *DataIterator) Prev() *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (i *DataIterator) Close() error {
	var err error
	err = errors.Join(
		i.secondLevelIndexIter.Close(),
		i.firstLevelIndexIter.Close(),
		i.dataBlockIter.Close(),
	)
	i.blockReader.Release()
	dataBlockIteratorPool.Put(i)
	i.secondLevelIndexIter = nil
	i.firstLevelIndexIter = nil
	i.dataBlockIter = nil
	i.secondLevelIndex = nil
	i.firstLevelIndex = nil
	return err
}

func (i *DataIterator) IsClosed() bool {
	return i.secondLevelIndexIter == nil || i.secondLevelIndexIter.IsClosed()
}

func (i *DataIterator) readMetaIndexBlock(footer *row_block.Footer) error {
	// TODO (medium - dat.ngthanh): Should we cache the metaIndex block ?
	// Read and decode the meta index block
	metaIndexBuf, err := i.blockReader.Read(footer.GetMetaIndex(), block_common.BlockKindMetaIntex)
	if err != nil {
		zap.L().Error("failed to read metaIndexBlock", zap.Error(err))
		return err
	}
	blkIter := row_block.NewBlockIterator(i.bpool, common.NewComparer(), metaIndexBuf)
	for iter := blkIter.First(); iter != nil; iter = blkIter.Next() {
		val := iter.V.Value()
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

// init1stLevelIndexBlockIterator init a 1st-level index iterator from the given 2nd-level index
// The newly created 1st-level index iterator is unpositioned, and also closed the dataBlockIter
func (i *DataIterator) init1stLevelIndexBlockIterator(secondLvlIndex *common.InternalKV) error {
	// Close the current iterators to move the new sections
	if err := errors.Join(
		i.firstLevelIndexIter.Close(),
		i.dataBlockIter.Close(),
	); err != nil {
		return err
	}

	firstLevelBh := &block_common.BlockHandle{}
	n := firstLevelBh.DecodeFrom(secondLvlIndex.V.Value())
	if n != len(secondLvlIndex.V.Value()) {
		return fmt.Errorf("failed to decode the 1st level index block")
	}

	firstLevelIndex, err := i.blockReader.Read(firstLevelBh, block_common.BlockKindIndex)
	if err != nil {
		zap.L().Error("failed to read 1st level index block", zap.Error(err))
		return err
	}

	i.firstLevelIndexIter = row_block.NewBlockIterator(i.bpool, common.NewComparer(), firstLevelIndex)
	return nil
}

func (i *DataIterator) init2ndLevelIndexBlockIterator() error {
	// TODO (low - dat.ngthanh): Should we cache the 2nd level index block ?
	secondLevelIndexBuf, err := i.blockReader.Read(i.secondLevelIndexBH, block_common.BlockKindIndex)
	if err != nil {
		zap.L().Error("failed to read secondLevelIndexBlock", zap.Error(err))
		return err
	}
	i.secondLevelIndexIter = row_block.NewBlockIterator(i.bpool, i.cmp, secondLevelIndexBuf)
	return nil
}

func (i *DataIterator) readFilter() error {
	var err error
	i.filter, err = i.blockReader.ReadThroughCache(i.filterBH, block_common.BlockKindFilter)
	if err != nil {
		zap.L().Error("failed to read filter", zap.Error(err))
		return err
	}
	return nil
}

func NewIterator(fr go_fs.Readable, cmp common.IComparer, opts *options.IteratorOpts) (*DataIterator, error) {
	iter := dataBlockIteratorPool.Get().(*DataIterator)
	var err error
	var footer *row_block.Footer
	var layoutReader storage.ILayoutReader
	defer func() {
		if err != nil {
			_ = layoutReader.Close()
			_ = iter.Close()
		}
	}()

	iter.cmp = cmp
	layoutReader = storage.NewLayoutReader(fr)
	fullSize := fr.Size()
	footer, err = row_block.ReadFooter(layoutReader, fullSize)
	if err != nil {
		return nil, err
	}

	if iter.blockReader == nil {
		iter.blockReader = &row_block.RowBlockReader{}
	}
	iter.blockReader.Init(iter.bpool, layoutReader, &opts.CacheOpts)

	if err = iter.readMetaIndexBlock(footer); err != nil {
		return nil, err
	}

	if err = iter.init2ndLevelIndexBlockIterator(); err != nil {
		return nil, err
	}

	if err = iter.readFilter(); err != nil {
		return nil, err
	}

	return iter, nil
}

var _ common.InternalIterator = (*DataIterator)(nil)
