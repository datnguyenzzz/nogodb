package iterators

import (
	"errors"
	"fmt"
	"sync"

	"github.com/datnguyenzzz/nogodb/lib/go-bytesbufferpool/predictable_size"
	go_fs "github.com/datnguyenzzz/nogodb/lib/go-fs"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	block_common "github.com/datnguyenzzz/nogodb/lib/go-sstable/common/block"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/filter"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/options"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/row_block"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/storage"
	"go.uber.org/zap"
)

// indexedIterator iter is the iteration of the data, received from the given index
type indexedIterator struct {
	common.InternalIterator
	cmp       common.IComparer
	reader    row_block.IBlockReader
	bpool     *predictable_size.PredictablePool
	index     *common.InternalKV
	blockKind block_common.BlockKind
}

func newIndexedIterator(
	cmp common.IComparer,
	reader row_block.IBlockReader,
	bpool *predictable_size.PredictablePool,
	kind block_common.BlockKind,
) *indexedIterator {
	return &indexedIterator{
		cmp:              cmp,
		reader:           reader,
		bpool:            bpool,
		index:            nil,
		InternalIterator: nil,
		blockKind:        kind,
	}
}

func (ii *indexedIterator) SetIndexAndLoad(index *common.InternalKV) error {
	if ii.index != nil && ii.index.Compare(ii.cmp, index) != 0 {
		if err := ii.Close(); err != nil {
			return err
		}
	}

	ii.index = index
	return ii.loadedIter()
}

// loadedIter ensure the iter is loaded from the current index.
// Usually it'd be called after the index updated
func (ii *indexedIterator) loadedIter() error {
	if ii.index == nil {
		return fmt.Errorf("%w: index is nil, can not load iterator", common.InternalServerError)
	}
	if ii.InternalIterator != nil {
		// iter is already loaded from the given current index
		return nil
	}

	bh := &block_common.BlockHandle{}
	if n := bh.DecodeFrom(ii.index.V.Value()); n <= 0 {
		zap.L().Error("failed to fully decode the index")
		return fmt.Errorf("%w: failed to fully decode the index", common.InternalServerError)
	}
	block, err := ii.reader.ReadThroughCache(bh, ii.blockKind)
	if err != nil {
		zap.L().Error("failed to read the block", zap.Error(err))
		return err
	}
	ii.InternalIterator = row_block.NewBlockIterator(ii.bpool, ii.cmp, block)
	return nil
}

func (ii *indexedIterator) Close() error {
	if ii.InternalIterator != nil {
		if err := ii.InternalIterator.Close(); err != nil {
			return err
		}
	}

	ii.InternalIterator = nil
	if ii.index != nil {
		ii.index.V.Release()
	}
	ii.index = nil
	return nil
}

// DataIterator reads the 2nd-level index block and creates and
// initializes an iterator over the 1st-level index, by which subsequently
// iterate over the datablock within the requested boundary [lower_bound, upper_bound]
//
// The index (1st + 2nd levels) have the last index, but not the first
// For example: (i+1-th key > index key ≥ i-th key )
// Data:    1 2 3     4 5 6    7 8 9    10 11
// L1(k,v):     [3,0]    [6,3]     [9,6]     [12,9]
// L2  :                    [6,0]                 [12,2]
//
// Todo (med) - Optimisation: Make all functions of DataIterator to be monotonic
type DataIterator struct {
	cmp         common.IComparer
	blockReader row_block.IBlockReader

	bpool *predictable_size.PredictablePool
	// filter
	filterBH *block_common.BlockHandle
	filter   filter.IRead

	// the 2nd level index iterator do
	secondLevelIndexBH   *block_common.BlockHandle
	secondLevelIndexIter common.InternalIterator

	// Iterators + indexes

	// Index: secondLevelIndex , Iter: firstLevelIndexIter
	firstLevelIndexedIter *indexedIterator
	// Index: firstLevelIndex , Iter: dataBlockIter
	dataIndexedIter *indexedIterator
}

var dataBlockIteratorPool = sync.Pool{
	New: func() interface{} {
		return &DataIterator{}
	},
}

// SeekPrefixGTE the prefix is only used for checking with the bloom filter of the SSTable.
// but not used later while iterating. Hence, we can use the existing iterator position
// it did not fail bloom filter matching.
// TODO [P0] - Untested code
func (i *DataIterator) SeekPrefixGTE(prefix, key []byte) *common.InternalKV {
	if !i.filter.MayContain(prefix) {
		// don't invalidate the indexes and data block, the other iterator might still read it
		return nil
	}
	return i.SeekGTE(key)
}

// SeekGTE
// TODO [P0] - Untested code
func (i *DataIterator) SeekGTE(key []byte) *common.InternalKV {
	// Important notes:
	//  - Ensure the data (lazyValue) is released properly once the block is no longer used
	new2ndIndex := i.secondLevelIndexIter.SeekGTE(key)
	if new2ndIndex == nil {
		zap.L().Warn("the target key is not in the block")
		return nil
	}

	err := i.firstLevelIndexedIter.SetIndexAndLoad(new2ndIndex)
	if err != nil {
		zap.L().Error("failed to load the first level index", zap.Error(err))
		return nil
	}

	new1stIndex := i.firstLevelIndexedIter.SeekGTE(key)
	if new1stIndex == nil {
		panic("impossible, the 1st must be found")
	}

	err = i.dataIndexedIter.SetIndexAndLoad(new1stIndex)
	if err != nil {
		zap.L().Error("failed to load the data block", zap.Error(err))
		return nil
	}

	return i.dataIndexedIter.SeekGTE(key)
}

func (i *DataIterator) SeekLTE(key []byte) *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (i *DataIterator) First() *common.InternalKV {
	panic("implement me")
	// TODO review later
	//if err := i.first2ndLevelIndex(); err != nil {
	//	return nil
	//}
	//
	//// move the 1st-level index to first position
	//if err := i.first1stLevelIndex(); err != nil {
	//	return nil
	//}
	//
	//return i.dataBlockIter.First()
}

// TODO review later
//// first2ndLevelIndex move the 2nd-level index to its first position
//// and rebuild the i.firstIndexIter
//func (i *DataIterator) first2ndLevelIndex() error {
//	// move the 2nd-level index to the first position
//	first2ndIndex := i.secondLevelIndexIter.First()
//	if first2ndIndex == nil {
//		zap.L().Error("failed to move to the first position of 2nd-level index")
//		return fmt.Errorf("failed to move to the first position of 2nd-level index")
//	}
//	if first2ndIndex != i.secondLevelIndex {
//		i.invalidate1stLevelIndexIter()
//		i.invalidateDataIter()
//	}
//
//	i.secondLevelIndex = first2ndIndex
//	return i.load1stIndexBlockIter()
//}

// TODO review later
// first1stLevelIndex move the 1st-level index to its first position
// and rebuild the i.dataBlockIter
//func (i *DataIterator) first1stLevelIndex() error {
//	first1stIndex := i.firstLevelIndexIter.First()
//	if first1stIndex == nil {
//		zap.L().Error("failed to move to the first position of 1st-level index")
//		return fmt.Errorf("failed to move to the first position of 1st-level index")
//	}
//	if first1stIndex != i.firstLevelIndex {
//		i.invalidateDataIter()
//	}
//
//	i.firstLevelIndex = first1stIndex
//	return i.loadDataBlockIter()
//}

func (i *DataIterator) Last() *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (i *DataIterator) Next() *common.InternalKV {
	panic("implement me")
	// TODO review later
	//if i.firstLevelIndexIter == nil || i.dataBlockIter == nil {
	//	zap.L().Warn("The iterator has not been initialized yet, moved to the first position")
	//	_ = i.First()
	//}
	//
	//nextKv := i.dataBlockIter.Next()
	//if nextKv != nil {
	//	return nextKv
	//}
	//// data block is already at the end, move the 1st-level index next
	//nextKv, err := i.next1stLevelIndex()
	//if err != nil {
	//	zap.L().Error("failed to move to the next 1st-level index", zap.Error(err))
	//	return nil
	//}
	//
	//if nextKv != nil {
	//	return nextKv
	//}
	//
	//// the 1st-index is already at the end, move the 2nd-level index next
	//nextKv, err = i.next2ndLevelIndex()
	//if err != nil {
	//	zap.L().Error("failed to move to the next 2nd-level index", zap.Error(err))
	//	return nil
	//}
	//
	//return nextKv
}

// TODO review later
//// next1stLevelIndex move the current 1st level index next, and return the first() cursor
//// returns nil if the 1st-level also at the end of the block
//func (i *DataIterator) next1stLevelIndex() (*common.InternalKV, error) {
//	i.invalidateDataIter()
//
//	next1stLvlIndex := i.firstLevelIndexIter.Next()
//	if next1stLvlIndex == nil {
//		return nil, nil
//	}
//
//	i.firstLevelIndex.V.Release()
//	i.firstLevelIndex = next1stLvlIndex
//
//	err := i.loadDataBlockIter()
//	if err != nil {
//		zap.L().Error("failed to load data block", zap.Error(err))
//		return nil, err
//	}
//
//	return i.dataBlockIter.First(), nil
//}

// TODO review later
//// next2ndLevelIndex move the current 2nd level index next, and return the first() cursor
//// returns nil if the 2nd-level also at the end of the block
//func (i *DataIterator) next2ndLevelIndex() (*common.InternalKV, error) {
//	// invalidate the current 1st index
//	i.invalidate1stLevelIndexIter()
//	i.invalidateDataIter()
//
//	next2ndLvlIndex := i.secondLevelIndexIter.Next()
//	if next2ndLvlIndex == nil {
//		return nil, nil
//	}
//
//	if err := i.load1stIndexBlockIter(); err != nil {
//		return nil, err
//	}
//
//	i.firstLevelIndex = i.firstLevelIndexIter.First()
//
//	if err := i.loadDataBlockIter(); err != nil {
//		return nil, err
//	}
//
//	return i.dataBlockIter.First(), nil
//}

func (i *DataIterator) Prev() *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (i *DataIterator) Close() error {
	var err error
	if i.secondLevelIndexIter != nil {
		err = errors.Join(i.secondLevelIndexIter.Close())
	}
	if i.firstLevelIndexedIter != nil {
		err = errors.Join(i.firstLevelIndexedIter.Close())
	}
	if i.dataIndexedIter != nil {
		err = errors.Join(i.dataIndexedIter.Close())
	}
	i.blockReader.Release()
	dataBlockIteratorPool.Put(i)
	i.secondLevelIndexIter = nil
	i.firstLevelIndexedIter = nil
	i.dataIndexedIter = nil
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
		if sz := bh.DecodeFrom(val); sz <= 0 {
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

func (i *DataIterator) init2ndLevelIndexBlockIterator() error {
	if i.secondLevelIndexBH == nil {
		zap.L().Error("the secondLevelIndex block handle is nil")
		return fmt.Errorf("%w: the secondLevelIndex block handle is nil", common.InternalServerError)
	}
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
	filterBlock, err := i.blockReader.ReadThroughCache(i.filterBH, block_common.BlockKindFilter)
	if err != nil {
		zap.L().Error("failed to read filter", zap.Error(err))
		return err
	}

	i.filter = filter.NewFilterReader(filter.BloomFilter, filterBlock.Value())
	return nil
}

func NewIterator(
	bpool *predictable_size.PredictablePool, // shared buffer pool across iterator
	fr go_fs.Readable,
	cmp common.IComparer,
	opts *options.IteratorOpts,
) (*DataIterator, error) {
	iter := dataBlockIteratorPool.Get().(*DataIterator)
	var err error
	var footer *row_block.Footer
	var layoutReader storage.ILayoutReader

	iter.bpool = bpool
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

	iter.blockReader.Init(iter.bpool, layoutReader, opts.CacheOpts)
	iter.firstLevelIndexedIter = newIndexedIterator(cmp, iter.blockReader, iter.bpool, block_common.BlockKindIndex)
	iter.dataIndexedIter = newIndexedIterator(cmp, iter.blockReader, iter.bpool, block_common.BlockKindData)

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
