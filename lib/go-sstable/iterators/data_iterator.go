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

// DataIterator reads the 2nd-level index block and creates and
// initializes an iterator over the 1st-level index, by which subsequently
// iterate over the datablock within the requested boundary [lower_bound, upper_bound]
//
// Todo (med) - Optimisation: Make all functions of DataIterator to be monotonic
type DataIterator struct {
	cmp         common.IComparer
	blockReader row_block.IBlockReader

	bpool *predictable_size.PredictablePool
	// filter
	filterBH *block_common.BlockHandle
	filter   filter.IRead

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

// SeekPrefixGTE the prefix is only used for checking with the bloom filter of the SSTable.
// but not used later while iterating. Hence, we can use the existing iterator position
// it did not fail bloom filter matching.
func (i *DataIterator) SeekPrefixGTE(prefix, key []byte) *common.InternalKV {
	if !i.filter.MayContain(prefix) {
		// don't invalidate the indexes and data block, the other iterator might still read it
		return nil
	}
	return i.SeekGTE(key)
}

// TODO -- Need to re-think on the SeekGTE logic
// The index (1st + 2nd levels) have the last index, but not the first
// For example: (i+1-th key > index key ≥ i-th key )
// Data:  1 2 3 4 5 6 7 8 9 10 11
// L1  :       3   5   7   9     [12]
// L2  :           5       9     [12]

// SeekGTE
// TODO(high) - Untested - to write the integration tests, involving write then read
// TODO(med) - The code looks repetitive, need to refactor this
func (i *DataIterator) SeekGTE(key []byte) *common.InternalKV {
	// Important notes:
	//  - Ensure the data (lazyValue) is released properly once the block is no longer used

	// 1. Seek LTE of the 2nd level index to get index key ≤ search key
	if err := i.seekLTE2ndLevelIndex(key); err != nil {
		return nil
	}

	if err := i.load1stIndexBlockIter(); err != nil {
		zap.L().Error("Error loading 1st index block iterator", zap.Error(err))
		return nil
	}

	// 2. Seek LTE of the 1st level index to get index key ≤ search key
	if err := i.seekLTE1stLevelIndex(key); err != nil {
		return nil
	}

	if err := i.loadDataBlockIter(); err != nil {
		zap.L().Error("Error loading data block iterator", zap.Error(err))
		return nil
	}

	// 3. seek data block to get key ≥ search key
	return i.dataBlockIter.SeekGTE(key)
}

func (i *DataIterator) seekLTE2ndLevelIndex(key []byte) error {
	newIndex := i.secondLevelIndexIter.SeekLTE(key)
	if newIndex == nil {
		return fmt.Errorf("failed to seek 2nd-level index")
	}

	if newIndex == i.secondLevelIndex {
		return nil
	}

	i.secondLevelIndex = newIndex
	i.invalidate1stLevelIndexIter()
	i.invalidateDataIter()
	return nil
}

func (i *DataIterator) seekLTE1stLevelIndex(key []byte) error {
	newIndex := i.firstLevelIndexIter.SeekLTE(key)
	if newIndex == nil {
		return fmt.Errorf("failed to seek 1st-level index")
	}

	if newIndex == i.firstLevelIndex {
		return nil
	}

	i.firstLevelIndex = newIndex
	i.invalidateDataIter()
	return nil
}

func (i *DataIterator) SeekLTE(key []byte) *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (i *DataIterator) First() *common.InternalKV {
	if err := i.first2ndLevelIndex(); err != nil {
		return nil
	}

	// move the 1st-level index to first position
	if err := i.first1stLevelIndex(); err != nil {
		return nil
	}

	return i.dataBlockIter.First()
}

// first2ndLevelIndex move the 2nd-level index to its first position
// and rebuild the i.firstIndexIter
func (i *DataIterator) first2ndLevelIndex() error {
	// move the 2nd-level index to the first position
	first2ndIndex := i.secondLevelIndexIter.First()
	if first2ndIndex == nil {
		zap.L().Error("failed to move to the first position of 2nd-level index")
		return fmt.Errorf("failed to move to the first position of 2nd-level index")
	}
	if first2ndIndex != i.secondLevelIndex {
		i.invalidate1stLevelIndexIter()
		i.invalidateDataIter()
	}

	i.secondLevelIndex = first2ndIndex
	return i.load1stIndexBlockIter()
}

// first1stLevelIndex move the 1st-level index to its first position
// and rebuild the i.dataBlockIter
func (i *DataIterator) first1stLevelIndex() error {
	first1stIndex := i.firstLevelIndexIter.First()
	if first1stIndex == nil {
		zap.L().Error("failed to move to the first position of 1st-level index")
		return fmt.Errorf("failed to move to the first position of 1st-level index")
	}
	if first1stIndex != i.firstLevelIndex {
		i.invalidateDataIter()
	}

	i.firstLevelIndex = first1stIndex
	return i.loadDataBlockIter()
}

func (i *DataIterator) Last() *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (i *DataIterator) Next() *common.InternalKV {
	if i.firstLevelIndexIter == nil || i.dataBlockIter == nil {
		zap.L().Warn("The iterator has not been initialized yet, moved to the first position")
		_ = i.First()
	}

	nextKv := i.dataBlockIter.Next()
	if nextKv != nil {
		return nextKv
	}
	// data block is already at the end, move the 1st-level index next
	nextKv, err := i.next1stLevelIndex()
	if err != nil {
		zap.L().Error("failed to move to the next 1st-level index", zap.Error(err))
		return nil
	}

	if nextKv != nil {
		return nextKv
	}

	// the 1st-index is already at the end, move the 2nd-level index next
	nextKv, err = i.next2ndLevelIndex()
	if err != nil {
		zap.L().Error("failed to move to the next 2nd-level index", zap.Error(err))
		return nil
	}

	return nextKv
}

// next1stLevelIndex move the current 1st level index next, and return the first() cursor
// returns nil if the 1st-level also at the end of the block
func (i *DataIterator) next1stLevelIndex() (*common.InternalKV, error) {
	i.invalidateDataIter()

	next1stLvlIndex := i.firstLevelIndexIter.Next()
	if next1stLvlIndex == nil {
		return nil, nil
	}

	i.firstLevelIndex.V.Release()
	i.firstLevelIndex = next1stLvlIndex

	err := i.loadDataBlockIter()
	if err != nil {
		zap.L().Error("failed to load data block", zap.Error(err))
		return nil, err
	}

	return i.dataBlockIter.First(), nil
}

// next2ndLevelIndex move the current 2nd level index next, and return the first() cursor
// returns nil if the 2nd-level also at the end of the block
func (i *DataIterator) next2ndLevelIndex() (*common.InternalKV, error) {
	// invalidate the current 1st index
	i.invalidate1stLevelIndexIter()
	i.invalidateDataIter()

	next2ndLvlIndex := i.secondLevelIndexIter.Next()
	if next2ndLvlIndex == nil {
		return nil, nil
	}

	if err := i.load1stIndexBlockIter(); err != nil {
		return nil, err
	}

	i.firstLevelIndex = i.firstLevelIndexIter.First()

	if err := i.loadDataBlockIter(); err != nil {
		return nil, err
	}

	return i.dataBlockIter.First(), nil
}

func (i *DataIterator) Prev() *common.InternalKV {
	//TODO implement me
	panic("implement me")
}

func (i *DataIterator) Close() error {
	var err error
	if i.secondLevelIndexIter != nil {
		err = errors.Join(i.secondLevelIndexIter.Close())
	}
	if i.firstLevelIndexIter != nil {
		err = errors.Join(i.firstLevelIndexIter.Close())
	}
	if i.dataBlockIter != nil {
		err = errors.Join(i.dataBlockIter.Close())
	}
	i.blockReader.Release()
	dataBlockIteratorPool.Put(i)
	i.secondLevelIndexIter = nil
	i.firstLevelIndexIter = nil
	i.dataBlockIter = nil
	i.secondLevelIndex = nil
	i.firstLevelIndex.V.Release()
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
	if n <= 0 {
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
	filterBlock, err := i.blockReader.ReadThroughCache(i.filterBH, block_common.BlockKindFilter)
	if err != nil {
		zap.L().Error("failed to read filter", zap.Error(err))
		return err
	}

	i.filter = filter.NewFilterReader(filter.BloomFilter, filterBlock.Value())
	return nil
}

func (i *DataIterator) loadDataBlockIter() error {
	dataBlockBh := &block_common.BlockHandle{}
	firstLvlIndexVal := i.firstLevelIndex.V.Value()
	if n := dataBlockBh.DecodeFrom(firstLvlIndexVal); n <= 0 {
		zap.L().Error("failed to fully decode the first-level index")
		return fmt.Errorf("failed to fully decode the first-level index. %w", common.InternalServerError)
	}
	dataBlock, err := i.blockReader.ReadThroughCache(dataBlockBh, block_common.BlockKindData)
	if err != nil {
		zap.L().Error("failed to read the data block", zap.Error(err))
		return err
	}

	i.dataBlockIter = row_block.NewBlockIterator(i.bpool, i.cmp, dataBlock)
	return nil
}

func (i *DataIterator) load1stIndexBlockIter() error {
	firstLvlIndexBH := &block_common.BlockHandle{}
	secondLvlIndexVal := i.secondLevelIndex.V.Value()
	if n := firstLvlIndexBH.DecodeFrom(secondLvlIndexVal); n <= 0 {
		zap.L().Error("failed to fully decode the 2nd-level index")
		return fmt.Errorf("failed to fully decode the 2nd-level index. %w", common.InternalServerError)
	}
	firstLvlIndexBlock, err := i.blockReader.ReadThroughCache(firstLvlIndexBH, block_common.BlockKindIndex)
	if err != nil {
		zap.L().Error("failed to read the first-level index block", zap.Error(err))
		return err
	}
	i.firstLevelIndexIter = row_block.NewBlockIterator(i.bpool, i.cmp, firstLvlIndexBlock)
	return nil
}

func (i *DataIterator) invalidateDataIter() {
	if i.dataBlockIter == nil {
		return
	}
	_ = i.dataBlockIter.Close()
	i.dataBlockIter = nil
}

func (i *DataIterator) invalidate1stLevelIndexIter() {
	if i.firstLevelIndexIter == nil {
		return
	}
	_ = i.firstLevelIndexIter.Close()
	i.firstLevelIndexIter = nil
	i.firstLevelIndex.V.Release()
	i.firstLevelIndexIter = nil
}

func NewIterator(fr go_fs.Readable, cmp common.IComparer, opts *options.IteratorOpts) (*DataIterator, error) {
	iter := dataBlockIteratorPool.Get().(*DataIterator)
	var err error
	var footer *row_block.Footer
	var layoutReader storage.ILayoutReader

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
