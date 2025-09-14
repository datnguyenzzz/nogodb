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

// DataIterator reads the 2nd-level index block and creates and
// initializes an iterator over the 1st-level index, by which subsequently
// iterate over the datablock within the requested boundary [lower_bound, upper_bound]
type DataIterator struct {
	lower, upper []byte
	cmp          common.IComparer

	bpool *predictable_size.PredictablePool
	// filterBH
	filterBH *block_common.BlockHandle
	filter   *common.InternalLazyValue

	// secondLevelIndexIter iterator through the 2nd level index block
	secondLevelIndexBH   *block_common.BlockHandle
	secondLevelIndexIter row_block.IBlockIterator
	blockReader          row_block.IBlockReader
}

var dataBlockIteratorPool = sync.Pool{
	New: func() interface{} {
		return &DataIterator{
			bpool: predictable_size.NewPredictablePool(),
		}
	},
}

func (i *DataIterator) SeekPrefixGE(prefix, key []byte) *common.InternalIterator {
	//TODO implement me
	panic("implement me")
}

func (i *DataIterator) SeekGTE(key []byte) *common.InternalKV {
	// 1. Seek GTE of the 2nd level index to get index key ≥ search key
	// 2. Seek GTE of the 1st level index to get index key ≥ search key
	// 3. Read data block from the given 1st-level index block
	// 4. seek data block to get key ≥ search key
	// Important notes:
	//  - Ensure the data (lazyValue) is released properly once the block is no longer used

	if i.cmp.Compare(key, i.lower) < 0 {
		key = i.lower
	}

	panic("implement me")
}

func (i *DataIterator) SeekLT(key []byte) *common.InternalKV {
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
	err := i.secondLevelIndexIter.Close()
	i.blockReader.Release()
	dataBlockIteratorPool.Put(i)
	return err
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

	iter.lower = opts.Lower
	iter.upper = opts.Upper
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
