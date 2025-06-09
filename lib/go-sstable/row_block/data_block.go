package row_block

import (
	"sync"

	"github.com/datnguyenzzz/nogodb/lib/go-sstable/base"
)

// A dataBlock holds all the state required to compress and write a data block to disk.
//
// When the RowBlockWriter client adds keys to the SStable, it writes directly into a buffer until the block is full.
// Once a dataBlock's block is full, the dataBlock will be passed to other goroutines for compression and file I/O.
type dataBlock struct {
	nEntries int
	// curKey represents the serialised value of the current internal key
	curKey []byte
}

func (d *dataBlock) EntryCount() int {
	return d.nEntries
}

func (d *dataBlock) CurKey() *base.InternalKey {
	return base.DeserializeKey(d.curKey)
}

var dataBlockPool = sync.Pool{
	New: func() interface{} {
		return &dataBlock{}
	},
}

func newDataBlock() *dataBlock {
	return dataBlockPool.Get().(*dataBlock)
}
