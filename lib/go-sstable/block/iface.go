package block

import (
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	commonBlock "github.com/datnguyenzzz/nogodb/lib/go-sstable/common/block"
)

type IIndexWriter interface {
	Add(key *common.InternalKey, bh *commonBlock.BlockHandle) error
	// BuildIndex build the 2-level index for the SST, and write to the stable storage
	BuildIndex() error
	// ...
}
