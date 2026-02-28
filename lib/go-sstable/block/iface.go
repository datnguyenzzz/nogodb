package block

import (
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	commonBlock "github.com/datnguyenzzz/nogodb/lib/go-sstable/common/block"
)

type IIndexWriter interface {
	Add(key *common.InternalKey, bh *commonBlock.BlockHandle) error
	// ...
}
