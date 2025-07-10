package row_block

import (
	"encoding/binary"

	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
)

type footer struct {
	version     common.TableVersion
	metaIndexBH common.BlockHandle
}

func (f *footer) Serialise() []byte {
	footerSize := common.TableFooterSize[f.version]
	buf := make([]byte, footerSize)
	n := 0
	n += f.metaIndexBH.EncodeInto(buf[:])
	binary.LittleEndian.PutUint32(buf[cap(buf)-common.MagicNumberLen-common.TableVersionLen:], uint32(f.version))
	copy(buf[cap(buf)-common.MagicNumberLen:], common.MagicNumber)
	return buf
}
