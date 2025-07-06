package row_block

import (
	"encoding/binary"

	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
)

const (
	// magicNumber 8 byte
	magicNumber = "\x6e\x6f\x67\x6f\x64\x62\x6b\x76"
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
	copy(buf[cap(buf)-common.MagicNumberLen:], magicNumber)
	return buf
}
