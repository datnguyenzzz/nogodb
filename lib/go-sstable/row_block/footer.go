package row_block

import (
	"encoding/binary"
	"fmt"

	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common/block"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/storage"
)

type Footer struct {
	version     common.TableVersion
	metaIndexBH block.BlockHandle
}

func (f *Footer) Serialise() []byte {
	footerSize := common.TableFooterSize[f.version]
	buf := make([]byte, footerSize)
	n := 0
	n += f.metaIndexBH.EncodeInto(buf[:])
	binary.LittleEndian.PutUint32(buf[cap(buf)-common.MagicNumberLen-common.TableVersionLen:], uint32(f.version))
	copy(buf[cap(buf)-common.MagicNumberLen:], common.MagicNumber)
	return buf
}

func (f *Footer) GetMetaIndex() *block.BlockHandle {
	return &f.metaIndexBH
}

func ReadFooter(
	reader storage.ILayoutReader,
	size uint64,
) (*Footer, error) {
	buf := make([]byte, common.MaxPossibleFooterSize)
	offset := size - uint64(common.MaxPossibleFooterSize)
	if offset < 0 {
		offset = 0
		buf = buf[:size]
	}
	if err := reader.ReadAt(buf, offset); err != nil {
		return nil, err
	}

	// Parse the footer from the read buffer, refer to
	// the function Footer.Serialise() or README to understand how the foot
	// is constructed
	switch magic := buf[len(buf)-common.MagicNumberLen:]; string(magic) {
	case common.MagicNumber:
		version := common.TableVersion(binary.LittleEndian.Uint32(buf[len(buf)-common.MagicNumberLen-common.TableVersionLen:]))
		if len(buf) < common.TableFooterSize[version] {
			return nil, fmt.Errorf("footer is too short")
		}

		metaIndexBuf := buf[len(buf)-common.TableFooterSize[version]:]
		metaIndexBH := &block.BlockHandle{}
		_ = metaIndexBH.DecodeFrom(metaIndexBuf)

		return &Footer{
			version:     version,
			metaIndexBH: *metaIndexBH,
		}, nil

	default:
		return nil, fmt.Errorf("unrecognized magic number: %s", magic)
	}
}
