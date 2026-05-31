package manifest

import (
	"bytes"
	"encoding/binary"
	"io"
)

// Tags for the versionEdit disk format
const (
	tagComparator = iota
	tagNextFileNumber
)

// versionEdit holds the state for an delta edit to a Version
type versionEdit struct {
	comparerName string
	nextFileNum  int64

	newTables []newTableEntry
}

type versionEditEncoder struct {
	*bytes.Buffer
}

func (e versionEditEncoder) writeString(s string) {
	e.writeUvarint(uint64(len(s)))
	e.WriteString(s)
}

func (e versionEditEncoder) writeUvarint(u uint64) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], u)
	e.Write(buf[:n])
}

func (ve *versionEdit) Encode(w io.Writer) error {
	enc := versionEditEncoder{new(bytes.Buffer)}

	if len(ve.comparerName) > 0 {
		enc.writeUvarint(tagComparator)
		enc.writeString(ve.comparerName)
	}

	if ve.nextFileNum > 0 {
		enc.writeUvarint(tagNextFileNumber)
		enc.writeUvarint(uint64(ve.nextFileNum))
	}

	for _, table := range ve.newTables {
		enc.writeUvarint(uint64(table.level))
		enc.writeUvarint(table.meta.TableNum)
		enc.writeUvarint(table.meta.Size)
	}

	_, err := w.Write(enc.Bytes())
	return err
}
