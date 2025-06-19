package common

import "encoding/binary"

const TrailerLen = 5

// PhysicalBlock represents a block  as it is stored
// physically on disk, including its trailer.
type PhysicalBlock struct {
	data []byte
	// Trailer is the trailer at the end of a block, encoding the block type
	// (compression) and a checksum.
	trailer [TrailerLen]byte
}

func (p *PhysicalBlock) SetData(data []byte) {
	p.data = data
}

func (p *PhysicalBlock) SetTrailer(auxiliary byte, checksum uint32) {
	var trailer [TrailerLen]byte
	trailer[0] = auxiliary
	binary.LittleEndian.PutUint32(trailer[1:], checksum)

	p.trailer = trailer
}

// BlockHandle is the file offset and length of a block.
type BlockHandle struct {
	// Offset identifies the offset of the block within the file.
	Offset uint64
	// Length is the length of the block data (excludes the trailer).
	Length uint64
}
