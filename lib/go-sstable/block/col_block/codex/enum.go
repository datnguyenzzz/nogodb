package codex

type DataType byte

const (
	UnknownDT DataType = iota
	PrefixCompressedBytesDT
	RawBytesDT
	UintDT
)

type EncodableDataType interface {
	UintType | ByteType
}

type UintType interface {
	~uint8 | ~uint16 | ~uint32 | ~uint64
}

type ByteType interface {
	~[]byte
}

type IEncoderFinisher interface {
	DataType() DataType
	// Finish serialises the encoded column into a [buf] from [offset], return the offset after written
	Finish(offset uint32, buf []byte) uint32
}

type IColumnEncoder[T EncodableDataType] interface {
	Init()
	// Reset reuses the existing encoder with its already allocated memory
	Reset()
	Append(v T)
	// Size returns the size of the column, if the its row were encoded starting from an [offset]
	Size(offset uint32) uint32

	IEncoderFinisher
}

type IColumnDecoder[T EncodableDataType] interface {
	Get(row uint32) T
	DataType() DataType
}

// DecoderInstructor create a decoder for a column that has "rows" rows
// and from data[offset:]
type DecoderInstructor[T EncodableDataType] func(rows, offset uint32, data []byte) (IColumnDecoder[T], uint32)
