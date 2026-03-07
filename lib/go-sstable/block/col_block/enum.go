package colblock

type DataType byte

const (
	UnknownDT DataType = iota
	PrefixCompressedBytesDT
	RawBytesDT
	UintDT
)

type EncodableDataType interface {
	UintType | ~[]byte
}

type UintType interface {
	~uint8 | ~uint16 | ~uint32 | ~uint64
}
