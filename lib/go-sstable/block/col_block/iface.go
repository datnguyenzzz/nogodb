package colblock

type IColumnEncoder[T EncodableDataType] interface {
	Init()
	// Reset reuses the existing encoder with its already allocated memory
	Reset()
	Append(v T)
	// Size returns the size of the column, if the its row were encoded starting from an [offset]
	Size(offset uint32) uint32
	// Finish serialises the encoded column into a [buf] from [offset], return the offset after written
	Finish(offset uint32, buf []byte) uint32
}

type IColumnDecoder[T EncodableDataType] interface {
	// todo: add me
}
