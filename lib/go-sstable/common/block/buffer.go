package block

// Buffer is a block buffer for the decompressed data of the block, excludes the trailer.
// It's either backed by a BufferPool or Block Cache
type Buffer struct {
	buf   []byte
	cache *BlockCache // No-use for now, reserved for the future implementation of the BlockCache
}

func MakeBufferRaw(buf []byte) *Buffer {
	return &Buffer{
		buf: buf,
	}
}

func (b *Buffer) ToByte() []byte {
	return b.buf
}

type BlockCache struct{}
