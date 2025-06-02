package base

// Compression is the per-block compression algorithm to use.
type Compression int

// The available compression types.
const (
	NoCompression Compression = iota
	SnappyCompression
	ZstdCompression
)
