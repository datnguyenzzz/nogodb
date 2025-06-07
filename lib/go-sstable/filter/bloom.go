package filter

import (
	"encoding/binary"
	"sync"
)

const (
	// TODO Make this configurable from 1->20
	defaultBitsPerKeys = 10
	hashBlockLen       = 0x4000
	cacheLineBytesSize = 64
	cacheLineBitsSize  = 8 * cacheLineBytesSize
)

type blockHash [hashBlockLen]uint32

var blockHashBuffer = sync.Pool{
	New: func() interface{} {
		return &blockHash{}
	},
}

// bloomFilter is an implementation of the blocked Bloom Filter
// https://save-buffer.github.io/bloom_filter.html
type bloomFilter struct{}
type bloomFilterWriter struct {
	// bitsPerKeys = <bits size of entire of an entire array> / <number of the keys>
	// For the given eps (the desired false positive rate),
	//   - The minimum of bitsPerKeys = log(2)*eps
	//   - With the corresponding number of hash functions (probe), k = log(2)*eps
	//
	// Simply the equation backwards, we get: eps = bitPerKeys / log(2)
	// Therefore, there are diminishing returns on eps past around bitsPerKeys = 10 (we chose 10 as defaultBitsPerKeys)
	bitsPerKeys int

	blocks   []*blockHash
	numKeys  int
	lastHash uint32
}

// Writer \\

func (bw *bloomFilterWriter) Add(key []byte) {
	h := bloomHash(key)
	if bw.numKeys > 0 && bw.lastHash == h {
		return
	}

	pos := bw.numKeys % hashBlockLen
	if pos == 0 {
		// alloc a new block
		bw.blocks = append(bw.blocks, blockHashBuffer.Get().(*blockHash))
	}

	bw.blocks[len(bw.blocks)-1][pos] = h
	bw.lastHash = h
	bw.numKeys++
}

func (bw *bloomFilterWriter) Build(b *[]byte) {
	var nLines int
	var nProbes byte
	// 1. calculate number of cache lines to fit all the added keys (round up)
	nLines = (bw.numKeys*bw.bitsPerKeys + cacheLineBitsSize - 1) / cacheLineBitsSize
	// Make nLines an odd number to make sure more bits are involved when
	// determining which block.
	if nLines%2 == 0 {
		nLines++
	}
	nBytes := nLines * cacheLineBytesSize

	// freeSpaces points to the starting index of the "b" buffer that are free to write
	var freeSpaces []byte

	// 2. grow the given buffer to have at least spaces to hold the data
	// [nBytes (for all the hashes) + 4 (for number of lines) + 1 (for number of probes)] bytes
	wantSize := nBytes + 5 + len(*b)
	if wantSize <= cap(*b) {
		*b = (*b)[:wantSize]
		freeSpaces = (*b)[len(*b):]
		clear(freeSpaces)
	} else {
		// grow (exponentially) the given buffer to have enough needed spaces
		neededSize := 1024
		for neededSize < wantSize {
			neededSize += neededSize / 4
		}

		tmp := *b
		*b = make([]byte, wantSize, neededSize)
		freeSpaces = (*b)[len(tmp):]
		copy(*b, tmp)
	}

	// 3. build the filters from the blocks
	nProbes = calculateProbes(bw.bitsPerKeys)
	for idx, block := range bw.blocks {
		nHashes := hashBlockLen
		if idx == len(bw.blocks)-1 && bw.numKeys%hashBlockLen != 0 {
			nHashes = bw.numKeys % hashBlockLen
		}

		for _, h := range block[:nHashes] {
			delta := h>>17 | h<<15
			startPos := (h % uint32(nLines)) * cacheLineBitsSize
			for p := byte(0); p < nProbes; p++ {
				bitPos := startPos + (h % cacheLineBitsSize)
				freeSpaces[bitPos/8] |= 1 << (bitPos % 8)
				h += delta
			}
		}
	}
	freeSpaces[nBytes] = nProbes
	binary.LittleEndian.PutUint32(freeSpaces[nBytes+1:], uint32(nLines))

	// 4. Release the hashblock pool
	for i, block := range bw.blocks {
		blockHashBuffer.Put(block)
		bw.blocks[i] = nil
	}
	bw.blocks = bw.blocks[:0]
	bw.numKeys = 0
}

// End of Writer \\

func (bf *bloomFilter) NewWriter() IWriter {
	return &bloomFilterWriter{
		bitsPerKeys: defaultBitsPerKeys,
		blocks:      []*blockHash{},
	}
}

func (bf *bloomFilter) MayContain(filter, key []byte) bool {
	// required at least 5 bytes for the nLines and nProbes
	if len(filter) <= 5 {
		return false
	}
	n := len(filter) - 5
	nProbes := filter[n]
	nLines := binary.LittleEndian.Uint32(filter[n+1:])
	cacheLineBits := 8 * (uint32(n) / nLines)

	h := bloomHash(key)
	delta := h>>17 | h<<15
	b := (h % nLines) * cacheLineBits

	for j := uint8(0); j < nProbes; j++ {
		bitPos := b + (h % cacheLineBits)
		if filter[bitPos/8]&(1<<(bitPos%8)) == 0 {
			return false
		}
		h += delta
	}
	return true
}

func (bf *bloomFilter) Name() string {
	return "nogodb.BloomFilter"
}

func newBloomFilter() IFilter {
	return &bloomFilter{}
}

func calculateProbes(bitsPerKey int) byte {
	n := byte(float64(bitsPerKey) * 0.69) // 0.69 =~ ln(2)
	if n < 1 {
		n = 1
	}
	if n > 30 {
		n = 30
	}
	return n
}

// bloomHash return hash of the given data. Similar to murmur hash function
func bloomHash(b []byte) uint32 {
	const (
		seed = 0xbc9f1d34
		m    = 0xc6a4a793
	)
	h := uint32(seed) ^ uint32(uint64(uint32(len(b))*m))
	for ; len(b) >= 4; b = b[4:] {
		h += uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		h *= m
		h ^= h >> 16
	}

	// The code below first casts each byte to a signed 8-bit integer. This is
	// necessary to match RocksDB's behavior. Note that the `byte` type in Go is
	// unsigned. What is the difference between casting a signed 8-bit value vs
	// unsigned 8-bit value into an unsigned 32-bit value?
	// Sign-extension. Consider the value 250 which has the bit pattern 11111010:
	//
	//   uint32(250)        = 00000000000000000000000011111010
	//   uint32(int8(250))  = 11111111111111111111111111111010
	//
	// Note that the original LevelDB code did not explicitly cast to a signed
	// 8-bit value which left the behavior dependent on whether C characters were
	// signed or unsigned which is a compiler flag for gcc (-funsigned-char).
	switch len(b) {
	case 3:
		h += uint32(int8(b[2])) << 16
		fallthrough
	case 2:
		h += uint32(int8(b[1])) << 8
		fallthrough
	case 1:
		h += uint32(int8(b[0]))
		h *= m
		h ^= h >> 24
	}
	return h
}

var _ IFilter = (*bloomFilter)(nil)
var _ IWriter = (*bloomFilterWriter)(nil)
