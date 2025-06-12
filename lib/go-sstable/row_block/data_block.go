package row_block

import (
	"encoding/binary"

	go_bytesbufferpool "github.com/datnguyenzzz/nogodb/lib/go-bytesbufferpool"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/base"
)

const (
	// maximumRestartOffset indicates the maximum offset that we can encode
	// within a restart point of a row-oriented block.
	// If a block exceeds this size and we attempt to add another KV pair, the
	// restart points table will be unable to express the position of the pair,
	// resulting in undefined behavior and arbitrary corruption.
	maximumRestartOffset = 1<<31 - 1
)

// A dataBlockBuf holds the buffer and all the state required to compress and write a data block to disk.
//
// When the RowBlockWriter client adds keys to the SStable, it writes directly into a buffer until the block is full.
// Once a dataBlockBuf's block is full, the dataBlockBuf will be passed to other goroutines for compression and file I/O.
type dataBlockBuf struct {
	nEntries int
	// curKey represents the serialised value of the current internal key
	curKey []byte
	// prevKey represents the serialised value of the previous internal key
	prevKey []byte

	restartInterval int
	// Note: The first restart always at 0
	nextRestartEntry int
	restartOffset    []uint32

	buf []byte
}

func (d *dataBlockBuf) EntryCount() int {
	return d.nEntries
}

func (d *dataBlockBuf) CurKey() *base.InternalKey {
	return base.DeserializeKey(d.curKey)
}

// WriteToBuf write the key-value into the buffer block
func (d *dataBlockBuf) WriteToBuf(key base.InternalKey, value []byte) error {
	d.prevKey = d.curKey

	size := key.Size()
	if cap(d.curKey) < size {
		d.curKey = make([]byte, 0, 2*size) // reduce number of times that need to allocate
	}
	d.curKey = d.curKey[:size]
	key.SerializeTo(d.curKey)
	return d.writeToBuf(key, value)
}

// Generate finalizes the data block, and returns the serialized data.
func (d *dataBlockBuf) Generate() []byte {
	// write the trailer
	//+-- 4-bytes --+
	///               \
	//+-----------------+-----------------+-----------------+------------------------------+
	//| restart point 1 |       ....      | restart point n | restart points len (4-bytes) |
	//+-----------------+-----------------+-----------------+------------------------------+
	if d.EntryCount() == 0 {
		if cap(d.restartOffset) > 0 {
			d.restartOffset = d.restartOffset[:1]
			d.restartOffset[0] = 0
		} else {
			d.restartOffset = append(d.restartOffset, 0)
		}
	}

	var tmp [4]byte
	for _, restart := range d.restartOffset {
		binary.LittleEndian.PutUint32(tmp[:], restart)
		d.buf = append(d.buf, tmp[:]...)
	}
	binary.LittleEndian.PutUint32(tmp[:], uint32(len(d.restartOffset)))
	d.buf = append(d.buf, tmp[:]...)

	res := d.buf

	// Clean up the state
	d.nEntries = 0
	d.nextRestartEntry = 0
	d.restartOffset = d.restartOffset[:0]
	d.buf = d.buf[:0]

	return res
}

func (d *dataBlockBuf) writeToBuf(key base.InternalKey, value []byte) error {
	if len(d.buf) > maximumRestartOffset {
		return base.ClientInvalidRequestError
	}

	// 1. Compute shared or restart point
	var shared int
	if d.nEntries == d.nextRestartEntry {
		d.nextRestartEntry = d.nEntries + d.restartInterval
		d.restartOffset = append(d.restartOffset, uint32(len(d.buf)))
	} else {
		// Iterate 8 bytes at once
		comparePrefix := func(idx int) bool {
			curKeyPref := binary.LittleEndian.Uint64(d.curKey[shared:])
			prevKeyPref := binary.LittleEndian.Uint64(d.prevKey[shared:])
			return curKeyPref == prevKeyPref
		}
		for ; shared < min(len(d.curKey), len(d.prevKey)); shared += 8 {
			if !comparePrefix(shared) {
				break
			}
		}

		for ; shared < min(len(d.curKey), len(d.prevKey)); shared++ {
			if !comparePrefix(shared) {
				break
			}
		}
	}

	// Append to the buffer
	//
	//+-------+---------+-----------+---------+--------------------+--------------+----------------+
	//| shared (varint) | not shared (varint) | value len (varint) | key (varlen) | value (varlen) |
	//+-----------------+---------------------+--------------------+--------------+----------------+
	needed := 3*binary.MaxVarintLen32 + len(d.curKey[shared:]) + len(value)
	n := len(d.buf)
	if cap(d.buf) < n+needed {
		newCap := cap(d.buf)
		if cap(d.buf) == 0 {
			newCap = 1024 // minimum of 1KB
		}
		for newCap < n+needed {
			newCap <<= 1
		}
		tmp := make([]byte, n, newCap)
		copy(tmp, d.buf)
		d.buf = tmp
	}

	d.buf = d.buf[:n+needed]
	// shared key len
	n = binary.PutUvarint(d.buf[n:], uint64(shared))
	// non shared key len
	n = binary.PutUvarint(d.buf[n:], uint64(len(d.curKey)-shared))
	// value len
	n = binary.PutUvarint(d.buf[n:], uint64(len(value)))
	// key without the shared prefix
	n += copy(d.buf[n:], d.curKey[shared:])
	// value
	n += copy(d.buf[n:], value)

	d.buf = d.buf[:n]
	return nil
}

func newDataBlock(restartInterval int) *dataBlockBuf {
	d := &dataBlockBuf{
		buf: go_bytesbufferpool.Get(maximumRestartOffset),
	}
	d.restartInterval = restartInterval
	return d
}
