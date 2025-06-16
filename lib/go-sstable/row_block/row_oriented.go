package row_block

import (
	"encoding/binary"

	go_bytesbufferpool "github.com/datnguyenzzz/nogodb/lib/go-bytesbufferpool"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
)

const (
	// maximumRestartOffset indicates the maximum offset that we can encode
	// within a restart point of a row-oriented block.
	// If a block exceeds this size and we attempt to add another KV pair, the
	// restart points table will be unable to express the position of the pair,
	// resulting in undefined behavior and arbitrary corruption.
	maximumRestartOffset = 1<<31 - 1
)

// A rowBlockBuf holds the buffer and all the state required to compress and write a data block to disk.
//
// When the RowBlockWriter client adds keys to the SStable, it writes directly into a buffer until the block is full.
// Once a rowBlockBuf's block is full, the rowBlockBuf will be passed to other goroutines for compression and file I/O.
type rowBlockBuf struct {
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

func (d *rowBlockBuf) EntryCount() int {
	return d.nEntries
}

func (d *rowBlockBuf) CurKey() *common.InternalKey {
	return common.DeserializeKey(d.curKey)
}

// WriteToBuf write the key-value into the buffer block
func (d *rowBlockBuf) WriteToBuf(key common.InternalKey, value []byte) error {
	d.prevKey = d.curKey

	size := key.Size()
	if cap(d.curKey) < size {
		d.curKey = make([]byte, 0, 2*size) // reduce number of times that need to allocate
	}
	d.curKey = d.curKey[:size]
	key.SerializeTo(d.curKey)
	return d.writeToBuf(value)
}

func (d *rowBlockBuf) CleanUpForReuse() {
	d.nEntries = 0
	d.nextRestartEntry = 0
	d.restartOffset = d.restartOffset[:0]
	d.buf = d.buf[:0]
}

// Finish finalizes the row block, and returns the serialized data.
func (d *rowBlockBuf) Finish() []byte {
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

	d.CleanUpForReuse()
	return res
}

func (d *rowBlockBuf) writeToBuf(value []byte) error {
	if len(d.buf) > maximumRestartOffset {
		return common.ClientInvalidRequestError
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

// ShouldFlush returns true if we should flush the current row block, because
// adding a new K/V would breach the configured threshold
func (d *rowBlockBuf) ShouldFlush(
	pendingKeyLen int,
	pendingValueLen int,
	decider common.IFlushDecider,
) bool {
	// We shouldn't flush if the block doesn't have any data at all
	if d.EntryCount() == 0 {
		return false
	}
	estCurrentSize := d.EstimateSize()
	estNewSize := estCurrentSize + pendingValueLen + pendingKeyLen
	if d.EntryCount()%d.restartInterval == 0 {
		estNewSize += 4
	}
	estNewSize += 4                                   // assume 0 as a shared varint
	estNewSize += uvarintLen(uint32(pendingKeyLen))   // assume all pendingKeyLen is non-shared
	estNewSize += uvarintLen(uint32(pendingValueLen)) // for value len

	return decider.ShouldFlush(estCurrentSize, estNewSize)
}

func (d *rowBlockBuf) EstimateSize() int {
	// buffer + 4 bytes for each entry offset + reserved 4-byte space for the restarts len
	return len(d.buf) + 4*len(d.restartOffset) + 4
}

func uvarintLen(v uint32) int {
	i := 0
	for v >= 0x80 {
		v >>= 7
		i++
	}
	return i + 1
}

func closeRowBlock(d *rowBlockBuf) {
	go_bytesbufferpool.Put(d.buf)
	*d = rowBlockBuf{}
}

func newDataBlock(restartInterval int) *rowBlockBuf {
	d := &rowBlockBuf{
		buf: go_bytesbufferpool.Get(maximumRestartOffset),
	}
	d.restartInterval = restartInterval
	return d
}
