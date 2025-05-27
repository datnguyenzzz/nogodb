package go_wal

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"

	go_bytesbufferpool "github.com/datnguyenzzz/nogodb/lib/go-bytesbufferpool"
	"go.uber.org/zap"
)

const (
	firstPageId      PageID = 0
	defaultBlockSize        = 32 * 1024 // 32KB
	// headerSize = CRC (4B) + Payload Size (2B) + Record Type (1B) + Log Number (8B) = 15B
	headerSize = 15
)

// readBufferPool maintains a pool of 32KB buffers, each serving as a dedicated buffer for individual blockp.
// This design helps reduce garbage collection (GC) pressure and minimizes memory allocations by reusing buffers,
// eliminating the need to create new buffers for every read and write operation, so the GC doesn't have to be kicked in
// to clean up the buffers after used. Since records are guaranteed to never exceed a data size of 32KB,
// the maximum buffer size is predictable.
var readBufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, defaultBlockSize)
	},
}

// An enhancement to address the issue of inefficient memory usage, where code that
// requires a small amount of memory may receive a large buffer from the pool, and vice versa.
// For example, in the case of writeBufferPool, the size of data writes can vary, making the
// allocation of a fixed 32KB buffer wasteful.
//
// A Non-optimised implementation for the writeBufferPool
//var writeBufferPool = sync.Pool{
//	New: func() interface{} {
//		return make([]byte, blockSize)
//	},
//}

func (p *Page) Delete(ctx context.Context) error {
	return os.Remove(p.F.Name())
}

// Sync manually flush the page to the disk
func (p *Page) Sync(ctx context.Context) error {
	return p.F.Sync()
}

func (p *Page) Size() int64 {
	return int64(p.TotalBlockCount)*int64(defaultBlockSize) + int64(p.LastBlockSize)
}

func (p *Page) Close(ctx context.Context) error {
	return p.F.Close()
}

// Read data from given Position. Reader always reads full record with the size of 32KB
func (p *Page) Read(ctx context.Context, pos *Position) ([]byte, error) {
	rBuf := readBufferPool.Get().([]byte)
	defer func() {
		rBuf = rBuf[:0]
		// ensure the memory buffer always be 32KB
		if cap(rBuf) != defaultBlockSize {
			rBuf = make([]byte, defaultBlockSize)
		}
		readBufferPool.Put(rBuf)
	}()

	var res []byte

	record := pos.BlockNumber
	recordOffset := pos.Offset
	totalSize := p.Size()

	for {
		pageOffset := defaultBlockSize * record
		size := min(defaultBlockSize, totalSize-int64(pageOffset))

		if recordOffset > defaultBlockSize {
			zap.L().Error("Read out of range")
			return nil, io.ErrUnexpectedEOF
		}

		// read whole record into the allocated buffer
		if _, err := p.F.ReadAt(rBuf[:size], int64(pageOffset)); err != nil {
			return nil, err
		}

		header := make([]byte, headerSize)
		copy(header, rBuf[recordOffset:recordOffset+headerSize])
		dataLen := binary.LittleEndian.Uint16(header[4:6])
		start := recordOffset + headerSize
		end := start + uint32(dataLen)
		res = append(res, rBuf[start:end]...)

		// Checksum to ensure data integrity
		savedCRC := binary.LittleEndian.Uint32(header[:4])
		crc := crc32.ChecksumIEEE(rBuf[recordOffset+4 : end])
		if crc != savedCRC {
			return nil, ErrInvalidChecksum
		}

		recType := RecordType(header[6])
		if recType == FullType || recType == LastType {
			break
		}

		// Read the next record
		recordOffset = 0
		record++
	}

	return res, nil
}

// Write append an arbitrary slice of bytes to the OS buffer
func (p *Page) Write(ctx context.Context, data []byte) (*Position, error) {
	neededSpaces := estimateNeededSpaces(data)
	if p.LastBlockSize+headerSize >= defaultBlockSize {
		// need spaces for padded bytes
		neededSpaces += int(defaultBlockSize - p.LastBlockSize)
	}
	wBuf := go_bytesbufferpool.Get(neededSpaces)

	// put back (and reset) when finish using the buffer
	defer go_bytesbufferpool.Put(wBuf)

	// 1. Manage to write the data onto the already allocated buffer
	rec, err := p.writeToMemBuffer(ctx, data, &wBuf)
	if err != nil {
		return nil, err
	}

	// 2. Write to OS buffer, aka page cache, which will be asynchronously flush (managed by OS kernel) to the disk later
	if _, err := p.F.Write(wBuf); err != nil {
		return nil, err
	}

	return rec, nil
}

// writeToMemBuffer append an arbitrary slice of bytes to the memory buffer
func (p *Page) writeToMemBuffer(ctx context.Context, data []byte, buf *[]byte) (*Position, error) {
	// If a data is not fit into the current block
	if p.LastBlockSize+headerSize >= defaultBlockSize {
		padding := defaultBlockSize - p.LastBlockSize
		if padding > 0 {
			startPos := len(*buf)
			if startPos+int(padding) > cap(*buf) {
				zap.L().Error(fmt.Sprintf("padding overflow, buf capacity: %d", cap(*buf)))
				return nil, io.ErrShortWrite
			}
			*buf = append(*buf, make([]byte, padding)...)
			p.LastBlockSize = 0
			p.TotalBlockCount += 1
		}
	}

	pos := &Position{
		PageId:      p.Id,
		BlockNumber: p.TotalBlockCount,
		Offset:      p.LastBlockSize,
		Size:        0, // compute later
	}

	willBeOverflow := p.LastBlockSize+headerSize+uint32(len(data)) > defaultBlockSize
	if willBeOverflow {
		pendingWriteBytes := uint32(len(data))
		pos.Size = pendingWriteBytes
		for pendingWriteBytes > 0 {
			// write [header + writableBytes] to buffer
			writableBytes := min(defaultBlockSize-headerSize-p.LastBlockSize, pendingWriteBytes)

			var recordType RecordType
			switch {
			case pendingWriteBytes == uint32(len(data)):
				recordType = FirstType
			case pendingWriteBytes-writableBytes > 0:
				recordType = MiddleType
			default:
				recordType = LastType
			}

			pendingWriteBytes -= writableBytes
			end := uint32(len(data)) - pendingWriteBytes
			start := end - writableBytes
			writeToBuffer(buf, data[start:end], recordType)

			// Move to next batch
			p.LastBlockSize = (p.LastBlockSize + headerSize + writableBytes) % defaultBlockSize
			if p.LastBlockSize == 0 {
				p.TotalBlockCount += 1
			}

			pos.Size += headerSize
		}
	} else {
		writeToBuffer(buf, data, FullType)
		pos.Size = uint32(headerSize + len(data))

		p.LastBlockSize = (p.LastBlockSize + pos.Size) % defaultBlockSize
		if p.LastBlockSize == 0 {
			p.TotalBlockCount += 1
		}
	}

	return pos, nil
}

func writeToBuffer(buf *[]byte, data []byte, recType RecordType) {
	header := make([]byte, headerSize)
	// 2-bytes: [4,5] for storing data length
	binary.LittleEndian.PutUint16(header[4:6], uint16(len(data)))
	// 1-byte: [6] for storing chunk type
	header[6] = byte(recType)
	// 8-bytes: [7,14] for storing the current timestamp
	currTs := time.Now().UTC().Unix()
	binary.LittleEndian.PutUint64(header[7:], uint64(currTs))
	// 4-bytes: [0,4] for storing the checksum of header[4:] + payload
	checksum := crc32.ChecksumIEEE(header[4:])
	checksum = crc32.Update(checksum, crc32.IEEETable, data)
	binary.LittleEndian.PutUint32(header[:4], checksum)

	*buf = append(*buf, header...)
	*buf = append(*buf, data...)
}

func openPageByPath(path string, id PageID, mode PageAccessMode) (*Page, error) {
	var flag int
	switch mode {
	case PageAccessModeReadWrite:
		flag = os.O_CREATE | os.O_RDWR | os.O_TRUNC
	case PageAccessModeReadWriteSync:
		flag = os.O_CREATE | os.O_RDWR | os.O_TRUNC | os.O_SYNC
	case PageAccessModeReadOnly:
		flag = os.O_RDONLY
	default:
		return nil, fmt.Errorf("invalid page mode: %d", mode)
	}

	f, err := os.OpenFile(path, flag, 0644)
	if err != nil {
		return nil, err
	}

	offset, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	return &Page{
		Id:              id,
		F:               f,
		TotalBlockCount: uint32(offset / defaultBlockSize),
		LastBlockSize:   uint32(offset % defaultBlockSize),
	}, nil
}
