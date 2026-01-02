package go_wal

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"testing"

	go_fs "github.com/datnguyenzzz/nogodb/lib/go-fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_writeToMemBuffer(t *testing.T) {
	type param struct {
		testName string
		// input
		data     []byte
		pageInfo *Page
		// expectation
		expectedSize   int64
		expectedPos    *Position
		expectedPage   *Page
		expectedRecord []byte
	}

	testCases := []param{
		{
			testName: "write small data, without padding",
			data:     generateBytes(10),
			pageInfo: &Page{
				Id:              1,
				TotalBlockCount: 1,
				LastBlockSize:   21 * 1024,
			},
			expectedSize: 25,
			expectedPos: &Position{
				PageId:      1,
				BlockNumber: 1,
				Offset:      21 * 1024,
			},
			expectedPage: &Page{
				Id:              1,
				TotalBlockCount: 1,
				LastBlockSize:   21*1024 + 25,
			},
			expectedRecord: []byte{byte(FullType)},
		},
		{
			testName: "write small data, with padding",
			data:     generateBytes(10),
			pageInfo: &Page{
				Id:              1,
				TotalBlockCount: 1,
				LastBlockSize:   32*1024 - 15,
			},
			expectedSize: 25,
			expectedPos: &Position{
				PageId:      1,
				BlockNumber: 2,
				Offset:      0,
			},
			expectedPage: &Page{
				Id:              1,
				TotalBlockCount: 2,
				LastBlockSize:   25,
			},
			expectedRecord: []byte{byte(FullType)},
		},
		{
			testName: "write medium data, without padding",
			data:     generateBytes(30 * 1024), // 30KB
			pageInfo: &Page{
				Id:              1,
				TotalBlockCount: 1,
				LastBlockSize:   15 * 1024,
			},
			expectedSize: 15*2 + 30*1024,
			expectedPos: &Position{
				PageId:      1,
				BlockNumber: 1,
				Offset:      15 * 1024,
			},
			expectedPage: &Page{
				Id:              1,
				TotalBlockCount: 2,
				LastBlockSize:   2*15 + 13*1024,
			},
			expectedRecord: []byte{byte(FirstType), byte(LastType)},
		},
		{
			testName: "write medium data, with padding",
			data:     generateBytes(35 * 1024), // 35KB
			pageInfo: &Page{
				Id:              1,
				TotalBlockCount: 1,
				LastBlockSize:   32*1024 - 15,
			},
			expectedSize: 15*2 + 35*1024,
			expectedPos: &Position{
				PageId:      1,
				BlockNumber: 2,
				Offset:      0,
			},
			expectedPage: &Page{
				Id:              1,
				TotalBlockCount: 3,
				LastBlockSize:   2*15 + 3*1024,
			},
			expectedRecord: []byte{byte(FirstType), byte(LastType)},
		},
		{
			testName: "write big data, without padding",
			data:     generateBytes(62 * 1024), // 62KB
			pageInfo: &Page{
				Id:              1,
				TotalBlockCount: 1,
				LastBlockSize:   15 * 1024,
			},
			expectedSize: 15*3 + 62*1024,
			expectedPos: &Position{
				PageId:      1,
				BlockNumber: 1,
				Offset:      15 * 1024,
			},
			expectedPage: &Page{
				Id:              1,
				TotalBlockCount: 3,
				LastBlockSize:   3*15 + 13*1024,
			},
			expectedRecord: []byte{byte(FirstType), byte(MiddleType), byte(LastType)},
		},
		{
			testName: "write big data, with padding",
			data:     generateBytes(67 * 1024), // 67KB
			pageInfo: &Page{
				Id:              1,
				TotalBlockCount: 1,
				LastBlockSize:   32*1024 - 15,
			},
			expectedSize: 15*3 + 67*1024,
			expectedPos: &Position{
				PageId:      1,
				BlockNumber: 2,
				Offset:      0,
			},
			expectedPage: &Page{
				Id:              1,
				TotalBlockCount: 4,
				LastBlockSize:   3*15 + 3*1024,
			},
			expectedRecord: []byte{byte(FirstType), byte(MiddleType), byte(LastType)},
		},
		{
			testName: "write 2xbig data, without padding",
			data:     generateBytes(94 * 1024), // 94KB
			pageInfo: &Page{
				Id:              1,
				TotalBlockCount: 1,
				LastBlockSize:   15 * 1024,
			},
			expectedSize: 15*4 + 94*1024,
			expectedPos: &Position{
				PageId:      1,
				BlockNumber: 1,
				Offset:      15 * 1024,
			},
			expectedPage: &Page{
				Id:              1,
				TotalBlockCount: 4,
				LastBlockSize:   4*15 + 13*1024,
			},
			expectedRecord: []byte{byte(FirstType), byte(MiddleType), byte(MiddleType), byte(LastType)},
		},
		{
			testName: "write 2xbig data, with padding",
			data:     generateBytes(99 * 1024), // 99KB
			pageInfo: &Page{
				Id:              1,
				TotalBlockCount: 1,
				LastBlockSize:   32*1024 - 15,
			},
			expectedSize: 15*4 + 99*1024,
			expectedPos: &Position{
				PageId:      1,
				BlockNumber: 2,
				Offset:      0,
			},
			expectedPage: &Page{
				Id:              1,
				TotalBlockCount: 5,
				LastBlockSize:   4*15 + 3*1024,
			},
			expectedRecord: []byte{byte(FirstType), byte(MiddleType), byte(MiddleType), byte(LastType)},
		},
	}

	for i, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			storage := go_fs.NewInmemStorage()
			writer, _, err := storage.Create(go_fs.TypeWAL, int64(i))
			require.NoError(t, err)
			ctx := context.Background()
			p := tc.pageInfo
			p.writer = writer
			neededSpaces := estimateNeededSpaces(tc.data)
			padding := 0
			if p.LastBlockSize+headerSize >= defaultBlockSize {
				// need spaces for padded bytes
				padding = int(defaultBlockSize - p.LastBlockSize)
				neededSpaces += padding
			}
			buf := make([]byte, 0, neededSpaces)

			pos, size, err := p.writeToMemBuffer(ctx, tc.data, &buf)
			assert.Nil(t, err)
			assert.Equal(t, tc.expectedSize, size)
			assert.Equal(t, tc.expectedPos, pos)
			assert.Equal(t, tc.expectedPage.Id, p.Id)
			assert.Equal(t, tc.expectedPage.TotalBlockCount, p.TotalBlockCount)
			assert.Equal(t, tc.expectedPage.LastBlockSize, p.LastBlockSize)

			// assert buf output
			assertBuf(t, padding, buf, tc.data, tc.expectedRecord)

			err = storage.Close()
			require.NoError(t, err)
		})
	}
}

func assertBuf(t *testing.T, padding int, buf []byte, expectedData []byte, expectedRecord []byte) {
	assert.Equal(t, buf[:padding], make([]byte, padding), fmt.Sprintf("First %d bytes should be padded", padding))
	startBuf, startData, startRec := padding, 0, 0
	// Loop through header --> data --> header --> data ....
	for startData < len(expectedData) {
		header := buf[startBuf : startBuf+headerSize]
		dataLen := binary.LittleEndian.Uint16(header[4:6])

		// assert record type
		rec := header[6]
		assert.Equal(t, rec, expectedRecord[startRec], "Record should match")

		// assert data
		data := buf[startBuf+headerSize : startBuf+headerSize+int(dataLen)]
		assert.Equal(t, expectedData[startData:startData+int(dataLen)], data, "Data should match")

		startBuf += headerSize + int(dataLen)
		startData += int(dataLen)
		startRec += 1
	}

	left := len(buf) - startBuf
	assert.Equal(t, buf[startBuf:], make([]byte, left))
}

func generateBytes(n int) []byte {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		return nil
	}
	return b
}
