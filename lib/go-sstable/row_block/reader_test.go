package row_block

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/datnguyenzzz/nogodb/lib/go-bytesbufferpool/predictable_size"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common/block"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/compression"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/storage"
	"github.com/stretchr/testify/assert"
)

var (
	defaultCompressor = compression.NewCompressor(compression.SnappyCompression)
	defaultChecksumer = common.NewChecksumer(common.CRC32Checksum)
)

type mockStorageReader struct {
	stored  []byte
	mockErr error
}

func (m *mockStorageReader) ReadAt(p []byte, off uint64) error {
	if m.mockErr != nil {
		return m.mockErr
	}
	copy(p, m.stored)
	return nil
}
func (m *mockStorageReader) Close() error {
	// ignore for now
	return nil
}

var _ storage.ILayoutReader = (*MockLayoutReader)(nil)

func Test_Read(t *testing.T) {
	type params struct {
		desc        string
		size        int
		readerError error
		corrupted   bool
	}

	tests := []params{
		{
			desc:        "failed to read from storage",
			readerError: fmt.Errorf("failed to read from storage"),
		},
		{
			desc: "non-corrupted small data - 10B",
			size: 10,
		},
		{
			desc: "non-corrupted medium data - 2 KiB",
			size: 2 * 1024,
		},
		{
			desc: "non-corrupted large data - 2 MiB",
			size: 2 * 1024 * 1024,
		},
		{
			desc:      "corrupted data",
			size:      2 * 1024,
			corrupted: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			original := randomBytes(tc.size)
			pb := compressToPb(defaultCompressor, defaultChecksumer, original)
			stored := append(pb.Data, pb.Trailer[:]...)
			if tc.corrupted {
				stored[len(stored)-1] += 1
			}
			mStorageReader := &mockStorageReader{
				stored:  stored,
				mockErr: tc.readerError,
			}

			r := &RowBlockReader{}
			r.Init(predictable_size.NewPredictablePool(), mStorageReader)

			val, err := r.Read(&block.BlockHandle{
				Offset: 0,
				Length: uint64(len(stored)),
			}, block.BlockKindData)

			if tc.corrupted {
				assert.ErrorIs(t, err, common.MismatchedChecksumError)
			} else if tc.readerError != nil {
				assert.ErrorIs(t, err, tc.readerError)
			} else {
				assert.NoError(t, err)

				assert.Equal(t, val.ValueSource, common.ValueFromBuffer)
				assert.Equal(t, original, val.Value())
			}
		})
	}
}

func randomBytes(n int) []byte {
	res := make([]byte, n)
	for i := 0; i < n; i++ {
		res[i] = byte(rand.Intn(255))
	}
	return res
}
