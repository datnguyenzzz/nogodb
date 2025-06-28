package row_block

import (
	"testing"

	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	"github.com/stretchr/testify/assert"
)

func Test_WriteEntry_Then_Finish(t *testing.T) {
	type param struct {
		desc          string
		inputUserKeys []string
		inputValue    []string
	}

	tests := []param{
		{
			desc: "#1",
			inputUserKeys: []string{
				"apple", "apricot", "avocado", "avowed", "cherry", "mango",
			},
			inputValue: []string{
				"red", "orange", "purple", "yellow", "red", "blue",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			blk := newBlock(2)
			n := len(tc.inputUserKeys)
			for i := 0; i < n; i++ {
				key := makeKey(tc.inputUserKeys[i])
				value := []byte(tc.inputValue[i])
				// 1. Write a new entry, expect there is no error
				err := blk.WriteEntry(key, value)
				assert.NoError(t, err, "should not have an error when writing entry")

				// 2. Assert current key / value
				currKey := blk.CurKey()
				assert.Equal(t, key.UserKey, currKey.UserKey, "user key should be equal")
				assert.Equal(t, value, blk.currValue, "value should be equal")

				// 3. Assert entries count
				assert.Equal(t, i+1, blk.EntryCount(), "entry count should be equal")
			}
			blkData := make([]byte, blk.EstimateSize())
			blk.Finish(blkData)

			// 4. Ensure the block has been reset, but don't lose the allocated buffer
			assert.Zero(t, blk.EntryCount())
			assert.Zero(t, blk.nextRestartEntry)
			assert.Zero(t, len(blk.restartOffset))
			assert.Zero(t, len(blk.curKey))
			assert.Zero(t, len(blk.prevKey))
			assert.Zero(t, len(blk.currValue))
			assert.Zero(t, len(blk.buf))
			assert.Greater(t, cap(blk.buf), 0)
			assert.Greater(t, cap(blk.restartOffset), 0)
			assert.Greater(t, cap(blk.currValue), 0)
			assert.Greater(t, cap(blk.curKey), 0)

			// 5. Assert the data block
			expectedBlkData := []byte(
				"\x00\x0d\x03apple\x00\x00\x00\x00\x00\x00\x00\x00red" +
					"\x02\x0d\x06ricot\x00\x00\x00\x00\x00\x00\x00\x00orange" +
					"\x00\x0f\x06avocado\x00\x00\x00\x00\x00\x00\x00\x00purple" +
					"\x03\x0b\x06wed\x00\x00\x00\x00\x00\x00\x00\x00yellow" +
					"\x00\x0e\x03cherry\x00\x00\x00\x00\x00\x00\x00\x00red" +
					"\x00\x0d\x04mango\x00\x00\x00\x00\x00\x00\x00\x00blue" +
					// block trailer [0, 41, 85] 3, we use little endian
					"\x00\x00\x00\x00\x29\x00\x00\x00\x55\x00\x00\x00\x03\x00\x00\x00")

			assert.Equal(t, expectedBlkData, blkData)
		})
	}
}

func makeKey(userKey string) common.InternalKey {
	return common.MakeKey([]byte(userKey), 0, common.KeyKindUnknown)
}
