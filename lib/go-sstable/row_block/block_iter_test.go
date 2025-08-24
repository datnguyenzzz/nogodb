package row_block

import (
	"testing"

	"github.com/datnguyenzzz/nogodb/lib/go-bytesbufferpool/predictable_size"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	"github.com/stretchr/testify/assert"
)

func TestDataBlockIterator_readEntry(t *testing.T) {
	type testEntry struct {
		offset        uint64
		expectedKey   string
		expectedValue string
	}

	type testCase struct {
		desc            string
		inputUserKeys   []string
		inputValues     []string
		restartInterval int
		testEntries     []testEntry
	}

	tests := []testCase{
		{
			desc: "#1 - restart interval 2, test multiple entries",
			inputUserKeys: []string{
				"apple", "apricot", "avocado", "avowed", "cherry", "mango",
			},
			inputValues: []string{
				"red", "orange", "purple", "yellow", "red", "blue",
			},
			restartInterval: 2,
			testEntries: []testEntry{
				{offset: 0, expectedKey: "apple", expectedValue: "red"},
				{offset: 19, expectedKey: "ricot", expectedValue: "orange"},
				{offset: 41, expectedKey: "avocado", expectedValue: "purple"},
			},
		},
		{
			desc: "#2 - restart interval 1, all entries are restart points",
			inputUserKeys: []string{
				"banana", "berry", "blueberry",
			},
			inputValues: []string{
				"yellow", "red", "blue",
			},
			restartInterval: 1,
			testEntries: []testEntry{
				{offset: 0, expectedKey: "banana", expectedValue: "yellow"},
				{offset: 23, expectedKey: "berry", expectedValue: "red"},
				{offset: 42, expectedKey: "blueberry", expectedValue: "blue"},
			},
		},
		{
			desc: "#3 - restart interval 3, mixed restart and compressed entries",
			inputUserKeys: []string{
				"grape", "grapefruit", "guava", "kiwi", "lemon", "lime",
			},
			inputValues: []string{
				"purple", "pink", "green", "brown", "yellow", "green",
			},
			restartInterval: 3,
			testEntries: []testEntry{
				{offset: 0, expectedKey: "grape", expectedValue: "purple"},
				{offset: 22, expectedKey: "fruit", expectedValue: "pink"},
				{offset: 42, expectedKey: "uava", expectedValue: "green"},
				{offset: 62, expectedKey: "kiwi", expectedValue: "brown"},
			},
		},
		{
			desc: "#4 - single entry",
			inputUserKeys: []string{
				"strawberry",
			},
			inputValues: []string{
				"red",
			},
			restartInterval: 5,
			testEntries: []testEntry{
				{offset: 0, expectedKey: "strawberry", expectedValue: "red"},
			},
		},
		{
			desc: "#5 - large restart interval",
			inputUserKeys: []string{
				"orange", "papaya", "peach", "pear",
			},
			inputValues: []string{
				"orange", "orange", "pink", "green",
			},
			restartInterval: 4,
			testEntries: []testEntry{
				{offset: 0, expectedKey: "orange", expectedValue: "orange"},
				{offset: 23, expectedKey: "papaya", expectedValue: "orange"},
				{offset: 46, expectedKey: "each", expectedValue: "pink"},
				{offset: 65, expectedKey: "r", expectedValue: "green"},
			},
		},
		{
			desc: "#6 - empty values",
			inputUserKeys: []string{
				"key1", "key2", "key3",
			},
			inputValues: []string{
				"", "", "",
			},
			restartInterval: 2,
			testEntries: []testEntry{
				{offset: 0, expectedKey: "key1", expectedValue: ""},
				{offset: 15, expectedKey: "2", expectedValue: ""},
			},
		},
		{
			desc: "#7 - keys with common prefixes",
			inputUserKeys: []string{
				"prefix_key_001", "prefix_key_002", "prefix_key_003", "prefix_key_004",
			},
			inputValues: []string{
				"value1", "value2", "value3", "value4",
			},
			restartInterval: 3,
			testEntries: []testEntry{
				{offset: 0, expectedKey: "prefix_key_001", expectedValue: "value1"},
				{offset: 31, expectedKey: "2", expectedValue: "value2"},
				{offset: 49, expectedKey: "3", expectedValue: "value3"},
				{offset: 67, expectedKey: "prefix_key_004", expectedValue: "value4"},
			},
		},
		{
			desc: "#8 - sequential read all entries",
			inputUserKeys: []string{
				"apple", "apricot", "avocado", "banana", "cherry", "grape",
			},
			inputValues: []string{
				"red", "orange", "green", "yellow", "red", "purple",
			},
			restartInterval: 2,
			testEntries: []testEntry{
				{offset: 0, expectedKey: "apple", expectedValue: "red"},
				{offset: 19, expectedKey: "ricot", expectedValue: "orange"},
				{offset: 41, expectedKey: "avocado", expectedValue: "green"},
				{offset: 64, expectedKey: "banana", expectedValue: "yellow"},
				{offset: 87, expectedKey: "cherry", expectedValue: "red"},
				{offset: 107, expectedKey: "grape", expectedValue: "purple"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			// 1. Create block data using the existing writer
			bp := predictable_size.NewPredictablePool()
			blk := newBlock(tc.restartInterval, bp)

			// Write all entries to the block
			for i, userKey := range tc.inputUserKeys {
				key := makeDummyKey(userKey)
				value := []byte(tc.inputValues[i])
				err := blk.WriteEntry(key, value)
				assert.NoError(t, err, "should not have error writing entry")
			}

			// Finalize the block
			blockData := make([]byte, blk.EstimateSize())
			blk.Finish(blockData)

			// 2. Create iterator and test readEntry on specific offsets
			cmp := common.NewComparer()

			for _, entry := range tc.testEntries {
				iter := NewBlockIterator(cmp, blockData)
				iter.offset = entry.offset

				iter.readEntry()
				actualKey := common.DeserializeKey(iter.key)
				assert.Equal(t, entry.expectedKey, string(actualKey.UserKey), "key should match at offset %d", entry.offset)
				assert.Equal(t, entry.expectedValue, string(iter.value), "value should match at offset %d", entry.offset)
				assert.Greater(t, iter.nextOffset, iter.offset, "nextOffset should be greater than current offset")
				assert.LessOrEqual(t, iter.nextOffset, iter.trailerOffset, "nextOffset should not exceed trailer offset")
			}
		})
	}
}

func TestDataBlockIterator_First(t *testing.T) {
	type testCase struct {
		desc               string
		inputUserKeys      []string
		inputValues        []string
		restartInterval    int
		expectedFirstKey   string
		expectedFirstValue string
	}

	tests := []testCase{
		{
			desc: "#1 - restart interval 2",
			inputUserKeys: []string{
				"apple", "apricot", "avocado", "avowed", "cherry", "mango",
			},
			inputValues: []string{
				"red", "orange", "purple", "yellow", "red", "blue",
			},
			restartInterval:    2,
			expectedFirstKey:   "apple",
			expectedFirstValue: "red",
		},
		{
			desc: "#2 - restart interval 1",
			inputUserKeys: []string{
				"banana", "berry", "blueberry",
			},
			inputValues: []string{
				"yellow", "red", "blue",
			},
			restartInterval:    1,
			expectedFirstKey:   "banana",
			expectedFirstValue: "yellow",
		},
		{
			desc: "#3 - restart interval 3",
			inputUserKeys: []string{
				"grape", "grapefruit", "guava", "kiwi", "lemon", "lime",
			},
			inputValues: []string{
				"purple", "pink", "green", "brown", "yellow", "green",
			},
			restartInterval:    3,
			expectedFirstKey:   "grape",
			expectedFirstValue: "purple",
		},
		{
			desc: "#4 - single entry",
			inputUserKeys: []string{
				"strawberry",
			},
			inputValues: []string{
				"red",
			},
			restartInterval:    5,
			expectedFirstKey:   "strawberry",
			expectedFirstValue: "red",
		},
		{
			desc: "#5 - large restart interval",
			inputUserKeys: []string{
				"orange", "papaya", "peach", "pear",
			},
			inputValues: []string{
				"orange", "orange", "pink", "green",
			},
			restartInterval:    4,
			expectedFirstKey:   "orange",
			expectedFirstValue: "orange",
		},
		{
			desc: "#6 - empty first value",
			inputUserKeys: []string{
				"key1", "key2", "key3",
			},
			inputValues: []string{
				"", "value2", "value3",
			},
			restartInterval:    2,
			expectedFirstKey:   "key1",
			expectedFirstValue: "",
		},
		{
			desc: "#7 - keys with common prefixes",
			inputUserKeys: []string{
				"prefix_key_001", "prefix_key_002", "prefix_key_003", "prefix_key_004",
			},
			inputValues: []string{
				"value1", "value2", "value3", "value4",
			},
			restartInterval:    3,
			expectedFirstKey:   "prefix_key_001",
			expectedFirstValue: "value1",
		},
		{
			desc: "#8 - very long first key and value",
			inputUserKeys: []string{
				"verylongkeyname1234567890abcdefghijklmnopqrstuvwxyz",
				"short",
				"medium_key",
			},
			inputValues: []string{
				"verylongvaluecontent1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
				"small",
				"medium_value",
			},
			restartInterval:    2,
			expectedFirstKey:   "verylongkeyname1234567890abcdefghijklmnopqrstuvwxyz",
			expectedFirstValue: "verylongvaluecontent1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			// 1. Create block data using the existing writer
			bp := predictable_size.NewPredictablePool()
			blk := newBlock(tc.restartInterval, bp)

			for i, userKey := range tc.inputUserKeys {
				key := makeDummyKey(userKey)
				value := []byte(tc.inputValues[i])
				err := blk.WriteEntry(key, value)
				assert.NoError(t, err, "should not have error writing entry")
			}

			blockData := make([]byte, blk.EstimateSize())
			blk.Finish(blockData)

			// 2. Create iterator and test First()
			cmp := common.NewComparer()
			iter := NewBlockIterator(cmp, blockData)

			firstKV := iter.First()

			assert.NotNil(t, firstKV, "First() should return non-nil InternalKV")
			assert.Equal(t, tc.expectedFirstKey, string(firstKV.K.UserKey), "First() should return the first key")
			assert.Equal(t, tc.expectedFirstValue, string(firstKV.V), "First() should return the first value")
			assert.Equal(t, uint64(0), iter.offset, "iterator offset should be 0 after First()")
			assert.Greater(t, iter.nextOffset, uint64(0), "nextOffset should be greater than 0 after First()")
			assert.LessOrEqual(t, iter.nextOffset, iter.trailerOffset, "nextOffset should not exceed trailer offset")

			// Verify iterator's internal key and value are set
			actualIterKey := common.DeserializeKey(iter.key)
			assert.Equal(t, tc.expectedFirstKey, string(actualIterKey.UserKey), "iterator's internal key should match first key")
			assert.Equal(t, tc.expectedFirstValue, string(iter.value), "iterator's internal value should match first value")
		})
	}
}
