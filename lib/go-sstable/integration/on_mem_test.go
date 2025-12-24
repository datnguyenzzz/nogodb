package integration

import (
	"bytes"
	"fmt"
	"testing"

	go_block_cache "github.com/datnguyenzzz/nogodb/lib/go-block-cache"
	"github.com/datnguyenzzz/nogodb/lib/go-bytesbufferpool/predictable_size"
	go_fs "github.com/datnguyenzzz/nogodb/lib/go-fs"
	go_sstable "github.com/datnguyenzzz/nogodb/lib/go-sstable"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/options"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testSize = 10_000
)

const (
	kB = 1024
	mB = kB * 1024
)

func Test_Integration_Writer_No_Errors(t *testing.T) {
	type param struct {
		name      string
		restart   int
		blockSize int
		isUnique  bool
		operation string
	}

	tests := []param{
		{
			name:      "small block, all keys are unique",
			isUnique:  true,
			restart:   5,
			blockSize: 2 * kB,
			operation: "SET",
		},
		{
			name:      "small block, no unique are unique",
			restart:   5,
			blockSize: 2 * kB,
			operation: "SET",
		},
		{
			name:      "big block, all keys are unique",
			isUnique:  true,
			restart:   10,
			blockSize: 4 * mB,
			operation: "SET",
		},
		{
			name:      "big block, no unique are unique",
			restart:   10,
			blockSize: 4 * mB,
			operation: "SET",
		},
		{
			name:      "small block, all keys are unique",
			isUnique:  true,
			restart:   5,
			blockSize: 2 * kB,
			operation: "DEL",
		},
		{
			name:      "small block, no unique are unique",
			restart:   5,
			blockSize: 2 * kB,
			operation: "DEL",
		},
		{
			name:      "big block, all keys are unique",
			isUnique:  true,
			restart:   10,
			blockSize: 4 * mB,
			operation: "DEL",
		},
		{
			name:      "big block, no unique are unique",
			restart:   10,
			blockSize: 4 * mB,
			operation: "DEL",
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			inMemStorage := go_fs.NewInmemStorage()
			fileWritable, _, err := inMemStorage.Create(go_fs.TypeTable, int64(i))
			assert.NoError(t, err)
			writer := go_sstable.NewWriter(
				fileWritable,
				common.TableV1,
				go_sstable.WithBlockRestartInterval(tc.restart),
				go_sstable.WithBlockSize(tc.blockSize),
			)

			defer func() {
				err := writer.Close()
				assert.NoError(t, err)
			}()

			sample := generateKV(testSize, tc.isUnique)
			for _, kv := range sample {
				switch tc.operation {
				case "SET":
					err := writer.Set(kv.key, kv.value)
					assert.NoError(t, err, "failed to set")
				case "DEL":
					err := writer.Delete(kv.key)
					assert.NoError(t, err, "failed to delete")
				}
			}
		})
	}
}

func Test_Iterator_Seeking_Ops_single_table(t *testing.T) {
	type param struct {
		name        string
		restart     int
		isUnique    bool
		cacheSize   int // 0 means no cache
		sampleSize  int
		asyncWriter bool
	}

	tests := []param{
		{
			name:        "volume = 1,  block cache disabled",
			isUnique:    true,
			restart:     5,
			sampleSize:  1,
			asyncWriter: false,
		},
		{
			name:        "volume = 1,  block cache enabled",
			isUnique:    true,
			restart:     5,
			cacheSize:   2 * kB,
			sampleSize:  1,
			asyncWriter: false,
		},
		{
			name:        "synchronous writer, volume = 100_000, all keys are unique, block cache disable",
			isUnique:    true,
			restart:     5,
			sampleSize:  100_000,
			asyncWriter: false,
		},
		{
			name:        "synchronous writer, volume = 100_000, keys are shared prefix, block cache disable",
			isUnique:    false,
			restart:     5,
			sampleSize:  100_000,
			asyncWriter: false,
		},
		{
			name:        "synchronous writer, volume = 100_000, all keys are unique, block cache enabled",
			isUnique:    true,
			restart:     5,
			sampleSize:  100_000,
			asyncWriter: false,
			cacheSize:   1 * mB,
		},
		{
			name:        "synchronous writer, volume = 100_000, keys are shared prefix, block cache enabled",
			isUnique:    false,
			restart:     5,
			sampleSize:  100_000,
			asyncWriter: false,
			cacheSize:   1 * mB,
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Init a table
			inMemStorage := go_fs.NewInmemStorage()
			fileWritable, _, err := inMemStorage.Create(go_fs.TypeTable, int64(i))
			assert.NoError(t, err)
			writer := go_sstable.NewWriter(
				fileWritable,
				common.TableV1,
				go_sstable.WithBlockRestartInterval(tc.restart),
				go_sstable.WithBlockSize(2*kB),
			)

			sample := generateKV(tc.sampleSize, tc.isUnique)
			for _, kv := range sample {
				err := writer.Set(kv.key, kv.value)
				assert.NoError(t, err, "failed to set")
			}

			err = writer.Close()
			assert.NoError(t, err)

			// Evaluate the result of the seek operations
			fileReadable, fd, err := inMemStorage.Open(go_fs.TypeTable, int64(i))
			assert.NoError(t, err)
			var iterOpts []options.IteratorOptsFunc
			if tc.cacheSize > 0 {
				iterOpts = []options.IteratorOptsFunc{
					options.WithBlockCache(go_block_cache.LRU, fd),
					options.WithBlockCacheSize(int64(tc.cacheSize)),
				}
			}
			sharedBufferPool := predictable_size.NewPredictablePool()
			iter, err := go_sstable.NewSingularIterator(
				sharedBufferPool,
				fileReadable,
				iterOpts...,
			)
			require.NoError(t, err)

			defer func() {
				err := iter.Close()
				assert.NoError(t, err)
			}()

			for i, expectedKV := range sample {
				// SeekGTE with an exact key
				kv := iter.SeekGTE(expectedKV.key)
				require.NotNil(t, kv, fmt.Sprintf("SeekGTE with an exact key must found, test case #%d", i))
				assert.Zero(t, bytes.Compare(expectedKV.key, kv.K.UserKey), fmt.Sprintf("SeekGTE with smaller key: key must match, test case #%d", i))
				assert.Zero(t, bytes.Compare(expectedKV.value, kv.V.Value()), fmt.Sprintf("SeekGTE with smaller key: key must value, test case #%d", i))
				// SeekGTEPrefix with an exact key
				kv = iter.SeekPrefixGTE(expectedKV.key, expectedKV.key)
				require.NotNil(t, kv, fmt.Sprintf("SeekGTEPrefix with an exact key must found, test case #%d", i))
				assert.Zero(t, bytes.Compare(expectedKV.key, kv.K.UserKey), fmt.Sprintf("SeekGTEPrefix with an exact key: key must match, test case #%d", i))
				assert.Zero(t, bytes.Compare(expectedKV.value, kv.V.Value()), fmt.Sprintf("SeekGTEPrefix with an exact key: key must value, test case #%d", i))
				// SeekGTE with smaller key
				var prevKV kvType
				if i > 0 {
					prevKV = sample[i-1]
				}
				k := generateBytes(prevKV.key, expectedKV.value)
				assert.Less(t, bytes.Compare(k, expectedKV.key), 0, fmt.Sprintf("a test key must be < than the current key, test case #%d", i))
				assert.Greater(t, bytes.Compare(k, prevKV.key), 0, fmt.Sprintf("a test key must be > than the previous key, test case #%d", i))
				kv = iter.SeekGTE(k)
				require.NotNil(t, kv, fmt.Sprintf("SeekGTE with smaller key must found, test case #%d", i))
				assert.Zero(t, bytes.Compare(expectedKV.key, kv.K.UserKey), fmt.Sprintf("SeekGTE with smaller key: key must match, test case #%d", i))
				assert.Zero(t, bytes.Compare(expectedKV.value, kv.V.Value()), fmt.Sprintf("SeekGTE with smaller key: key must value, test case #%d", i))
			}

			// seek GTE with a key that is out of range of the SST
			k := make([]byte, len(sample[len(sample)-1].key))
			copy(k, sample[len(sample)-1].key)
			k[0] += 1
			kv := iter.SeekGTE(k)
			assert.Nil(t, kv)
		})
	}
}
