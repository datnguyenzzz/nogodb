package integration

import (
	"testing"

	go_block_cache "github.com/datnguyenzzz/nogodb/lib/go-block-cache"
	go_fs "github.com/datnguyenzzz/nogodb/lib/go-fs"
	go_sstable "github.com/datnguyenzzz/nogodb/lib/go-sstable"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/options"
	"github.com/stretchr/testify/assert"
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

func Test_Iterator_Seeking_Ops(t *testing.T) {
	type param struct {
		name       string
		restart    int
		blockSize  int
		isUnique   bool
		cacheSize  int // 0 means no cache
		sampleSize int
	}

	tests := []param{
		{
			name:       "volume = 1, small block, disable block cache",
			isUnique:   true,
			restart:    5,
			blockSize:  2 * kB,
			sampleSize: 1,
		},
		//{
		//	name:      "small block, all keys are unique, disable block cache",
		//	isUnique:  true,
		//	restart:   5,
		//	blockSize: 2 * kB,
		//},
		//{
		//	name:      "small block, no unique are unique",
		//	restart:   5,
		//	blockSize: 2 * kB,
		//},
		//{
		//	name:      "big block, all keys are unique",
		//	isUnique:  true,
		//	restart:   10,
		//	blockSize: 4 * mB,
		//},
		//{
		//	name:      "big block, no unique are unique",
		//	restart:   10,
		//	blockSize: 4 * mB,
		//},
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
				go_sstable.WithBlockSize(tc.blockSize),
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
			iter, err := go_sstable.NewSingularIterator(
				fileReadable,
				iterOpts...,
			)
			assert.NoError(t, err)

			defer func() {
				err := iter.Close()
				assert.NoError(t, err)
			}()

			for _, expectedKV := range sample {
				// SeekGTE with an exact key
				kv := iter.SeekGTE(expectedKV.key)
				assert.Equal(t, expectedKV.key, kv.K.UserKey, "SeekGTE with an exact key: key must match")
				assert.Equal(t, expectedKV.value, kv.V.Value(), "SeekGTE with an exact key: value must match")

				// SeekGTE with smaller key
				k := make([]byte, len(expectedKV.key))
				copy(k, expectedKV.key)
				k[0] -= 1
				kv = iter.SeekGTE(k)
				assert.Equal(t, expectedKV.key, kv.K.UserKey, "SeekGTE with smaller key: key must match")
				assert.Equal(t, expectedKV.value, kv.V.Value(), "SeekGTE with smaller key: key must value")
			}
		})
	}
}
