package integration

import (
	"testing"

	go_fs "github.com/datnguyenzzz/nogodb/lib/go-fs"
	go_sstable "github.com/datnguyenzzz/nogodb/lib/go-sstable"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
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

//// Validate the iterator seeking operations with very small (1) data
//func Test_Iterator_Seeking_Ops_Small(t *testing.T) {
//	inMemStorage := go_fs.NewInmemStorage()
//	fileWritable, _, err := inMemStorage.Create(go_fs.TypeTable, 0)
//	assert.NoError(t, err)
//	writer := go_sstable.NewWriter(
//		fileWritable,
//		common.TableV1,
//	)
//
//	sample := generateKV(1, true)
//	for _, kv := range sample {
//		err := writer.Set(kv.key, kv.value)
//		assert.NoError(t, err, "failed to set")
//	}
//
//	err = writer.Close()
//	assert.NoError(t, err)
//
//	// Evaluate the result of the seek operations
//	fileReadable, _, err := inMemStorage.Open(go_fs.TypeTable, 0)
//	assert.NoError(t, err)
//	var iterOpts []options.IteratorOptsFunc
//	iter, err := go_sstable.NewSingularIterator(
//		fileReadable,
//		iterOpts...,
//	)
//	assert.NoError(t, err)
//
//	defer func() {
//		err := iter.Close()
//		assert.NoError(t, err)
//	}()
//
//	// SeekGTE with an exact key
//	kv := iter.SeekGTE(sample[0].key)
//	assert.Equal(t, sample[0].key, kv.K.UserKey, "key returned by SeekGTE must be the same")
//	assert.Equal(t, sample[0].value, kv.V.Value(), "value returned by SeekGTE must be the same")
//
//	// SeekGTEPrefix with an exact key
//	//kv = iter.SeekPrefixGTE(sample[0].key, sample[0].key)
//	//assert.Equal(t, sample[0].key, kv.K.UserKey, "key returned by SeekPrefixGTE must be the same")
//	//assert.Equal(t, sample[0].value, kv.V.Value(), "value returned by SeekPrefixGTE must be the same")
//}

//func Test_Iterator_Seeking_Ops(t *testing.T) {
//	type param struct {
//		name      string
//		restart   int
//		blockSize int
//		isUnique  bool
//		cacheSize int // 0 means no cache
//	}
//
//	tests := []param{
//		{
//			name:      "small block, all keys are unique, disable block cache",
//			isUnique:  true,
//			restart:   5,
//			blockSize: 2 * kB,
//		},
//		{
//			name:      "small block, no unique are unique",
//			restart:   5,
//			blockSize: 2 * kB,
//		},
//		{
//			name:      "big block, all keys are unique",
//			isUnique:  true,
//			restart:   10,
//			blockSize: 4 * mB,
//		},
//		{
//			name:      "big block, no unique are unique",
//			restart:   10,
//			blockSize: 4 * mB,
//		},
//	}
//
//	for i, tc := range tests {
//		t.Run(tc.name, func(t *testing.T) {
//			// Init a table
//			inMemStorage := go_fs.NewInmemStorage()
//			fileWritable, _, err := inMemStorage.Create(go_fs.TypeTable, int64(i))
//			assert.NoError(t, err)
//			writer := go_sstable.NewWriter(
//				fileWritable,
//				common.TableV1,
//				go_sstable.WithBlockRestartInterval(tc.restart),
//				go_sstable.WithBlockSize(tc.blockSize),
//			)
//
//			sample := generateKV(testSize, tc.isUnique)
//			for _, kv := range sample {
//				err := writer.Set(kv.key, kv.value)
//				assert.NoError(t, err, "failed to set")
//			}
//
//			err = writer.Close()
//			assert.NoError(t, err)
//
//			// Evaluate the result of the seek operations
//			fileReadable, fd, err := inMemStorage.Open(go_fs.TypeTable, int64(i))
//			assert.NoError(t, err)
//			var iterOpts []options.IteratorOptsFunc
//			if tc.cacheSize > 0 {
//				iterOpts = []options.IteratorOptsFunc{
//					options.WithBlockCache(go_block_cache.LRU, fd),
//					options.WithBlockCacheSize(int64(tc.cacheSize)),
//				}
//			}
//			iter, err := go_sstable.NewSingularIterator(
//				fileReadable,
//				iterOpts...,
//			)
//			assert.NoError(t, err)
//
//			defer func() {
//				err := iter.Close()
//				assert.NoError(t, err)
//			}()
//
//			// SeekGTE with an exact key
//			for _, expectedKV := range sample {
//				kv := iter.SeekGTE(expectedKV.key)
//				assert.Equal(t, expectedKV.key, kv.K.UserKey)
//				assert.Equal(t, expectedKV.value, kv.V.Value())
//			}
//		})
//	}
//}
