//go:build functional_tests

package functional

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"

	go_block_cache "github.com/datnguyenzzz/nogodb/lib/go-block-cache"
	"github.com/datnguyenzzz/nogodb/lib/go-bytesbufferpool/predictable_size"
	go_fs "github.com/datnguyenzzz/nogodb/lib/go-fs"
	go_sstable "github.com/datnguyenzzz/nogodb/lib/go-sstable"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/options"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/sync/errgroup"
)

const (
	testSize = 10_000
)

const (
	kB = 1024
	mB = kB * 1024
)

type WalSuite struct {
	suite.Suite
}

func (w *WalSuite) Test_Integration_Writer_No_Errors() {
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

	t := w.T()
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

func (w *WalSuite) Test_Iterator_Seeking_Ops_single_table() {
	type param struct {
		name                string
		restart             int
		isUnique            bool
		cacheSize           int // 0 means no cache
		sampleSize          int
		isRandomisedOrdered bool
	}

	tests := []param{
		// Sequence read
		{
			name:       "volume = 1,  block cache disabled",
			isUnique:   true,
			restart:    5,
			sampleSize: 1,
		},
		{
			name:       "volume = 1,  block cache enabled",
			isUnique:   true,
			restart:    5,
			cacheSize:  2 * kB,
			sampleSize: 1,
		},
		{
			name:       "volume = 100_000, all keys are unique, block cache disable",
			isUnique:   true,
			restart:    5,
			sampleSize: 100_000,
		},
		{
			name:       "volume = 100_000, keys are shared prefix, block cache disable",
			isUnique:   false,
			restart:    5,
			sampleSize: 100_000,
		},
		{
			name:       "volume = 100_000, all keys are unique, block cache enabled",
			isUnique:   true,
			restart:    5,
			sampleSize: 100_000,
			cacheSize:  1 * mB,
		},
		{
			name:       "volume = 100_000, keys are shared prefix, block cache enabled",
			isUnique:   false,
			restart:    5,
			sampleSize: 100_000,
			cacheSize:  1 * mB,
		},
		// Randomised-order read
		{
			name:                "volume = 100_000, all keys are unique, block cache disable, randomised read order",
			isUnique:            true,
			restart:             5,
			sampleSize:          100_000,
			isRandomisedOrdered: true,
		},
		{
			name:                "volume = 100_000, keys are shared prefix, block cache disable, randomised read order",
			isUnique:            false,
			restart:             5,
			sampleSize:          100_000,
			isRandomisedOrdered: true,
		},
		{
			name:                "volume = 100_000, all keys are unique, block cache enabled, randomised read order",
			isUnique:            true,
			restart:             5,
			sampleSize:          100_000,
			cacheSize:           1 * mB,
			isRandomisedOrdered: true,
		},
		{
			name:                "volume = 100_000, keys are shared prefix, block cache enabled, randomised read order",
			isUnique:            false,
			restart:             5,
			sampleSize:          100_000,
			cacheSize:           1 * mB,
			isRandomisedOrdered: true,
		},
	}

	t := w.T()
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
			fileReadable, fd, err := inMemStorage.Open(go_fs.TypeTable, int64(i), 0)
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

			for i := 0; i < len(sample); i++ {
				idx := i
				if tc.isRandomisedOrdered {
					r := rand.New(rand.NewSource(time.Now().UnixNano()))
					idx = int(r.Int31n(int32(len(sample))))
				}
				// SeekGTE with an exact key
				var prevKV kvType
				if idx > 0 {
					prevKV = sample[idx-1]
				}
				validateSeekGTE(t, iter, sample[idx], prevKV, idx)
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

func (w *WalSuite) Test_Iterator_Concurrently_Seeking_Ops_multiple_tables() {
	type param struct {
		name                string
		restart             int
		isUnique            bool
		cacheSize           int // 0 means no cache
		sampleSize          int
		isRandomisedOrdered bool
	}

	tests := []param{
		// Sequence read
		{
			name:       "volume = 100_000, all keys are unique, block cache disable",
			isUnique:   true,
			restart:    5,
			sampleSize: 100_000,
		},
		{
			name:       "volume = 100_000, keys are shared prefix, block cache disable",
			isUnique:   false,
			restart:    5,
			sampleSize: 100_000,
		},
		{
			name:       "volume = 100_000, all keys are unique, block cache enabled",
			isUnique:   true,
			restart:    5,
			sampleSize: 100_000,
			cacheSize:  1 * mB,
		},
		{
			name:       "volume = 100_000, keys are shared prefix, block cache enabled",
			isUnique:   false,
			restart:    5,
			sampleSize: 100_000,
			cacheSize:  1 * mB,
		},
		// Randomised-order read
		{
			name:                "volume = 100_000, all keys are unique, block cache disable, randomised read order",
			isUnique:            true,
			restart:             5,
			sampleSize:          100_000,
			isRandomisedOrdered: true,
		},
		{
			name:                "volume = 100_000, keys are shared prefix, block cache disable, randomised read order",
			isUnique:            false,
			restart:             5,
			sampleSize:          100_000,
			isRandomisedOrdered: true,
		},
		{
			name:                "volume = 100_000, all keys are unique, block cache enabled, randomised read order",
			isUnique:            true,
			restart:             5,
			sampleSize:          100_000,
			cacheSize:           1 * mB,
			isRandomisedOrdered: true,
		},
		{
			name:                "volume = 100_000, keys are shared prefix, block cache enabled, randomised read order",
			isUnique:            false,
			restart:             5,
			sampleSize:          100_000,
			cacheSize:           1 * mB,
			isRandomisedOrdered: true,
		},
	}

	numberOfSSTs := 3

	t := w.T()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Init a table
			inMemStorage := go_fs.NewInmemStorage()
			sample := make([][]kvType, 0, numberOfSSTs)
			for sst := 1; sst <= numberOfSSTs; sst++ {
				fileWritable, _, err := inMemStorage.Create(go_fs.TypeTable, int64(sst))
				assert.NoError(t, err)
				writer := go_sstable.NewWriter(
					fileWritable,
					common.TableV1,
					go_sstable.WithBlockRestartInterval(tc.restart),
					go_sstable.WithBlockSize(2*kB),
				)

				kvs := generateKV(tc.sampleSize, tc.isUnique)
				sample = append(sample, kvs)
				for _, kv := range kvs {
					err := writer.Set(kv.key, kv.value)
					assert.NoError(t, err, "failed to set")
				}

				err = writer.Close()
				assert.NoError(t, err)
			}

			// Evaluate the result of the seek operations
			eg := errgroup.Group{}
			for sst := 1; sst <= numberOfSSTs; sst++ {
				eg.Go(func() error {
					kvs := sample[sst-1]
					fileReadable, fd, err := inMemStorage.Open(go_fs.TypeTable, int64(sst), 0)
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

					for i := 0; i < len(kvs); i++ {
						idx := i
						if tc.isRandomisedOrdered {
							r := rand.New(rand.NewSource(time.Now().UnixNano()))
							idx = int(r.Int31n(int32(len(kvs))))
						}
						// SeekGTE with an exact key
						var prevKV kvType
						if idx > 0 {
							prevKV = kvs[idx-1]
						}
						validateSeekGTE(t, iter, kvs[idx], prevKV, idx)
					}

					// seek GTE with a key that is out of range of the SST
					k := make([]byte, len(kvs[len(kvs)-1].key))
					copy(k, kvs[len(kvs)-1].key)
					k[0] += 1
					kv := iter.SeekGTE(k)
					assert.Nil(t, kv)
					return nil
				})
			}

			err := eg.Wait()
			assert.NoError(t, err)
		})
	}
}

func validateSeekGTE(t *testing.T, iter go_sstable.IIterator, expectedKV kvType, prevKV kvType, i int) {
	kv := iter.SeekGTE(expectedKV.key)
	require.NotNil(t, kv, fmt.Sprintf("SeekGTE with an exact key must found, test case #%d", i))
	assertKv(t, expectedKV, kv, i)
	assert.Zero(t, bytes.Compare(expectedKV.value, kv.V.Value()), fmt.Sprintf("SeekGTE with smaller key: key must value, test case #%d. Expected: %v, actual: %v", i, expectedKV.value, kv.V.Value()))
	// SeekGTEPrefix with an exact key
	kv = iter.SeekPrefixGTE(expectedKV.key, expectedKV.key)
	require.NotNil(t, kv, fmt.Sprintf("SeekGTEPrefix with an exact key must found, test case #%d", i))
	assertKv(t, expectedKV, kv, i)
	// SeekGTE with smaller key

	k := generateBytes(prevKV.key, expectedKV.value)
	assert.Less(t, bytes.Compare(k, expectedKV.key), 0, fmt.Sprintf("a test key must be < than the current key, test case #%d.", i))
	assert.Greater(t, bytes.Compare(k, prevKV.key), 0, fmt.Sprintf("a test key must be > than the previous key, test case #%d.", i))
	kv = iter.SeekGTE(k)
	require.NotNil(t, kv, fmt.Sprintf("SeekGTE with smaller key must found, test case #%d", i))
	assertKv(t, expectedKV, kv, i)
}

func assertKv(t *testing.T, expectedKV kvType, kv *common.InternalKV, i int) {
	assert.Zero(t, bytes.Compare(expectedKV.key, kv.K.UserKey), fmt.Sprintf("SeekGTE with smaller key: key must match, test case #%d. Expected: %v, actual: %v", i, expectedKV.key, kv.K.UserKey))
	assert.Zero(t, bytes.Compare(expectedKV.value, kv.V.Value()), fmt.Sprintf("SeekGTE with smaller key: key must value, test case #%d. Expected: %v, actual: %v", i, expectedKV.value, kv.V.Value()))
}

func (w *WalSuite) Test_Iterator_First_Then_Next_Ops() {
	type param struct {
		name      string
		restart   int
		isUnique  bool
		cacheSize int // 0 means no cache
		sstNum    int
	}

	tests := []param{
		{
			name:     "volume = 100_000, all keys are unique, block cache disable, single table",
			isUnique: true,
			restart:  5,
			sstNum:   1,
		},
		{
			name:     "volume = 100_000, keys are shared prefix, block cache disable, single table",
			isUnique: false,
			restart:  5,
			sstNum:   1,
		},
		{
			name:      "volume = 100_000, all keys are unique, block cache enabled, single table",
			isUnique:  true,
			restart:   5,
			sstNum:    1,
			cacheSize: 1 * mB,
		},
		{
			name:      "volume = 100_000, keys are shared prefix, block cache enabled, single table",
			isUnique:  false,
			restart:   5,
			sstNum:    1,
			cacheSize: 1 * mB,
		},
		{
			name:     "volume = 100_000, all keys are unique, block cache disable, multiple tables",
			isUnique: true,
			restart:  5,
			sstNum:   3,
		},
		{
			name:     "volume = 100_000, keys are shared prefix, block cache disable, multiple tables",
			isUnique: false,
			restart:  5,
			sstNum:   3,
		},
		{
			name:      "volume = 100_000, all keys are unique, block cache enabled, multiple tables",
			isUnique:  true,
			restart:   5,
			sstNum:    3,
			cacheSize: 1 * mB,
		},
		{
			name:      "volume = 100_000, keys are shared prefix, block cache enabled, multiple tables",
			isUnique:  false,
			restart:   5,
			sstNum:    3,
			cacheSize: 1 * mB,
		},
	}

	sampleSize := 100_000

	t := w.T()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Init a table
			inMemStorage := go_fs.NewInmemStorage()
			sample := make([][]kvType, 0, tc.sstNum)
			for sst := 1; sst <= tc.sstNum; sst++ {
				fileWritable, _, err := inMemStorage.Create(go_fs.TypeTable, int64(sst))
				assert.NoError(t, err)
				writer := go_sstable.NewWriter(
					fileWritable,
					common.TableV1,
					go_sstable.WithBlockRestartInterval(tc.restart),
					go_sstable.WithBlockSize(2*kB),
				)

				kvs := generateKV(sampleSize, tc.isUnique)
				sample = append(sample, kvs)
				for _, kv := range kvs {
					err := writer.Set(kv.key, kv.value)
					assert.NoError(t, err, "failed to set")
				}

				err = writer.Close()
				assert.NoError(t, err)
			}

			// Evaluate the result of the seek operations
			eg := errgroup.Group{}
			for sst := 1; sst <= tc.sstNum; sst++ {
				eg.Go(func() error {
					kvs := sample[sst-1]
					fileReadable, fd, err := inMemStorage.Open(go_fs.TypeTable, int64(sst), 0)
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

					firstKv := iter.First()
					assertKv(t, kvs[0], firstKv, 0)

					for i := 1; i < len(kvs); i++ {
						kv := iter.Next()
						assertKv(t, kvs[i], kv, i)
					}

					lastKv := iter.Next()
					assert.Nil(t, lastKv)

					return nil
				})
			}

			err := eg.Wait()
			assert.NoError(t, err)
		})
	}
}

func TestWalSuite(t *testing.T) {
	suite.Run(t, new(WalSuite))
}
