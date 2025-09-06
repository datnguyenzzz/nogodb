package row_block

import (
	"fmt"
	"math/rand"
	"testing"

	go_block_cache "github.com/datnguyenzzz/nogodb/lib/go-block-cache"
	"github.com/datnguyenzzz/nogodb/lib/go-bytesbufferpool/predictable_size"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common/block"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/compression"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/options"
	rowBlockMocks "github.com/datnguyenzzz/nogodb/lib/go-sstable/row_block/mocks"
	storageMocks "github.com/datnguyenzzz/nogodb/lib/go-sstable/storage/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var (
	defaultCompressor = compression.NewCompressor(compression.SnappyCompression)
	defaultChecksumer = common.NewChecksumer(common.CRC32Checksum)
)

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

			// Create mockery-generated mock
			mockStorageReader := storageMocks.NewILayoutReader(t)

			// Setup mock expectations
			if tc.readerError != nil {
				mockStorageReader.On("ReadAt", mock.MatchedBy(func(p []byte) bool {
					return len(p) == len(stored)
				}), uint64(0)).Return(tc.readerError)
			} else {
				mockStorageReader.On(
					"ReadAt",
					mock.MatchedBy(func(p []byte) bool {
						return len(p) == len(stored)
					}),
					uint64(0),
				).Run(func(args mock.Arguments) {
					p := args.Get(0).([]byte)
					copy(p, stored)
				}).Return(nil)
			}

			// Setup Close expectation for cleanup
			mockStorageReader.On("Close").Return(nil)

			r := &RowBlockReader{}
			cacheOpts := &options.CacheOptions{
				CacheMethod: go_block_cache.LRU,
				MaxSize:     1 * 1024 * 1024,
				FileNum:     10,
			}
			r.Init(predictable_size.NewPredictablePool(), mockStorageReader, cacheOpts)

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

			if val != nil {
				val.Release()
			}
			r.Release()
		})
	}
}

func Test_ReadThroughCache(t *testing.T) {
	type params struct {
		desc              string
		size              int
		cacheHit          bool
		storageReadError  error
		cacheSetError     error
		corrupted         bool
		expectStorageRead bool
		expectCacheSet    bool
	}

	tests := []params{
		{
			desc:              "cache hit - should return cached value",
			size:              1024,
			cacheHit:          true,
			expectStorageRead: false,
			expectCacheSet:    false,
		},
		{
			desc:              "cache miss - successful storage read and cache set",
			size:              1024,
			cacheHit:          false,
			expectStorageRead: true,
			expectCacheSet:    true,
		},
		{
			desc:              "cache miss - storage read error",
			size:              1024,
			cacheHit:          false,
			storageReadError:  fmt.Errorf("storage read failed"),
			expectStorageRead: true,
			expectCacheSet:    false,
		},
		{
			desc:              "cache miss - corrupted data from storage",
			size:              1024,
			cacheHit:          false,
			corrupted:         true,
			expectStorageRead: true,
			expectCacheSet:    false,
		},
		{
			desc:              "cache miss - successful storage read but cache set fails",
			size:              1024,
			cacheHit:          false,
			cacheSetError:     failedUpdateToCache,
			expectStorageRead: true,
			expectCacheSet:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			// Setup test data
			original := randomBytes(tc.size)
			pb := compressToPb(defaultCompressor, defaultChecksumer, original)
			stored := append(pb.Data, pb.Trailer[:]...)
			if tc.corrupted {
				stored[len(stored)-1] += 1
			}

			// Create mocks
			mockStorageReader := storageMocks.NewILayoutReader(t)
			mockBlockCache := &rowBlockMocks.IBlockCacheWrapper{}
			mockBlockCache.Test(t)

			bh := &block.BlockHandle{
				Offset: 0,
				Length: uint64(len(stored)),
			}

			// Setup cache mock expectations
			if tc.cacheHit {
				mockFetcher := NewLazyValueMock(original)
				mockVal := common.NewBlankInternalLazyValue(common.ValueFromCache)
				err := mockVal.SetCacheFetcher(&lazyValueWrapper{mockFetcher})
				assert.NoError(t, err)

				mockBlockCache.On("Get", bh).Return(&mockVal, nil).Once()
			} else {
				mockBlockCache.On("Get", bh).Return(nil, cacheMiss).Once()
			}

			// Setup storage mock expectations if needed
			if tc.expectStorageRead {
				if tc.storageReadError != nil {
					mockStorageReader.On("ReadAt", mock.MatchedBy(func(p []byte) bool {
						return len(p) == len(stored)
					}), uint64(0)).Return(tc.storageReadError).Once()
				} else {
					mockStorageReader.On("ReadAt", mock.MatchedBy(func(p []byte) bool {
						return len(p) == len(stored)
					}), uint64(0)).Run(func(args mock.Arguments) {
						p := args.Get(0).([]byte)
						copy(p, stored)
					}).Return(nil).Once()
				}
			}

			// Setup cache set mock expectations if needed
			if tc.expectCacheSet {
				if tc.cacheSetError != nil {
					mockBlockCache.On("Set", bh, mock.AnythingOfType("*common.InternalLazyValue")).Return(tc.cacheSetError).Once()
				} else {
					mockBlockCache.On("Set", bh, mock.AnythingOfType("*common.InternalLazyValue")).Return(nil).Once()
				}
			}

			// Setup Close expectations
			mockStorageReader.On("Close").Return(nil).Maybe()
			mockBlockCache.On("Close").Maybe()

			// Create reader with mocked dependencies
			r := &RowBlockReader{
				bpool:         predictable_size.NewPredictablePool(),
				storageReader: mockStorageReader,
				blockCache:    mockBlockCache,
			}

			// Execute ReadThroughCache
			val, err := r.ReadThroughCache(bh, block.BlockKindData)

			// Verify results
			if tc.cacheHit {
				assert.NoError(t, err)
				assert.NotNil(t, val)
				assert.Equal(t, original, val.Value())
				assert.Equal(t, common.ValueFromCache, val.ValueSource)
			} else if tc.storageReadError != nil {
				assert.ErrorIs(t, err, tc.storageReadError)
				assert.Nil(t, val)
			} else if tc.corrupted {
				assert.ErrorIs(t, err, common.MismatchedChecksumError)
				assert.Nil(t, val)
			} else if tc.cacheSetError != nil {
				assert.ErrorIs(t, err, tc.cacheSetError)
				assert.Nil(t, val)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, val)
				assert.Equal(t, original, val.Value())
				assert.Equal(t, common.ValueFromBuffer, val.ValueSource)
			}

			// Cleanup
			if val != nil {
				val.Release()
			}

			// Assert all mock expectations were met
			mockBlockCache.AssertExpectations(t)
		})
	}
}

type lazyValueMock struct {
	v go_block_cache.Value
}

func (l lazyValueMock) Load() go_block_cache.Value {
	return l.v
}

func (l lazyValueMock) Release() {}

func NewLazyValueMock(value []byte) go_block_cache.LazyValue {
	return &lazyValueMock{
		v: value,
	}
}

var _ go_block_cache.LazyValue = (*lazyValueMock)(nil)

func randomBytes(n int) []byte {
	res := make([]byte, n)
	for i := 0; i < n; i++ {
		res[i] = byte(rand.Intn(255))
	}
	return res
}
