package options

import (
	go_block_cache "github.com/datnguyenzzz/nogodb/lib/go-block-cache"
	go_fs "github.com/datnguyenzzz/nogodb/lib/go-fs"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
)

type IteratorOptsFunc func(opts *IteratorOpts)

type IteratorOpts struct {
	CacheOpts *CacheOptions
}

func WithBlockCache(method go_block_cache.CacheType, fd go_fs.FileDesc) IteratorOptsFunc {
	return func(opts *IteratorOpts) {
		opts.CacheOpts = &CacheOptions{}
		opts.CacheOpts.CacheMethod = method
		opts.CacheOpts.MaxSize = defaultCacheSize
		// notes: the block cache use file_num as a part of the cache key
		// a block cache with key = [file_num + offset], value = data[offset:offset + length]
		opts.CacheOpts.FileNum = common.FromFileDescToFileNum(fd)
	}
}

func WithBlockCacheSize(size int64) IteratorOptsFunc {
	return func(opts *IteratorOpts) {
		if opts.CacheOpts == nil {
			return
		}
		opts.CacheOpts.MaxSize = size
	}
}
