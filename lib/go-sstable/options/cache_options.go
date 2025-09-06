package options

import (
	go_block_cache "github.com/datnguyenzzz/nogodb/lib/go-block-cache"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/common"
)

const (
	defaultCacheMethod = go_block_cache.LRU
	defaultMaxSize     = 8 * 1024 * 1024 // 8MiB
)

type CacheOptions struct {
	CacheMethod go_block_cache.CacheType
	MaxSize     int64
	FileNum     common.DiskFileNum
}
