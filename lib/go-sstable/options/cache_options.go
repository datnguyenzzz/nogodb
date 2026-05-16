package options

import (
	nogodb_common "github.com/datnguyenzzz/nogodb/lib/common"
	go_block_cache "github.com/datnguyenzzz/nogodb/lib/go-block-cache"
)

const (
	defaultCacheSize = 2 * 1024 * 1024 // 2mB
)

type CacheOptions struct {
	CacheMethod go_block_cache.CacheType
	MaxSize     int64
	FileNum     nogodb_common.DiskfileNum
	ShardNum    int
}
