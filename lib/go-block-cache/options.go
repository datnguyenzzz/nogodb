package go_block_cache

type CacheOpt func(c *hashMap)

func WithShardNum(shardNum int) CacheOpt {
	return func(c *hashMap) {
		c.shardNum = shardNum
	}
}

func WithMaxSize(maxSize int64) CacheOpt {
	return func(c *hashMap) {
		c.maxSize = maxSize
	}
}

func WithCacheType(cacheType CacheType) CacheOpt {
	return func(c *hashMap) {
		c.cacheType = cacheType
	}
}
