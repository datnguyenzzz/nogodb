package go_hash_map

type CacheOpt func(c *cache)

func WithMaxSize(maxSize int64) CacheOpt {
	return func(c *cache) {
		c.maxSize = maxSize
	}
}

func WithCacheType(cacheType CacheType) CacheOpt {
	return func(c *cache) {
		c.cacheType = cacheType
	}
}
