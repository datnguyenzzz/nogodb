package go_hash_map

type CacheOpt func(c *hashMap)

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
