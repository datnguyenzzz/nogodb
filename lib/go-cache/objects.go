package go_cache

type Value interface{}

type CacheType byte

const (
	LRU CacheType = iota
	ClockPro
)
