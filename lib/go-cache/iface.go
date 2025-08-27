package go_cache

type LazyValue interface {
	Load() Value
}

type ICache interface {
	Get(ns, key uint64) (LazyValue, bool)
	Delete(ns, key uint64) bool
}
