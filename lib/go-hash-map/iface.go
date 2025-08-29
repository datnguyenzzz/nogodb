package go_hash_map

type LazyValue interface {
	Load() Value
	Release()
}

type IMap interface {
	Get(fileNum, key uint64) (LazyValue, bool)
	Set(fileNum, key uint64, value Value) bool
	Delete(fileNum, key uint64) bool
	Close()
}

type iCache interface {
	SetCapacity(capacity int64)
	Promote(node *kv) bool
	Evict( /* ... */)
	Ban(node *kv)
}
