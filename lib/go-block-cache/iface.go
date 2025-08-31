package go_block_cache

type LazyValue interface {
	Load() Value
	Release()
}

type IMap interface {
	Get(fileNum, key uint64) (LazyValue, bool)
	Set(fileNum, key uint64, value Value) bool
	Delete(fileNum, key uint64) bool
	Close(force bool)
	SetCapacity(capacity int64)

	// utils

	GetStats() Stats
	GetInUsed() int64
}
