package common

type LazyFetcher interface {
	Load() []byte
}

type InternalLazyValue struct {
	V       []byte
	Fetcher LazyFetcher
}

type InternalKV struct {
	K InternalKey
	V InternalLazyValue
}

func (iv *InternalLazyValue) Value() []byte {
	if iv.Fetcher == nil {
		return iv.V
	}

	return iv.Fetcher.Load()
}

func (iv *InternalLazyValue) SetInplaceValue(value []byte) {
	iv.V = value
}

func (iv *InternalLazyValue) SetLazyFetcher(fetcher LazyFetcher) {
	iv.Fetcher = fetcher
}
