package common

type LazyFetcher interface {
	Load() []byte
	// Release freed associated resources. Release should always success
	// and can be called multiple times without causing error.
	Release()
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

func (iv *InternalLazyValue) Release() {
	if iv.Fetcher == nil {
		iv.V = nil
	}

	iv.Fetcher.Release()
}

func (iv *InternalLazyValue) SetInplaceValue(value []byte) {
	iv.V = value
}

func (iv *InternalLazyValue) SetLazyFetcher(fetcher LazyFetcher) {
	iv.Fetcher = fetcher
}
