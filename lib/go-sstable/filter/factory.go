package filter

import "github.com/datnguyenzzz/nogodb/lib/go-blocked-bloom-filter"

type Method byte

const (
	Unknown Method = iota
	BloomFilter
	RibbonFilter
)

type IRead interface {
	Name() string
	// MayContain returns whether the encoded filter may contain given key.
	// False positives are possible, where it returns true for keys not in the
	// original set.
	MayContain(filter, key []byte) bool
}

type IWriter interface {
	// Add adds a key to the filter generator.
	Add(key []byte)
	// Build generates encoded filters based on keys passed so far.
	Build(pb *[]byte)
}

func NewFilterWriter(method Method) IWriter {
	switch method {
	case BloomFilter:
		bf := go_blocked_bloom_filter.NewBloomFilter()
		return bf.NewWriter()
	default:
		panic("unsupported / unknown filtering method")
	}
}

func NewFilterReader(method Method) IRead {
	switch method {
	case BloomFilter:
		return go_blocked_bloom_filter.NewBloomFilter()
	default:
		panic("unsupported / unknown filtering method")
	}
}
