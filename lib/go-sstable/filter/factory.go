package filter

type Method byte

const (
	Unknown Method = iota
	BloomFilter
	RibbonFilter
)

// IFilter the methods that implement IFilter are usually static: they have a build phase and a probe phase.
// Once probing begins, new insertions are not valid.
type IFilter interface {
	NewWriter() IWriter
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

func NewFilter(method Method) IFilter {
	switch method {
	case BloomFilter:
		return newBloomFilter()
	default:
		panic("unsupported / unknown filtering method")
	}
}
