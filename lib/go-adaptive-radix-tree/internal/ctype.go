package internal

const (
	maxPrefixLen = 8
)

type Kind int8

const (
	NodeNoop Kind = iota
	NodeLeaf
	Node4
	Node16
	Node48
	Node256
)

type nodeHeader struct {
	// prefix used in the node to store the key compressed prefix.
	prefix [maxPrefixLen]byte
	// the number of children
	childrenLen uint64
	// node kind, eg. node 4, node 16, node 48, node 256, node leaf
	kind Kind
}

type INode[V any] interface {
	// ...
}
