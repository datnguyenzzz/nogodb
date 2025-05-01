package internal

import "context"

type nodeHeader struct {
	// prefix used in the node to store the key compressed prefix.
	prefix    []byte
	prefixLen uint8
	// the number of children
	childrenLen uint16
}

func (n *nodeHeader) setPrefix(ctx context.Context, prefix []byte) {
	n.prefixLen = uint8(len(prefix))
	n.prefix = make([]byte, len(prefix))
	copy(prefix, n.prefix)
}

func (n *nodeHeader) setChildrenLen(ctx context.Context, childrenLen uint16) {
	n.childrenLen = childrenLen
}

func (n *nodeHeader) getPrefix(ctx context.Context) []byte {
	return n.prefix
}

func (n *nodeHeader) getPrefixLen(ctx context.Context) uint8 {
	return n.prefixLen
}

func (n *nodeHeader) checkPrefix(ctx context.Context, key []byte, offset uint8) uint8 {
	i := uint8(0)
	for ; i < n.prefixLen && offset+i < uint8(len(key)); i++ {
		if n.prefix[i] != key[offset+i] {
			break
		}
	}
	return i
}

var _ iNodeHeader = (*nodeHeader)(nil)
