package internal

import "context"

type nodeHeader struct {
	// prefix used in the node to store the compressed portion of the key.
	// Note: To optimize memory usage, the prefix is not intended to store the entire key
	// starting from the first byte. Instead, it only stores the compressed portion that
	// has not been preserved in the upper nodes.
	prefix    []byte
	prefixLen uint8
	// the number of children
	childrenLen uint8
}

func (n *nodeHeader) setPrefix(ctx context.Context, prefix []byte) {
	n.prefixLen = uint8(len(prefix))
	n.prefix = make([]byte, len(prefix))
	copy(n.prefix, prefix)
}

func (n *nodeHeader) getChildrenLen(ctx context.Context) uint8 {
	return n.childrenLen
}

func (n *nodeHeader) setChildrenLen(ctx context.Context, childrenLen uint8) {
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
