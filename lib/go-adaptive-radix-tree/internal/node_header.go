package internal

import "context"

type nodeHeader struct {
	// prefix used in the node to store the key compressed prefix.
	prefix    []byte
	prefixLen uint16
	// the number of children
	childrenLen uint16
}

func (n *nodeHeader) setPrefix(ctx context.Context, prefix []byte) {
	n.prefixLen = uint16(len(prefix))
	n.prefix = make([]byte, len(prefix))
	copy(prefix, n.prefix)
}

func (n *nodeHeader) setChildrenLen(ctx context.Context, childrenLen uint16) {
	n.childrenLen = childrenLen
}

func (n *nodeHeader) getPrefix(ctx context.Context) []byte {
	return n.prefix
}

var _ iNodeHeader = (*nodeHeader)(nil)
