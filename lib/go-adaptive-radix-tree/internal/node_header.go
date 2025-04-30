package internal

import "context"

type nodeHeader struct {
	// prefix used in the node to store the key compressed prefix.
	prefix    [MaxPrefixLen]byte
	prefixLen uint16
	// the number of children
	childrenLen uint16
}

func (n *nodeHeader) setPrefix(ctx context.Context, prefix []byte) {
	n.prefixLen = uint16(len(prefix))
	for i := 0; uint16(i) < min(MaxPrefixLen, n.prefixLen); i++ {
		n.prefix[i] = prefix[i]
	}
}

func (n *nodeHeader) setChildrenLen(ctx context.Context, childrenLen uint16) {
	n.childrenLen = childrenLen
}

var _ iNodeHeader = (*nodeHeader)(nil)
