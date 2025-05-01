package internal

import (
	"context"
)

// NodeLeaf aka Single-value leaf: The values are stored
// in an additional leaf node type, instead of in the inner node.
// It's also an implication that the prefix on the leaf node is equal to the key itself.
type NodeLeaf[V any] struct {
	nodeHeader
	value V
}

func (n *NodeLeaf[V]) getValue(ctx context.Context) V {
	return n.value
}

func (n *NodeLeaf[V]) setValue(ctx context.Context, v V) {
	n.value = v
}

func (n *NodeLeaf[V]) getKind(ctx context.Context) Kind {
	return KindNodeLeaf
}

func (n *NodeLeaf[V]) addChild(ctx context.Context, key byte, child *INode[V]) error {
	panic("node leaf doesn't support this function")
}

func (n *NodeLeaf[V]) removeChild(ctx context.Context, key byte) error {
	panic("node leaf doesn't support this function")
}

func (n *NodeLeaf[V]) getChild(ctx context.Context, key byte) (INode[V], error) {
	panic("node leaf doesn't support this function")
}

func (n *NodeLeaf[V]) grow(ctx context.Context) INode[V] {
	panic("node leaf doesn't support this function")
}

func (n *NodeLeaf[V]) shrink(ctx context.Context) INode[V] {
	panic("node leaf doesn't support this function")
}

func (n *NodeLeaf[V]) hasEnoughSpace(ctx context.Context) bool {
	panic("node leaf doesn't support this function")
}

var _ INode[any] = (*NodeLeaf[any])(nil)
