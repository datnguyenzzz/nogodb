package internal

import (
	"context"
)

// NodeLeaf aka Single-value leaf: The values are stored
// in an additional leaf node type, instead of in the inner node.
// It's also an implication that the prefix on the leaf node is equal to the Key itself.
type NodeLeaf[V any] struct {
	nodeHeader
	locker INodeLocker
	value  V
}

func (n *NodeLeaf[V]) getValue(ctx context.Context) V {
	return n.value
}

func (n *NodeLeaf[V]) setValue(ctx context.Context, v V) {
	n.value = v
}

func (n *NodeLeaf[V]) GetKind(ctx context.Context) Kind {
	return KindNodeLeaf
}

func (n *NodeLeaf[V]) addChild(ctx context.Context, key byte, child *INode[V]) error {
	panic("node leaf doesn't support this function")
}

func (n *NodeLeaf[V]) removeChild(ctx context.Context, key byte) error {
	panic("node leaf doesn't support this function")
}

func (n *NodeLeaf[V]) getChild(ctx context.Context, key byte) (*INode[V], error) {
	panic("node leaf doesn't support this function")
}

func (n *NodeLeaf[V]) getAllChildren(ctx context.Context, order Order) []*INode[V] {
	return []*INode[V]{}
}

func (n *NodeLeaf[V]) getChildByIndex(ctx context.Context, idx uint8) (byte, *INode[V], error) {
	panic("node leaf doesn't support this function")
}

func (n *NodeLeaf[V]) grow(ctx context.Context) (*INode[V], error) {
	panic("node leaf doesn't support this function")
}

func (n *NodeLeaf[V]) shrink(ctx context.Context) (*INode[V], error) {
	panic("node leaf doesn't support this function")
}

func (n *NodeLeaf[V]) hasEnoughSpace(ctx context.Context) bool {
	panic("node leaf doesn't support this function")
}

func (n *NodeLeaf[V]) isShrinkable(ctx context.Context) bool {
	return false
}

func (n *NodeLeaf[V]) GetLocker() INodeLocker {
	return n.locker
}

func (n *NodeLeaf[V]) setLocker(locker INodeLocker) {
	n.locker = locker
}

func (n *NodeLeaf[V]) clone() INode[V] {
	nn := &NodeLeaf[V]{}
	nn.nodeHeader = n.nodeHeader
	nn.locker = n.locker
	nn.value = n.value
	return nn
}

var _ INode[any] = (*NodeLeaf[any])(nil)
