package internal

import (
	"context"
)

const (
	Node16KeysLen     = 16
	Node16PointersLen = 16
)

// Node16 This node type is used for storing between 5 and
// 16 child pointers. Like the Node4, the keys and pointers
// are stored in separate arrays at corresponding positions, but
// both arrays have space for 16 entries. A key can be found
// efficiently with binary search or, on modern hardware, with
// parallel comparisons using SIMD instructions.
type Node16[V any] struct {
	nodeHeader
	keys     [Node16KeysLen]byte          // an array of length 16 for 1-byte key
	children [Node16PointersLen]*INode[V] // pointers to children node
}

func (n *Node16[V]) getValue(ctx context.Context) V {
	panic("node 16 doesn't hold any value")
}

func (n *Node16[V]) setValue(ctx context.Context, v V) {
	panic("node 16 doesn't hold any value")
}

func (n *Node16[V]) getKind(ctx context.Context) Kind {
	return KindNode16
}

func (n *Node16[V]) addChild(ctx context.Context, key byte, child *INode[V]) error {
	//TODO implement me
	panic("implement me")
}

func (n *Node16[V]) removeChild(ctx context.Context, key byte) error {
	//TODO implement me
	panic("implement me")
}

func (n *Node16[V]) getChild(ctx context.Context, key byte) (INode[V], error) {
	//TODO implement me
	panic("implement me")
}

func (n *Node16[V]) getAllChildren(ctx context.Context, order Order) []INode[V] {
	panic("implement me")
}

func (n *Node16[V]) grow(ctx context.Context) INode[V] {
	//TODO implement me
	panic("implement me")
}

func (n *Node16[V]) shrink(ctx context.Context) INode[V] {
	//TODO implement me
	panic("implement me")
}

func (n *Node16[V]) hasEnoughSpace(ctx context.Context) bool {
	//TODO implement me
	panic("implement me")
}

var _ INode[any] = (*Node16[any])(nil)
