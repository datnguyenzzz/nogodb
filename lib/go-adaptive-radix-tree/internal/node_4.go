package internal

import (
	"context"
)

const (
	Node4KeysLen     = 4
	Node4PointersLen = 4
)

// Node4 The smallest node type can store up to 4 child
// pointers and uses an array of length 4 for keys and another
// array of the same length for pointers. The keys and pointers
// are stored at corresponding positions and the keys are sorted.
type Node4[V any] struct {
	nodeHeader
	keys     [Node4KeysLen]byte          // an array of length 4 for 1-byte key
	children [Node4PointersLen]*INode[V] // pointers to children node
}

func (n *Node4[V]) getValue(ctx context.Context) V {
	panic("node 4 doesn't hold any value")
}

func (n *Node4[V]) setValue(ctx context.Context, v V) {
	panic("node 4 doesn't hold any value")
}

func (n *Node4[V]) getKind(ctx context.Context) Kind {
	return KindNode4
}

func (n *Node4[V]) addChild(ctx context.Context, key byte, child *INode[V]) error {
	//TODO implement me
	panic("implement me")
}

func (n *Node4[V]) removeChild(ctx context.Context, key byte) error {
	//TODO implement me
	panic("implement me")
}

func (n *Node4[V]) getChild(ctx context.Context, key byte) (INode[V], error) {
	//TODO implement me
	panic("implement me")
}

func (n *Node4[V]) getAllChildren(ctx context.Context, order Order) []INode[V] {
	panic("implement me")
}

func (n *Node4[V]) grow(ctx context.Context) INode[V] {
	//TODO implement me
	panic("implement me")
}

func (n *Node4[V]) shrink(ctx context.Context) INode[V] {
	//TODO implement me
	panic("implement me")
}

func (n *Node4[V]) hasEnoughSpace(ctx context.Context) bool {
	//TODO implement me
	panic("implement me")
}

var _ INode[any] = (*Node4[any])(nil)
