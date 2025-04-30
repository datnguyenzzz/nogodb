package internal

import (
	"context"
)

const (
	Node48KeysLen     = 256
	Node48PointersLen = 48
)

// Node48 As the number of entries in a node increases,
// searching the key array becomes expensive. Therefore, nodes
// with more than 16 pointers do not store the keys explicitly.
// Instead, a 256-element array is used, which can be indexed
// with key bytes directly. If a node has between 17 and 48 child
// pointers, this array stores indexes into a second array which
// contains up to 48 pointers.
type Node48[V any] struct {
	nodeHeader
	keys     [Node48KeysLen]byte          // an array of length 16 for 1-byte key
	children [Node48PointersLen]*INode[V] // pointers to children node
}

func (n *Node48[V]) getValue(ctx context.Context) V {
	panic("node 48 doesn't hold any value")
}

func (n *Node48[V]) setValue(ctx context.Context, v V) {
	panic("node 48 doesn't hold any value")
}

func (n *Node48[V]) getKind(ctx context.Context) Kind {
	return KindNode48
}

func (n *Node48[V]) addChild(ctx context.Context, key byte, child *INode[V]) error {
	//TODO implement me
	panic("implement me")
}

func (n *Node48[V]) removeChild(ctx context.Context, key byte) error {
	//TODO implement me
	panic("implement me")
}

func (n *Node48[V]) getChild(ctx context.Context, key byte) (INode[V], error) {
	//TODO implement me
	panic("implement me")
}

func (n *Node48[V]) grow(ctx context.Context) INode[V] {
	//TODO implement me
	panic("implement me")
}

func (n *Node48[V]) shrink(ctx context.Context) INode[V] {
	//TODO implement me
	panic("implement me")
}

func (n *Node48[V]) hasEnoughSpace(ctx context.Context) bool {
	//TODO implement me
	panic("implement me")
}

var _ INode[any] = (*Node48[any])(nil)
