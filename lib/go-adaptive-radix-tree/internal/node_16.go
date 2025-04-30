package internal

import (
	"context"
)

const (
	Node16KeysLen     = 16
	Node16PointersLen = 16
)

type Node16[V any] struct {
	nodeHeader
	keys     [Node16KeysLen]byte         // an array of length 16 for 1-byte key
	children [Node16PointersLen]INode[V] // pointers to children node
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

func (n *Node16[V]) addChild(ctx context.Context, key []byte, child INode[V]) error {
	//TODO implement me
	panic("implement me")
}

func (n *Node16[V]) removeChild(ctx context.Context, key []byte) error {
	//TODO implement me
	panic("implement me")
}

func (n *Node16[V]) getChild(ctx context.Context, key []byte) (INode[V], error) {
	//TODO implement me
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
