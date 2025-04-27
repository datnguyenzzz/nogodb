package internal

import (
	"context"
)

const (
	Node256PointersLen = 256
)

type Node256[V any] struct {
	nodeHeader
	children [Node256PointersLen]INode[V] // pointers to children node
}

func (n *Node256[V]) getValue(ctx context.Context) V {
	panic("node 256 doesn't hold any value")
}

func (n *Node256[V]) getKind(ctx context.Context) Kind {
	return KindNode256
}

func (n *Node256[V]) addChild(ctx context.Context, key []byte, child INode[V]) error {
	//TODO implement me
	panic("implement me")
}

func (n *Node256[V]) removeChild(ctx context.Context, key []byte) error {
	//TODO implement me
	panic("implement me")
}

func (n *Node256[V]) getChild(ctx context.Context, key []byte) (INode[V], error) {
	//TODO implement me
	panic("implement me")
}

func (n *Node256[V]) grow(ctx context.Context) INode[V] {
	//TODO implement me
	panic("implement me")
}

func (n *Node256[V]) shrink(ctx context.Context) INode[V] {
	//TODO implement me
	panic("implement me")
}

func (n *Node256[V]) hasEnoughSpace(ctx context.Context) bool {
	//TODO implement me
	panic("implement me")
}

var _ INode[any] = (*Node256[any])(nil)
