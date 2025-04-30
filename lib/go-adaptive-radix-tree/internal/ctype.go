package internal

import (
	"context"
)

const (
	MaxPrefixLen = 10
)

type Kind int8

const (
	KindNodeNoop Kind = iota
	KindNodeLeaf
	KindNode4
	KindNode16
	KindNode48
	KindNode256
)

type iNodeHeader interface {
	setPrefix(ctx context.Context, prefix []byte)
	setChildrenLen(ctx context.Context, childrenLen uint16)
}

// iNodeSizeManager to control the size of the node itself
type iNodeSizeManager[V any] interface {
	grow(ctx context.Context) INode[V]
	hasEnoughSpace(ctx context.Context) bool
	shrink(ctx context.Context) INode[V]
}

// iNodeChildrenManager control the node's children
type iNodeChildrenManager[V any] interface {
	addChild(ctx context.Context, key []byte, child INode[V]) error
	removeChild(ctx context.Context, key []byte) error
	getChild(ctx context.Context, key []byte) (INode[V], error)
}

type INode[V any] interface {
	iNodeHeader
	iNodeSizeManager[V]
	iNodeChildrenManager[V]

	getKind(ctx context.Context) Kind
	getValue(ctx context.Context) V
	setValue(ctx context.Context, v V)
}
