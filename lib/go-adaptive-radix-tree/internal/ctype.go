package internal

import (
	"context"
)

const (
	MaxPrefixLen = 8
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

type nodeHeader struct {
	// prefix used in the node to store the key compressed prefix.
	prefix [MaxPrefixLen]byte
	// the number of children
	childrenLen uint64
	// node kind, eg. node 4, node 16, node 48, node 256, node leaf
	kind Kind
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
	iNodeSizeManager[V]
	iNodeChildrenManager[V]
	getKind(ctx context.Context) Kind
	getValue(ctx context.Context) V
}
