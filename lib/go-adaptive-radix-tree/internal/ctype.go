package internal

import (
	"context"
	"fmt"
)

// errors
var (
	failedToAddChild  error = fmt.Errorf("failed to add child")
	failedToInitLock  error = fmt.Errorf("failed to init lock")
	childNodeNotFound error = fmt.Errorf("child node not found")
	noSuchKey         error = fmt.Errorf("not such key")
)

type Callback[V any] func(ctx context.Context, k []byte, v V)

type Kind int8

const (
	KindNodeNoop Kind = iota
	KindNodeLeaf
	KindNode4
	KindNode16
	KindNode48
	KindNode256
)

type Order int8

const (
	AscOrder Order = iota
	DescOrder
)

type iNodeHeader interface {
	// checkPrefix compares the compressed path of a node with the key and returns the number of equal bytes
	checkPrefix(ctx context.Context, key []byte, offset uint8) uint8
	getPrefix(ctx context.Context) []byte
	getPrefixLen(ctx context.Context) uint8
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
	addChild(ctx context.Context, key byte, child *INode[V]) error
	removeChild(ctx context.Context, key byte) error
	getChild(ctx context.Context, key byte) (INode[V], error)
	getAllChildren(ctx context.Context, order Order) []INode[V]
}

type INode[V any] interface {
	iNodeHeader
	iNodeSizeManager[V]
	iNodeChildrenManager[V]

	getKind(ctx context.Context) Kind
	getValue(ctx context.Context) V
	setValue(ctx context.Context, v V)
}
