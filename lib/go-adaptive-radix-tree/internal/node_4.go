package internal

import (
	"context"
	"fmt"
)

const (
	Node4KeysMin     uint8 = 2 // we only need a node4 if it has at least 2 children
	Node4KeysMax     uint8 = 4
	Node4PointersLen uint8 = 4
)

// Node4 The smallest node type can store up to 4 child
// pointers and uses an array of length 4 for keys and another
// array of the same length for pointers. The keys and pointers
// are stored at corresponding positions and the keys are sorted.
type Node4[V any] struct {
	nodeHeader
	// keys is an array of length 4 for a 1-byte key. The array is sorted in ascending order.
	keys [Node4KeysMax]byte
	// pointers to children node. pointers[i] is a pointer to a child node for a key = keys[i]
	children [Node4PointersLen]*INode[V]
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
	currChildrenLen := n.getChildrenLen(ctx)
	if currChildrenLen >= Node4KeysMax {
		return fmt.Errorf("node_4 is maxed out and don't have enough room for a new key")
	}

	pos := Node4KeysMax
	for i := uint8(0); i < Node4KeysMax; i++ {
		if n.keys[i] > key {
			pos = i
			break
		}
	}
	// shift all keys[:pos] 1 step to the left to make room
	for i := 0; uint8(i+1) < pos; i++ {
		n.keys[i] = n.keys[i+1]
		n.children[i] = n.children[i+1]
	}
	// add a new key to pos-1
	n.keys[pos-1] = key
	n.children[pos-1] = child
	n.setChildrenLen(ctx, currChildrenLen+1)

	return nil
}

func (n *Node4[V]) removeChild(ctx context.Context, key byte) error {
	currChildrenLen := n.getChildrenLen(ctx)
	if currChildrenLen == 0 {
		return fmt.Errorf("node is empty")
	}
	pos := -1
	for i := 0; i < int(Node4KeysMax); i++ {
		if n.keys[i] == key {
			pos = i
			break
		}
	}
	if pos == -1 {
		return childNodeNotFound
	}

	// shift all keys[:pos] 1 step to the right
	for i := pos; i > 1; i-- {
		n.keys[i] = n.keys[i-1]
		n.children[i] = n.children[i-1]
	}
	// remove the keys[0]
	n.keys[0] = 0
	n.children[0] = nil
	n.setChildrenLen(ctx, currChildrenLen-1)
	return nil
}

func (n *Node4[V]) getChild(ctx context.Context, key byte) (*INode[V], error) {
	pos := -1
	for i := 0; i < int(Node4KeysMax); i++ {
		if n.keys[i] == key {
			pos = i
			break
		}
	}
	if pos == -1 {
		return nil, childNodeNotFound
	}

	return n.children[pos], nil
}

func (n *Node4[V]) getAllChildren(ctx context.Context, order Order) []*INode[V] {
	switch order {
	case AscOrder:
		return n.children[:]
	case DescOrder:
		res := make([]*INode[V], n.getChildrenLen(ctx))
		for i := uint8(0); i < Node4KeysMax; i++ {
			res[Node4KeysMax-1-i] = n.children[i]
		}
		return res
	default:
		// shouldn't go into that block
		return make([]*INode[V], n.getChildrenLen(ctx))
	}
}

// grow to Node16
func (n *Node4[V]) grow(ctx context.Context) (*INode[V], error) {
	if n.getChildrenLen(ctx) != Node4KeysMax {
		return nil, fmt.Errorf("node4 is not maxed out yet, so don't have to grow to a bigger node")
	}
	n16 := newNode[V](KindNode16)
	n16.setPrefix(ctx, n.getPrefix(ctx))
	n16.setChildrenLen(ctx, n.getChildrenLen(ctx))

	for i := uint8(0); i < Node4KeysMax; i++ {
		if err := n16.addChild(ctx, n.keys[i], n.children[i]); err != nil {
			return nil, err
		}
	}

	return &n16, nil
}

// shrink to NodeLeaf
func (n *Node4[V]) shrink(ctx context.Context) (*INode[V], error) {
	if !n.isShrinkable(ctx) {
		return nil, fmt.Errorf("node4 is still too big for shrinking")
	}

	if n.getChildrenLen(ctx) == 0 {
		return nil, fmt.Errorf("node4 has 0 children, which is unexpected")
	}

	child := *n.children[Node4KeysMax-1]
	if child.getKind(ctx) == KindNodeLeaf {
		return &child, nil
	}

	// if a child is not leaf, then the node4 is not shrinkable
	// however it shouldn't return any errors either, thus we can
	// just simply return the current node without modifying anything
	var nn INode[V] = n
	return &nn, nil
}

func (n *Node4[V]) hasEnoughSpace(ctx context.Context) bool {
	return n.getChildrenLen(ctx) < Node4KeysMax
}

func (n *Node4[V]) isShrinkable(ctx context.Context) bool {
	return n.getChildrenLen(ctx) < Node4KeysMin
}

var _ INode[any] = (*Node4[any])(nil)
