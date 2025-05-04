package internal

import (
	"context"
	"fmt"
)

const (
	Node16KeysMin     uint8 = Node4KeysMax + 1 // node_16 needs at least 5 children, else it should be shrunk to the node4
	Node16KeysMax     uint8 = 16
	Node16PointersLen uint8 = 16
)

// Node16 This node type is used for storing between 5 and
// 16 Child pointers. Like the Node4, the keys and pointers
// are stored in separate arrays at corresponding positions, but
// both arrays have space for 16 entries. A Key can be found
// efficiently with binary search or, on modern hardware, with
// parallel comparisons using SIMD instructions.
type Node16[V any] struct {
	nodeHeader
	// At position i-th, keys[i] = Key value, pointers[i] = pointer to Child for the keys[i]
	// keys is an array of length 16 for a 1-byte Key.
	// The keys array is sorted in ascending order.
	keys [Node16KeysMax]byte
	// pointers to children node.
	children [Node16PointersLen]*INode[V]
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
	currChildrenLen := n.getChildrenLen(ctx)
	if currChildrenLen >= Node16KeysMax {
		return fmt.Errorf("node_16 is maxed out and don't have enough room for a new Key")
	}

	_, err := n.getChild(ctx, key)
	if err == nil {
		return fmt.Errorf("Key: %v already exists", key)
	}

	pos := Node16KeysMax
	for i := 0; i < int(Node16KeysMax); i++ {
		if n.keys[i] > key {
			pos = uint8(i)
			break
		}
	}
	// shift all keys[:pos] 1 step to the left to make room
	for i := 0; i+1 < int(pos); i++ {
		n.keys[i] = n.keys[i+1]
		n.children[i] = n.children[i+1]
	}
	// add a new Key to pos-1
	n.keys[pos-1] = key
	n.children[pos-1] = child
	n.setChildrenLen(ctx, currChildrenLen+1)

	return nil
}

func (n *Node16[V]) removeChild(ctx context.Context, key byte) error {
	currChildrenLen := n.getChildrenLen(ctx)
	if currChildrenLen == 0 {
		return fmt.Errorf("node is empty")
	}
	pos := -1
	for i := 0; i < int(Node16KeysMax); i++ {
		if n.keys[i] == key {
			pos = i
			break
		}
	}
	if pos == -1 {
		return childNodeNotFound
	}

	// shift all keys[:pos] 1 step to the right
	for i := pos; i >= 1; i-- {
		n.keys[i] = n.keys[i-1]
		n.children[i] = n.children[i-1]
	}
	// remove the keys[0]
	n.keys[0] = 0
	n.children[0] = nil
	n.setChildrenLen(ctx, currChildrenLen-1)
	return nil
}

func (n *Node16[V]) getChild(ctx context.Context, key byte) (*INode[V], error) {
	pos := -1
	for i := 0; i < int(Node16KeysMax); i++ {
		if n.keys[i] == key {
			pos = i
			break
		}
	}
	if pos == -1 || n.children[pos] == nil {
		return nil, childNodeNotFound
	}

	return n.children[pos], nil
}

func (n *Node16[V]) getAllChildren(ctx context.Context, order Order) []*INode[V] {
	switch order {
	case AscOrder:
		currLen := n.getChildrenLen(ctx)
		return n.children[Node16KeysMax-currLen:]
	case DescOrder:
		currLen := n.getChildrenLen(ctx)
		res := make([]*INode[V], currLen)
		for i := int8(Node16KeysMax - 1); i >= int8(Node16KeysMax-currLen); i-- {
			res[int8(Node16KeysMax)-1-i] = n.children[i]
		}
		return res
	default:
		// shouldn't go into that block
		return make([]*INode[V], n.getChildrenLen(ctx))
	}
}

func (n *Node16[V]) getChildByIndex(ctx context.Context, idx uint8) (byte, *INode[V], error) {
	currLen := n.getChildrenLen(ctx)
	if idx == currLen {
		return byte(0), nil, childNodeNotFound
	}

	pos := Node16KeysMax - currLen + idx
	return n.keys[pos], n.children[pos], nil
}

// grow to node48
func (n *Node16[V]) grow(ctx context.Context) (*INode[V], error) {
	if n.getChildrenLen(ctx) != Node16KeysMax {
		return nil, fmt.Errorf("node16 is not maxed out yet, so don't have to grow to a bigger node")
	}
	n48 := NewNode[V](KindNode48)
	n48.setPrefix(ctx, n.getPrefix(ctx))
	n48.setChildrenLen(ctx, n.getChildrenLen(ctx))

	for i := 0; i < int(Node16KeysMax); i++ {
		if err := n48.addChild(ctx, n.keys[i], n.children[i]); err != nil {
			return nil, err
		}
	}

	return &n48, nil
}

// shrink to node4
func (n *Node16[V]) shrink(ctx context.Context) (*INode[V], error) {
	if !n.isShrinkable(ctx) {
		return nil, fmt.Errorf("node16 is still too big for shrinking")
	}

	currChildrenLen := n.getChildrenLen(ctx)
	if currChildrenLen == 0 {
		return nil, fmt.Errorf("node16 has 0 children, which is unexpected")
	}

	n4 := NewNode[V](KindNode4)
	n4.setPrefix(ctx, n.getPrefix(ctx))

	for i := int(Node16KeysMax - 1); i >= int(Node16KeysMax-currChildrenLen); i-- {
		if err := n4.addChild(ctx, n.keys[i], n.children[i]); err != nil {
			return nil, err
		}
	}

	return &n4, nil
}

func (n *Node16[V]) hasEnoughSpace(ctx context.Context) bool {
	return n.getChildrenLen(ctx) < Node16KeysMax
}

func (n *Node16[V]) isShrinkable(ctx context.Context) bool {
	return n.getChildrenLen(ctx) < Node16KeysMin
}

var _ INode[any] = (*Node16[any])(nil)
