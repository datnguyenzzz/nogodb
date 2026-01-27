package internal

import (
	"context"
	"fmt"
)

const (
	Node256PointersMin uint8  = Node48PointersMax + 1
	Node256PointersMax uint16 = 256
)

// Node256 The largest node type is simply an array of 256
// pointers and is used for storing between 49 and 256 entries.
// With this representation, the next node can be found very
// efficiently using a single lookup of the Key byte in that array.
// No additional indirection is necessary. If most entries are not
// null, this representation is also very space efficient because
// only pointers need to be stored.
type Node256[V any] struct {
	nodeHeader
	nodeLocker
	// pointers to children node
	children [Node256PointersMax]*INode[V]
}

func (n *Node256[V]) getValue(ctx context.Context) V {
	panic("node 256 doesn't hold any value")
}

func (n *Node256[V]) setValue(ctx context.Context, v V) {
	panic("node 256 doesn't hold any value")
}

func (n *Node256[V]) getKind(ctx context.Context) Kind {
	return KindNode256
}

func (n *Node256[V]) addChild(ctx context.Context, key byte, child *INode[V]) error {
	currChildrenLen := n.getChildrenLen(ctx)
	if uint16(currChildrenLen) >= Node256PointersMax {
		return fmt.Errorf("node256 is maxed out and don't have enough room for a new Key")
	}

	n.children[key] = child
	n.setChildrenLen(ctx, currChildrenLen+1)
	return nil
}

func (n *Node256[V]) removeChild(ctx context.Context, key byte) error {
	currChildrenLen := n.getChildrenLen(ctx)
	n.children[key] = nil
	n.setChildrenLen(ctx, currChildrenLen-1)
	return nil
}

func (n *Node256[V]) getChild(ctx context.Context, key byte) (*INode[V], error) {
	child := n.children[key]
	if child == nil {
		return nil, childNodeNotFound
	}
	return child, nil
}

func (n *Node256[V]) getAllChildren(ctx context.Context, order Order) []*INode[V] {
	switch order {
	case AscOrder:
		res := make([]*INode[V], n.getChildrenLen(ctx))
		cnt := 0
		for k := 0; k < int(Node256PointersMax); k++ {
			child := n.children[k]
			if child == nil {
				continue
			}
			res[cnt] = child
			cnt += 1
		}
		return res
	case DescOrder:
		res := make([]*INode[V], n.getChildrenLen(ctx))
		cnt := 0
		for k := int(Node256PointersMax) - 1; k >= 0; k-- {
			child := n.children[k]
			if child == nil {
				continue
			}
			res[cnt] = child
			cnt += 1
		}
		return res
	default:
		// shouldn't go into that block
		return make([]*INode[V], n.getChildrenLen(ctx))
	}
}

func (n *Node256[V]) getChildByIndex(ctx context.Context, idx uint8) (byte, *INode[V], error) {
	currLen := n.getChildrenLen(ctx)
	if idx == currLen {
		return byte(0), nil, childNodeNotFound
	}

	cnt := 0
	for k := 0; k < int(Node256PointersMax); k++ {
		child := n.children[k]
		if child == nil {
			continue
		}
		if cnt == int(idx) {
			return byte(k), child, nil
		}
		cnt += 1
	}
	return byte(0), nil, childNodeNotFound
}

func (n *Node256[V]) grow(ctx context.Context) (*INode[V], error) {
	return nil, fmt.Errorf("node256 can not grow anymore")
}

// shrink to node48
func (n *Node256[V]) shrink(ctx context.Context) (*INode[V], error) {
	if !n.isShrinkable(ctx) {
		return nil, fmt.Errorf("node256 is still too big for shrinking")
	}

	currChildrenLen := n.getChildrenLen(ctx)
	if currChildrenLen == 0 {
		return nil, fmt.Errorf("node256 has 0 children, which is unexpected")
	}

	n48 := NewNode[V](KindNode48)
	n48.setPrefix(ctx, n.getPrefix(ctx))

	for k := 0; k < int(Node256PointersMax); k++ {
		child := n.children[k]
		if child == nil {
			continue
		}
		if err := n48.addChild(ctx, byte(k), child); err != nil {
			return nil, err
		}
	}

	return &n48, nil
}

func (n *Node256[V]) hasEnoughSpace(ctx context.Context) bool {
	// node256 is the biggest node so it always (supposed to be) has enough space
	return true
}

func (n *Node256[V]) isShrinkable(ctx context.Context) bool {
	return n.getChildrenLen(ctx) < Node256PointersMin
}

var _ INode[any] = (*Node256[any])(nil)
