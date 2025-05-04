package internal

import (
	"context"
	"errors"
	"fmt"
)

// InsertNode returns the previous value and an error indicating if any was set.
//
//	nodePtr: Pointer to the current node
//	Key, v: The target Key and value
//	offset: The number of bytes of the "Key" that have been processed
func InsertNode[V any](ctx context.Context, nodePtr *INode[V], key []byte, v V, offset uint8) (V, error) {
	if *nodePtr == nil {
		// Encounter an empty node.
		// Create a new leaf then replace the node pointer to the newly created leaf
		newLeaf := NewLeafWithKV[V](ctx, key, v)
		*nodePtr = newLeaf
		return *new(V), nil
	}

	if (*nodePtr).getKind(ctx) == KindNodeLeaf {
		currLeafKey := (*nodePtr).getPrefix(ctx)

		if isExactMatch(key, currLeafKey) {
			oldValue := (*nodePtr).getValue(ctx)
			(*nodePtr).setValue(ctx, v)
			return oldValue, nil
		}

		lcp := findLCP(key, currLeafKey, offset)
		// create the smallest inner node
		nn := NewNode[V](KindNode4)
		nn.setPrefix(ctx, key[offset:offset+lcp])
		offset += lcp
		// deep copy the current node to nodeCopy
		nodeCopy := new(INode[V])
		*nodeCopy = *nodePtr
		// add current leaf node to the new inner node
		if err := nn.addChild(ctx, currLeafKey[offset], nodeCopy); err != nil {
			return *new(V), fmt.Errorf("%w: %v", failedToAddChild, err)
		}
		// create new leaf node for the targeting Key, and add it to the inner node
		newLeaf := NewLeafWithKV[V](ctx, key, v)
		if err := nn.addChild(ctx, key[offset], &newLeaf); err != nil {
			return *new(V), fmt.Errorf("%w: %v", failedToAddChild, err)
		}
		// replace the current node with the newly created inner node
		*nodePtr = nn
		return *new(V), nil
	}

	matchedPrefixLen := (*nodePtr).checkPrefix(ctx, key, offset)
	if matchedPrefixLen != (*nodePtr).getPrefixLen(ctx) {
		currNodePrefix := (*nodePtr).getPrefix(ctx)
		nn := NewNode[V](KindNode4)
		nn.setPrefix(ctx, currNodePrefix[:matchedPrefixLen])

		newLeaf := NewLeafWithKV[V](ctx, key, v)
		if err := nn.addChild(ctx, key[offset+matchedPrefixLen], &newLeaf); err != nil {
			return *new(V), fmt.Errorf("%w: %v", failedToAddChild, err)
		}

		// adjust the current node compressed prefix accordingly
		// 1 character is in edge
		(*nodePtr).setPrefix(ctx, currNodePrefix[matchedPrefixLen+1:])
		// deep copy the current node to nodeCopy
		nodeCopy := new(INode[V])
		*nodeCopy = *nodePtr
		if err := nn.addChild(ctx, currNodePrefix[matchedPrefixLen], nodeCopy); err != nil {
			return *new(V), fmt.Errorf("%w: %v", failedToAddChild, err)
		}

		// replace the current node with the newly created inner node
		*nodePtr = nn
		return *new(V), nil
	}

	offset += (*nodePtr).getPrefixLen(ctx)
	childPtr, err := (*nodePtr).getChild(ctx, key[offset])
	if errors.Is(err, childNodeNotFound) {
		newLeaf := NewLeafWithKV[V](ctx, key, v)
		// grow to a bigger node if don't have enough space
		if !(*nodePtr).hasEnoughSpace(ctx) {
			biggerNodePtr, err := (*nodePtr).grow(ctx)
			if err != nil {
				return *new(V), fmt.Errorf("%w: %v", failedToGrowNode, err)
			}
			*nodePtr = *biggerNodePtr
		}
		if err := (*nodePtr).addChild(ctx, key[offset], &newLeaf); err != nil {
			return *new(V), fmt.Errorf("%w: %v", failedToAddChild, err)
		}
		return *new(V), nil
	}

	return InsertNode[V](ctx, childPtr, key, v, offset+1)
}

// RemoveNode is used to delete a given Key. Returns the old value if any
//
// Parameters:
// nodePtr: Pointer to the current node
// Key: The target Key
// offset: The number of bytes of the "Key" that have been processed
//
// Output:
// 1. old value before removal
// 2. is the "Child" node removable ?
// 3.removal error ?
func RemoveNode[V any](ctx context.Context, nodePtr *INode[V], key []byte, offset uint8) (V, bool, error) {
	if *nodePtr == nil || len(key) == 0 {
		return *new(V), false, noSuchKey
	}

	if (*nodePtr).getKind(ctx) == KindNodeLeaf {
		leafKey, leafV := (*nodePtr).getPrefix(ctx), (*nodePtr).getValue(ctx)
		if !isExactMatch(key, leafKey) {
			return *new(V), false, noSuchKey
		}

		return leafV, true, nil
	}
	matchedPrefixLen := (*nodePtr).checkPrefix(ctx, key, offset)
	if matchedPrefixLen != (*nodePtr).getPrefixLen(ctx) {
		return *new(V), false, noSuchKey
	}

	offset += (*nodePtr).getPrefixLen(ctx)
	childPtr, err := (*nodePtr).getChild(ctx, key[offset])
	if err != nil {
		return *new(V), false, noSuchKey
	}

	removedV, isChildRemovable, removeErr := RemoveNode[V](ctx, childPtr, key, offset+1)

	if removeErr != nil || !isChildRemovable {
		return removedV, isChildRemovable, removeErr
	}

	if err := (*nodePtr).removeChild(ctx, key[offset]); err != nil {
		return *new(V), false, fmt.Errorf("%w: %v", failedToRemoveChild, err)
	}

	switch (*nodePtr).getChildrenLen(ctx) {
	case 0:
		// mark the current node to be removable
		return removedV, true, nil
	case 1:
		// replace the node with its only Child and adjust the compressed prefix path
		// and NOT propagate the deletion action to the upper nodes.
		currNodePrefix := (*nodePtr).getPrefix(ctx)
		onlyChildK, onlyChildPtr, err := (*nodePtr).getChildByIndex(ctx, 0)
		if err != nil {
			return *new(V), false, err
		}
		var newPrefix []byte
		if (*onlyChildPtr).getKind(ctx) == KindNodeLeaf {
			newPrefix = (*onlyChildPtr).getPrefix(ctx)
		} else {
			newPrefix := append(currNodePrefix, onlyChildK)
			newPrefix = append(newPrefix, (*onlyChildPtr).getPrefix(ctx)...)
		}
		*nodePtr = *onlyChildPtr
		(*nodePtr).setPrefix(ctx, newPrefix)

		return removedV, false, nil
	}

	// shrink to a smaller node to save resources
	if (*nodePtr).isShrinkable(ctx) {
		smallerNodePtr, err := (*nodePtr).shrink(ctx)
		if err != nil {
			return *new(V), false, fmt.Errorf("%w: %v", failedToShrinkNode, err)
		}
		*nodePtr = *smallerNodePtr
	}

	return removedV, false, nil
}

// Get is used to look up a specific Key, returning the value and if it was found
//
//	node: The current node
//	Key: The target Key
//	offset: The number of bytes of the "Key" that have been processed
func Get[V any](ctx context.Context, node INode[V], key []byte, offset uint8) (V, error) {
	if node == nil {
		return *new(V), noSuchKey
	}

	// As we use the Single-value leaf, so the value must be always found in the leaf node,
	// not in the inner nodes
	if node.getKind(ctx) == KindNodeLeaf {
		currLeafKey := node.getPrefix(ctx)
		if isExactMatch(key, currLeafKey) {
			return node.getValue(ctx), nil
		}

		return *new(V), noSuchKey
	}

	matchedPrefixLen := node.checkPrefix(ctx, key, offset)
	if matchedPrefixLen != node.getPrefixLen(ctx) {
		return *new(V), noSuchKey
	}

	offset += node.getPrefixLen(ctx)
	childPtr, err := node.getChild(ctx, key[offset])
	if err != nil {
		return *new(V), noSuchKey
	}

	return Get[V](ctx, *childPtr, key, offset+1)
}

// Walk iterates over all keys in the tree and trigger the callback function.
func Walk[V any](ctx context.Context, node INode[V], cb Callback[V], order Order) {
	if node == nil {
		return
	}

	if node.getKind(ctx) == KindNodeLeaf {
		cb(ctx, node.getPrefix(ctx), node.getValue(ctx))
	}

	children := node.getAllChildren(ctx, order)
	for _, childPtr := range children {
		Walk[V](ctx, *childPtr, cb, order)
	}
}

func NewLeafWithKV[V any](ctx context.Context, key []byte, v V) INode[V] {
	newLeaf := NewNode[V](KindNodeLeaf)
	newLeaf.setPrefix(ctx, key)
	newLeaf.setValue(ctx, v)
	return newLeaf
}
