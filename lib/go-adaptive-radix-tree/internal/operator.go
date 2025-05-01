package internal

import (
	"bytes"
	"context"
	"errors"
)

// InsertNode returns the previous value and an error indicating if any was set.
//
//	nodePtr: Pointer to the current node
//	key, v: The target key and value
//	offset: The number of bytes of the "key" that have been processed
func InsertNode[V any](ctx context.Context, nodePtr *INode[V], key []byte, v V, offset uint8) (V, error) {
	if nodePtr == nil {
		// Encounter an empty node.
		// Create a new leaf then replace the node pointer to the newly created leaf
		newLeaf := newLeafWithKV[V](ctx, key, v)
		*nodePtr = newLeaf
		return *new(V), nil
	}

	node := *nodePtr
	if node.getKind(ctx) == KindNodeLeaf {
		currLeafKey := node.getPrefix(ctx)

		if len(key) == len(currLeafKey) && bytes.Equal(currLeafKey, key) {
			oldValue := node.getValue(ctx)
			node.setValue(ctx, v)
			return oldValue, nil
		}

		lcp := findLCP(key, currLeafKey, offset)
		// create the smallest inner node
		nn := newNode[V](KindNode4)
		nn.setPrefix(ctx, key[offset:offset+lcp])
		offset += lcp
		// add current leaf node to the new inner node
		if err := nn.addChild(ctx, currLeafKey[offset], nodePtr); err != nil {
			return *new(V), failedToAddChild
		}
		// create new leaf node for the targeting key, and add it to the inner node
		newLeaf := newLeafWithKV[V](ctx, key, v)
		if err := nn.addChild(ctx, key[offset], &newLeaf); err != nil {
			return *new(V), failedToAddChild
		}
		// replace the current node with the newly created inner node
		*nodePtr = nn
		return *new(V), nil
	}

	matchedPrefixLen := node.checkPrefix(ctx, key, offset)
	if matchedPrefixLen != node.getPrefixLen(ctx) {
		currNodePrefix := node.getPrefix(ctx)
		nn := newNode[V](KindNode4)
		nn.setPrefix(ctx, currNodePrefix[:matchedPrefixLen])

		newLeaf := newLeafWithKV[V](ctx, key, v)
		if err := nn.addChild(ctx, key[offset+matchedPrefixLen], &newLeaf); err != nil {
			return *new(V), failedToAddChild
		}

		// adjust the current node compressed prefix accordingly
		node.setPrefix(ctx, currNodePrefix[matchedPrefixLen:])
		if err := nn.addChild(ctx, currNodePrefix[matchedPrefixLen], nodePtr); err != nil {
			return *new(V), failedToAddChild
		}

		// replace the current node with the newly created inner node
		*nodePtr = nn
		return *new(V), nil
	}

	offset += node.getPrefixLen(ctx)
	child, err := node.getChild(ctx, key[offset])
	if errors.Is(err, childNotFound) {
		newLeaf := newLeafWithKV[V](ctx, key, v)
		if err := node.addChild(ctx, key[offset], &newLeaf); err != nil {
			return *new(V), failedToAddChild
		}
		return *new(V), nil
	}

	return InsertNode[V](ctx, &child, key, v, offset+1)
}

func newLeafWithKV[V any](ctx context.Context, key []byte, v V) INode[V] {
	newLeaf := newNode[V](KindNodeLeaf)
	newLeaf.setPrefix(ctx, key)
	newLeaf.setValue(ctx, v)
	return newLeaf
}
