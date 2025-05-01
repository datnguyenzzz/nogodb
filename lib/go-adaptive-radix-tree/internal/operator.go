package internal

import (
	"bytes"
	"context"
)

// InsertNode returns the previous value and an error indicating if any was set.
//
//	nodePtr: Pointer to the current node
//	key, v: The target key and value
//	offset: The number of bytes of the "key" that have been processed
func InsertNode[V any](ctx context.Context, nodePtr *INode[V], key []byte, v V, offset uint8) (V, error) {
	//Lazy expansion technique: inner nodes are only created if they are required
	//to distinguish at least 2 leaf nodes
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
		err := nn.addChild(ctx, currLeafKey[offset], nodePtr)
		if err != nil {
			return *new(V), failedToAddChild
		}
		// create new leaf node for the targeting key, and add it to the inner node
		newLeaf := newLeafWithKV[V](ctx, key, v)
		err = nn.addChild(ctx, key[offset], &newLeaf)
		if err != nil {
			return *new(V), failedToAddChild
		}
		// replace the current node with the newly created inner node
		*nodePtr = nn

		return *new(V), nil
	}

	matchedPrefixLen := node.checkPrefix(ctx, key, offset)
	if matchedPrefixLen != node.getPrefixLen(ctx) {
		//   A new inner node is created above the current node and the compressed paths are adjusted accordingly
	}

	// TODO Else continue the insertion
	//    1. Find a child of node with offset = offset + node.prefixLen
	//    2. If child exists --> continue InsertNode(child, key, value, offset + 1)
	//    3. If child doesn't exists --> add new leaf to the currNode (grow(currNode) if necessary)

	return *new(V), nil
}

func newLeafWithKV[V any](ctx context.Context, key []byte, v V) INode[V] {
	newLeaf := newNode[V](KindNodeLeaf)
	newLeaf.setPrefix(ctx, key)
	newLeaf.setValue(ctx, v)
	return newLeaf
}
