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
	if nodePtr == nil {
		// Encounter an empty node.
		// Create a new leaf then replace the node pointer to the newly created leaf
		newLeaf := newNode[V](KindNodeLeaf)
		newLeaf.setPrefix(ctx, key)
		newLeaf.setValue(ctx, v)
		*nodePtr = newLeaf
		return *new(V), nil
	}

	node := *nodePtr
	if node.getKind(ctx) == KindNodeLeaf {
		//Lazy expansion technique: inner nodes are only created if they are required
		//to distinguish at least 2 leaf nodes
		currLeafKey := node.getPrefix(ctx)

		if len(key) == len(currLeafKey) && bytes.Equal(currLeafKey, key) {
			oldValue := node.getValue(ctx)
			node.setValue(ctx, v)
			return oldValue, nil
		}

		lcp := findLCP(key, currLeafKey, offset)
		nn4 := newNode[V](KindNode4)
		nn4.setPrefix(ctx, key[offset:offset+lcp])
		offset += lcp
		// add current leaf node to the new inner node_4
		err := nn4.addChild(ctx, currLeafKey[offset], nodePtr)
		if err != nil {
			return *new(V), failedToAddChild
		}
		// create new leaf node for the targeting key, and add it to the inner node_4
		newLeaf := newNode[V](KindNodeLeaf)
		newLeaf.setPrefix(ctx, key)
		newLeaf.setValue(ctx, v)
		err = nn4.addChild(ctx, key[offset], &newLeaf)
		if err != nil {
			return *new(V), failedToAddChild
		}
		// replace the current node with the inner node_4
		*nodePtr = nn4

		return *new(V), nil
	}
	//TODO p = checkPrefix compares the compressed path of a node with the key and returns the number of equal bytes

	// TODO checkPrefix() != node.prefixLen
	//   A new inner node is created above the current node and the compressed paths are adjusted accordingly

	// TODO Else continue the insertion
	//    1. Find a child of node with offset = offset + node.prefixLen
	//    2. If child exists --> continue InsertNode(child, key, value, offset + 1)
	//    3. If child doesn't exists --> add new leaf to the currNode (grow(currNode) if necessary)

	return *new(V), nil
}
