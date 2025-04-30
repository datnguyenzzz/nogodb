package internal

import "context"

func InsertNode[V any](ctx context.Context, nodePtr *INode[V], prefix []byte, v V, offset uint16) (V, error) {
	if nodePtr == nil {
		// Encounter an empty node.
		// Create a new leaf then replace the node pointer to the newly created leaf
		newLeaf := newNode[V](KindNodeLeaf)
		newLeaf.setPrefix(ctx, prefix)
		newLeaf.setValue(ctx, v)
		*nodePtr = newLeaf
		return v, nil
	}

	node := *nodePtr
	if node.getKind(ctx) == KindNodeLeaf {
		// TODO grow the current leaf node to node 4 and append new children to it
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
