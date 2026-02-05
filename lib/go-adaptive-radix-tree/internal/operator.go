package internal

import (
	"context"
	"errors"
	"fmt"
	"slices"
)

// InsertNode returns the previous value and an error indicating if any was set.
//
//	nodePtr: Pointer to the current node
//	Key, v: The target Key and value
//	offset: The number of bytes of the "Key" that have been processed
func InsertNode[V any](
	ctx context.Context,
	nodePtr *INode[V],
	parentPtr *INode[V],
	key []byte,
	v V,
	offset uint8,
) (V, error) {
	if *nodePtr == nil {
		newLeaf := NewLeafWithKV[V](ctx, key, v)
		*nodePtr = newLeaf
		(*parentPtr).GetLocker().Unlock()
		return *new(V), nil
	}

	(*nodePtr).GetLocker().Lock()

	if (*nodePtr).GetKind(ctx) == KindNodeLeaf {
		currLeafKey := (*nodePtr).getPrefix(ctx)

		if isExactMatch(key, currLeafKey) {
			oldValue := (*nodePtr).getValue(ctx)
			(*nodePtr).setValue(ctx, v)
			(*nodePtr).GetLocker().Unlock()
			(*parentPtr).GetLocker().Unlock()
			return oldValue, nil
		}

		lcp := findLCP(key, currLeafKey, offset)
		nn := NewNode[V](KindNode4)
		nn.setPrefix(ctx, key[offset:offset+lcp])
		nn.setLocker((*nodePtr).GetLocker())
		offset += lcp

		nodeCopy := (*nodePtr).clone()
		nodeCopy.setLocker(NewNodeLocker(RWMutexLocker))

		// add current leaf node to the new inner node
		// but it needs a new locker
		if err := nn.addChild(ctx, currLeafKey[offset], &nodeCopy); err != nil {
			return *new(V), fmt.Errorf("%w: %v", failedToAddChild, err)
		}

		// create new leaf node for the targeting Key, and add it to the inner node
		newLeaf := NewLeafWithKV(ctx, key, v)
		if err := nn.addChild(ctx, key[offset], &newLeaf); err != nil {
			return *new(V), fmt.Errorf("%w: %v", failedToAddChild, err)
		}
		// replace the current node with the newly created inner node
		*nodePtr = nn

		(*parentPtr).GetLocker().Unlock()
		(*nodePtr).GetLocker().Unlock()
		return *new(V), nil
	}

	matchedPrefixLen := (*nodePtr).checkPrefix(ctx, key, offset)
	if matchedPrefixLen != (*nodePtr).getPrefixLen(ctx) {
		currNodePrefix := (*nodePtr).getPrefix(ctx)
		nn := NewNode[V](KindNode4)
		nn.setPrefix(ctx, currNodePrefix[:matchedPrefixLen])
		nn.setLocker((*nodePtr).GetLocker())

		newLeaf := NewLeafWithKV(ctx, key, v)
		if err := nn.addChild(ctx, key[offset+matchedPrefixLen], &newLeaf); err != nil {
			return *new(V), fmt.Errorf("%w: %v", failedToAddChild, err)
		}

		// adjust the current node compressed prefix accordingly
		// 1 character is in edge
		(*nodePtr).setPrefix(ctx, currNodePrefix[matchedPrefixLen+1:])

		// deep copy the current node to a new node nodeCopy
		nodeCopy := (*nodePtr).clone()
		nodeCopy.setLocker(NewNodeLocker(RWMutexLocker))

		if err := nn.addChild(ctx, currNodePrefix[matchedPrefixLen], &nodeCopy); err != nil {
			return *new(V), fmt.Errorf("%w: %v", failedToAddChild, err)
		}

		// replace the current node with the newly created inner node
		*nodePtr = nn

		(*parentPtr).GetLocker().Unlock()
		(*nodePtr).GetLocker().Unlock()
		return *new(V), nil
	}

	offset += (*nodePtr).getPrefixLen(ctx)
	nextNodePtr, err := (*nodePtr).getChild(ctx, key[offset])
	if errors.Is(err, childNodeNotFound) {
		newLeaf := NewLeafWithKV(ctx, key, v)
		// grow to a bigger node if don't have enough space
		shouldGrow := !(*nodePtr).hasEnoughSpace(ctx)
		if shouldGrow {
			biggerNodePtr, err := (*nodePtr).grow(ctx)
			if err != nil {
				return *new(V), fmt.Errorf("current node type - %v, %w: %v", (*nodePtr).GetKind(ctx), failedToGrowNode, err)
			}
			currLocker := (*nodePtr).GetLocker()
			*nodePtr = *biggerNodePtr
			(*nodePtr).setLocker(currLocker)
		}

		if err := (*nodePtr).addChild(ctx, key[offset], &newLeaf); err != nil {
			return *new(V), fmt.Errorf("%w: %v", failedToAddChild, err)
		}

		(*nodePtr).GetLocker().Unlock()
		(*parentPtr).GetLocker().Unlock()
		return *new(V), nil
	}

	(*parentPtr).GetLocker().Unlock()
	return InsertNode(ctx, nextNodePtr, nodePtr, key, v, offset+1)
}

func RemoveNode[V any](
	ctx context.Context,
	nodePtr *INode[V],
	parentPtr *INode[V],
	key []byte,
	offset uint8,
) (V, bool, error) {
	if nodePtr == nil || *nodePtr == nil {
		(*parentPtr).GetLocker().Unlock()
		return *new(V), false, NoSuchKey
	}

	(*nodePtr).GetLocker().Lock()

	if nodePtr == nil || *nodePtr == nil {
		(*parentPtr).GetLocker().Unlock()
		return *new(V), false, NoSuchKey
	}

	if (*nodePtr).GetKind(ctx) == KindNodeLeaf {
		// special case when a tree has only 1 node which is a leaf
		v, err := tryToRemoveLeafNode(ctx, key, nodePtr)
		(*nodePtr).GetLocker().Unlock()
		(*parentPtr).GetLocker().Unlock()
		return v, err == nil, err
	}

	matchedPrefixLen := (*nodePtr).checkPrefix(ctx, key, offset)
	if matchedPrefixLen != (*nodePtr).getPrefixLen(ctx) {
		(*nodePtr).GetLocker().Unlock()
		(*parentPtr).GetLocker().Unlock()
		return *new(V), false, NoSuchKey
	}

	offset += (*nodePtr).getPrefixLen(ctx)
	nextNodePtr, err := (*nodePtr).getChild(ctx, key[offset])
	if err != nil {
		(*nodePtr).GetLocker().Unlock()
		(*parentPtr).GetLocker().Unlock()
		return *new(V), false, NoSuchKey
	}

	if (*nextNodePtr).GetKind(ctx) == KindNodeLeaf {
		v, err := tryToRemoveLeafNode(ctx, key, nextNodePtr)
		if err != nil {
			(*nodePtr).GetLocker().Unlock()
			(*parentPtr).GetLocker().Unlock()
			return v, false, err
		}
		// attempt to remove the leaf node and unify the inner node if needed

		(*nextNodePtr).GetLocker().Lock()
		if err := (*nodePtr).removeChild(ctx, key[offset]); err != nil {
			return *new(V), false, fmt.Errorf("fail to remove child, current node type - %v, %w: %v", (*nodePtr).GetKind(ctx), failedToRemoveChild, err)
		}
		(*nextNodePtr).cleanup(ctx)
		(*nextNodePtr).GetLocker().Unlock()

		switch (*nodePtr).getChildrenLen(ctx) {
		case 0:
			prevOffset := offset - (*nodePtr).getPrefixLen(ctx) - 1
			if err = (*parentPtr).removeChild(ctx, prevOffset); err != nil {
				return *new(V), false, fmt.Errorf("fail to remove child, current node type - %v, %w: %v", (*parentPtr).GetKind(ctx), failedToRemoveChild, err)
			}
			(*nodePtr).cleanup(ctx)
		case 1:
			onlyChildK, onlyChildPtr, err := (*nodePtr).getChildByIndex(ctx, 0)
			if err != nil {
				return *new(V), false, err
			}
			(*onlyChildPtr).GetLocker().RLock()
			var newPrefix []byte
			if (*onlyChildPtr).GetKind(ctx) == KindNodeLeaf {
				newPrefix = (*onlyChildPtr).getPrefix(ctx)
			} else {
				// newPrefix := append((*nodePtr).getPrefix(ctx), onlyChildK)
				// newPrefix = append(newPrefix, (*onlyChildPtr).getPrefix(ctx)...)
				newPrefix = slices.Concat((*nodePtr).getPrefix(ctx), []byte{onlyChildK}, (*onlyChildPtr).getPrefix(ctx))
			}
			nodeCopy := (*onlyChildPtr).clone()
			nodeCopy.setLocker((*nodePtr).GetLocker())
			nodeCopy.setPrefix(ctx, newPrefix)
			(*onlyChildPtr).GetLocker().RUnlock()

			*nodePtr = nodeCopy
		}

		if (*nodePtr).isShrinkable(ctx) {
			smallerNodePtr, err := (*nodePtr).shrink(ctx)
			if err != nil {
				return *new(V), false, fmt.Errorf("%w: %v", failedToShrinkNode, err)
			}

			currLocker := (*nodePtr).GetLocker()
			*nodePtr = *smallerNodePtr
			(*nodePtr).setLocker(currLocker)
		}

		(*nodePtr).GetLocker().Unlock()
		(*parentPtr).GetLocker().Unlock()
		return v, false, nil
	}

	(*parentPtr).GetLocker().Unlock()
	return RemoveNode(ctx, nextNodePtr, nodePtr, key, offset+1)
}

func tryToRemoveLeafNode[V any](ctx context.Context, key []byte, nodePtr *INode[V]) (V, error) {
	if !isExactMatch(key, (*nodePtr).getPrefix(ctx)) {
		return *new(V), NoSuchKey
	}

	return (*nodePtr).getValue(ctx), nil
}

// Get is used to look up a specific Key, returning the value and if it was found
//
//	node: The current node
//	Key: The target Key
//	offset: The number of bytes of the "Key" that have been processed
func Get[V any](ctx context.Context, node INode[V], key []byte, offset uint8) (V, error) {
	if node == nil {
		return *new(V), NoSuchKey
	}

	node.GetLocker().RLock()

	// As we use the Single-value leaf, so the value must be always found in the leaf node,
	// not in the inner nodes
	if node.GetKind(ctx) == KindNodeLeaf {
		currLeafKey := node.getPrefix(ctx)
		if isExactMatch(key, currLeafKey) {
			node.GetLocker().RUnlock()
			return node.getValue(ctx), nil
		}

		node.GetLocker().RUnlock()
		return *new(V), NoSuchKey
	}

	matchedPrefixLen := node.checkPrefix(ctx, key, offset)
	if matchedPrefixLen != node.getPrefixLen(ctx) {
		node.GetLocker().RUnlock()
		return *new(V), NoSuchKey
	}

	offset += node.getPrefixLen(ctx)
	childPtr, err := node.getChild(ctx, key[offset])
	if err != nil {
		node.GetLocker().RUnlock()
		return *new(V), NoSuchKey
	}

	node.GetLocker().RUnlock()
	return Get[V](ctx, *childPtr, key, offset+1)
}

// Walk iterates over all keys in the tree and trigger the callback function.
func Walk[V any](ctx context.Context, node INode[V], cb Callback[V], order Order) {
	if node == nil {
		return
	}

	node.GetLocker().RLock()

	if node.GetKind(ctx) == KindNodeLeaf {
		cb(ctx, node.getPrefix(ctx), node.getValue(ctx))
		node.GetLocker().RUnlock()
		return
	}

	children := node.getAllChildren(ctx, order)
	node.GetLocker().RUnlock()
	for _, childPtr := range children {
		Walk[V](ctx, *childPtr, cb, order)
	}
}

func Visualize[V any](ctx context.Context, node INode[V]) {
	if node == nil {
		return
	}
	for _, childPtr := range node.getAllChildren(ctx, AscOrder) {
		fmt.Printf("%p --> %p\n", node.GetLocker(), (*childPtr).GetLocker())
	}
	for _, childPtr := range node.getAllChildren(ctx, AscOrder) {
		Visualize(ctx, *childPtr)
	}
}

func NewLeafWithKV[V any](ctx context.Context, key []byte, v V) INode[V] {
	newLeaf := NewNode[V](KindNodeLeaf)
	newLeaf.setPrefix(ctx, key)
	newLeaf.setValue(ctx, v)
	return newLeaf
}
