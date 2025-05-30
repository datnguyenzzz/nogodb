package internal

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_node256_str_insertAndRemoveChildren(t *testing.T) {
	type param struct {
		desc                 string
		actions              []NodeAction[string]
		expectedKeys         []byte // non 0 keys
		expectedChildrenLen  uint8
		expectedAscChildren  []*INode[string] // non nil pointers
		expectedDescChildren []*INode[string] // non nil pointers
		expectedGetChild     map[byte]*INode[string]
	}

	sampleLeaves := generateStringLeaves(4)

	testList := []param{
		{
			desc: "Happy case: #1",
			actions: []NodeAction[string]{
				{
					Kind:  InsertAction,
					Key:   1,
					Child: &sampleLeaves[0],
				},
				{
					Kind:  InsertAction,
					Key:   2,
					Child: &sampleLeaves[1],
				},
				{
					Kind:  InsertAction,
					Key:   3,
					Child: &sampleLeaves[2],
				},
			},
			expectedKeys:        []byte{1, 2, 3},
			expectedChildrenLen: 3,
			expectedAscChildren: []*INode[string]{
				&sampleLeaves[0],
				&sampleLeaves[1],
				&sampleLeaves[2],
			},
			expectedDescChildren: []*INode[string]{
				&sampleLeaves[2],
				&sampleLeaves[1],
				&sampleLeaves[0],
			},
			expectedGetChild: map[byte]*INode[string]{
				1: &sampleLeaves[0],
				2: &sampleLeaves[1],
				3: &sampleLeaves[2],
			},
		},
		{
			desc: "Happy case: #2",
			actions: []NodeAction[string]{
				{
					Kind:  InsertAction,
					Key:   1,
					Child: &sampleLeaves[0],
				},
				{
					Kind: RemoveAction,
					Key:  1,
				},
				{
					Kind:  InsertAction,
					Key:   1,
					Child: &sampleLeaves[3],
				},
			},
			expectedKeys:        []byte{1},
			expectedChildrenLen: 1,
			expectedAscChildren: []*INode[string]{
				&sampleLeaves[3],
			},
			expectedDescChildren: []*INode[string]{
				&sampleLeaves[3],
			},
			expectedGetChild: map[byte]*INode[string]{
				1: &sampleLeaves[3],
			},
		},
		{
			desc: "Happy case: #3",
			actions: []NodeAction[string]{
				{
					Kind:  InsertAction,
					Key:   1,
					Child: &sampleLeaves[0],
				},
				{
					Kind:  InsertAction,
					Key:   2,
					Child: &sampleLeaves[1],
				},
				{
					Kind: RemoveAction,
					Key:  1,
				},
				{
					Kind: RemoveAction,
					Key:  2,
				},
			},
			expectedKeys:         []byte{},
			expectedChildrenLen:  0,
			expectedAscChildren:  []*INode[string]{},
			expectedDescChildren: []*INode[string]{},
			expectedGetChild:     map[byte]*INode[string]{},
		},
		{
			desc: "Happy case: #4",
			actions: []NodeAction[string]{
				{
					Kind:  InsertAction,
					Key:   1,
					Child: &sampleLeaves[0],
				},
				{
					Kind:  InsertAction,
					Key:   2,
					Child: &sampleLeaves[1],
				},
				{
					Kind:  InsertAction,
					Key:   3,
					Child: &sampleLeaves[2],
				},
				{
					Kind: RemoveAction,
					Key:  2,
				},
				{
					Kind:  InsertAction,
					Key:   4,
					Child: &sampleLeaves[3],
				},
			},
			expectedKeys:        []byte{1, 3, 4},
			expectedChildrenLen: 3,
			expectedAscChildren: []*INode[string]{
				&sampleLeaves[0],
				&sampleLeaves[2],
				&sampleLeaves[3],
			},
			expectedDescChildren: []*INode[string]{
				&sampleLeaves[3],
				&sampleLeaves[2],
				&sampleLeaves[0],
			},
			expectedGetChild: map[byte]*INode[string]{
				1: &sampleLeaves[0],
				3: &sampleLeaves[2],
				4: &sampleLeaves[3],
			},
		},
		{
			desc: "Happy case: #5",
			actions: []NodeAction[string]{
				{
					Kind:  InsertAction,
					Key:   3,
					Child: &sampleLeaves[0],
				},
				{
					Kind:  InsertAction,
					Key:   2,
					Child: &sampleLeaves[1],
				},
				{
					Kind:  InsertAction,
					Key:   1,
					Child: &sampleLeaves[2],
				},
				{
					Kind: RemoveAction,
					Key:  2,
				},
				{
					Kind:  InsertAction,
					Key:   4,
					Child: &sampleLeaves[3],
				},
			},
			expectedKeys:        []byte{1, 3, 4},
			expectedChildrenLen: 3,
			expectedAscChildren: []*INode[string]{
				&sampleLeaves[2],
				&sampleLeaves[0],
				&sampleLeaves[3],
			},
			expectedDescChildren: []*INode[string]{
				&sampleLeaves[3],
				&sampleLeaves[0],
				&sampleLeaves[2],
			},
			expectedGetChild: map[byte]*INode[string]{
				1: &sampleLeaves[2],
				3: &sampleLeaves[0],
				4: &sampleLeaves[3],
			},
		},
		{
			desc: "Happy case: #6",
			actions: []NodeAction[string]{
				{
					Kind:  InsertAction,
					Key:   3,
					Child: &sampleLeaves[0],
				},
				{
					Kind:  InsertAction,
					Key:   2,
					Child: &sampleLeaves[1],
				},
				{
					Kind:  InsertAction,
					Key:   4,
					Child: &sampleLeaves[1],
				},
				{
					Kind:  InsertAction,
					Key:   1,
					Child: &sampleLeaves[2],
				},
				{
					Kind: RemoveAction,
					Key:  2,
				},
				{
					Kind: RemoveAction,
					Key:  4,
				},
				{
					Kind:  InsertAction,
					Key:   2,
					Child: &sampleLeaves[3],
				},
				{
					Kind:  InsertAction,
					Key:   4,
					Child: &sampleLeaves[3],
				},
			},
			expectedKeys:        []byte{1, 2, 3, 4},
			expectedChildrenLen: 4,
			expectedAscChildren: []*INode[string]{
				&sampleLeaves[2],
				&sampleLeaves[3],
				&sampleLeaves[0],
				&sampleLeaves[3],
			},
			expectedDescChildren: []*INode[string]{
				&sampleLeaves[3],
				&sampleLeaves[0],
				&sampleLeaves[3],
				&sampleLeaves[2],
			},
			expectedGetChild: map[byte]*INode[string]{
				1: &sampleLeaves[2],
				2: &sampleLeaves[3],
				3: &sampleLeaves[0],
				4: &sampleLeaves[3],
			},
		},
		{
			desc: "Happy case: #7",
			actions: []NodeAction[string]{
				{
					Kind:  InsertAction,
					Key:   3,
					Child: &sampleLeaves[0],
				},
				{
					Kind:  InsertAction,
					Key:   2,
					Child: &sampleLeaves[1],
				},
				{
					Kind: RemoveAction,
					Key:  2,
				},
				{
					Kind:  InsertAction,
					Key:   4,
					Child: &sampleLeaves[3],
				},
				{
					Kind:  InsertAction,
					Key:   1,
					Child: &sampleLeaves[2],
				},
			},
			expectedKeys:        []byte{1, 3, 4},
			expectedChildrenLen: 3,
			expectedAscChildren: []*INode[string]{
				&sampleLeaves[2],
				&sampleLeaves[0],
				&sampleLeaves[3],
			},
			expectedDescChildren: []*INode[string]{
				&sampleLeaves[3],
				&sampleLeaves[0],
				&sampleLeaves[2],
			},
			expectedGetChild: map[byte]*INode[string]{
				1: &sampleLeaves[2],
				3: &sampleLeaves[0],
				4: &sampleLeaves[3],
			},
		},
	}

	for _, tc := range testList {
		t.Run(tc.desc, func(t *testing.T) {
			n256 := NewNode[string](KindNode256)
			ctx := context.Background()
			// perform actions
			for _, action := range tc.actions {
				if action.Kind == InsertAction {
					err := n256.addChild(ctx, action.Key, action.Child)
					assert.NoError(t, err)
				} else {
					err := n256.removeChild(ctx, action.Key)
					assert.NoError(t, err)
				}
			}
			// verify output
			n256o, ok := n256.(*Node256[string])
			assert.True(t, ok, "can not cast to Node4[string]")
			//// fill expectedChildren with nil pointer
			var expectedChildren [Node256PointersMax]*INode[string]
			for i, children := range tc.expectedAscChildren {
				expectedChildren[tc.expectedKeys[i]] = children
			}
			assert.Equal(t, n256o.children, expectedChildren, "node children is different")
			assert.Equal(t, n256o.getChildrenLen(ctx), tc.expectedChildrenLen, "node children length is different")
			assert.Equal(t, n256o.getAllChildren(ctx, AscOrder), tc.expectedAscChildren, "node children in ASC is different")
			assert.Equal(t, n256o.getAllChildren(ctx, DescOrder), tc.expectedDescChildren, "node children in DESC is different")
			for k, expectedChild := range tc.expectedGetChild {
				child, err := n256o.getChild(ctx, k)
				assert.NoError(t, err)
				assert.Equal(t, child, expectedChild)
			}
		})
	}
}

func Test_node256_str_grow(t *testing.T) {
	ctx := context.Background()
	n256 := NewNode[string](KindNode256)

	samplePrefix := RandomBytes(5)
	n256.setPrefix(ctx, samplePrefix)

	sampleLeaves := generateStringLeaves(int(Node48PointersMax))
	// Add children to the node until it reaches its space capacity
	var children []*INode[string]
	for idx := byte(0); idx < Node48PointersMax; idx++ {
		leaf := sampleLeaves[idx]
		children = append(children, &leaf)
		err := n256.addChild(ctx, idx, &leaf)
		assert.NoError(t, err, fmt.Sprintf("shouldn't fail to add new Child with Key - %v", idx))
	}

	// grow to bigger node
	_, err := n256.grow(ctx)
	assert.Error(t, err, "shouldn't succeed to grow")
}

func Test_node256_str_shrink(t *testing.T) {
	ctx := context.Background()
	n256 := NewNode[string](KindNode256)

	samplePrefix := RandomBytes(5)
	n256.setPrefix(ctx, samplePrefix)

	sampleLeaves := generateStringLeaves(int(Node256PointersMin - 1))
	// Add children to the node which is lower than the minimum required capacity
	var keys []byte
	var children []*INode[string]
	for idx := byte(0); idx < Node256PointersMin-1; idx++ {
		leaf := sampleLeaves[idx]
		keys = append(keys, idx)
		children = append(children, &leaf)
		err := n256.addChild(ctx, idx, &leaf)
		assert.NoError(t, err, fmt.Sprintf("shouldn't fail to add new Child with Key - %v", idx))
	}

	// shrink to smaller node
	nn, err := n256.shrink(ctx)
	assert.NoError(t, err, "shouldn't fail to shrink")
	// verify output
	n48 := *nn
	n48o, ok := n48.(*Node48[string])
	assert.True(t, ok, "can not cast to Node48[string]")
	assert.Equal(t, n48o.getPrefix(ctx), samplePrefix)
	assert.Equal(t, n48o.getKind(ctx), KindNode48)
	// fill expectedKeys with 0
	var expectedKeys [Node48KeysLen]byte
	for _, key := range keys {
		expectedKeys[key] = key + 1
	}
	assert.Equal(t, n48o.keys, expectedKeys, "node keys is different")
	// fill expectedChildren with nil pointer
	var expectedChildren [Node48PointersMax]*INode[string]
	copy(expectedChildren[:Node256PointersMin-1], children)
	assert.Equal(t, n48o.children, expectedChildren, "node children is different")
}
