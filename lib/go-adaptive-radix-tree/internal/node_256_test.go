package internal

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_node256_insertAndRemoveChildren_str(t *testing.T) {
	type param struct {
		desc                 string
		actions              []nodeAction[string]
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
			actions: []nodeAction[string]{
				{
					kind:  insertAction,
					key:   1,
					child: &sampleLeaves[0],
				},
				{
					kind:  insertAction,
					key:   2,
					child: &sampleLeaves[1],
				},
				{
					kind:  insertAction,
					key:   3,
					child: &sampleLeaves[2],
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
			actions: []nodeAction[string]{
				{
					kind:  insertAction,
					key:   1,
					child: &sampleLeaves[0],
				},
				{
					kind: removeAction,
					key:  1,
				},
				{
					kind:  insertAction,
					key:   1,
					child: &sampleLeaves[3],
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
			actions: []nodeAction[string]{
				{
					kind:  insertAction,
					key:   1,
					child: &sampleLeaves[0],
				},
				{
					kind:  insertAction,
					key:   2,
					child: &sampleLeaves[1],
				},
				{
					kind: removeAction,
					key:  1,
				},
				{
					kind: removeAction,
					key:  2,
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
			actions: []nodeAction[string]{
				{
					kind:  insertAction,
					key:   1,
					child: &sampleLeaves[0],
				},
				{
					kind:  insertAction,
					key:   2,
					child: &sampleLeaves[1],
				},
				{
					kind:  insertAction,
					key:   3,
					child: &sampleLeaves[2],
				},
				{
					kind: removeAction,
					key:  2,
				},
				{
					kind:  insertAction,
					key:   4,
					child: &sampleLeaves[3],
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
			actions: []nodeAction[string]{
				{
					kind:  insertAction,
					key:   3,
					child: &sampleLeaves[0],
				},
				{
					kind:  insertAction,
					key:   2,
					child: &sampleLeaves[1],
				},
				{
					kind:  insertAction,
					key:   1,
					child: &sampleLeaves[2],
				},
				{
					kind: removeAction,
					key:  2,
				},
				{
					kind:  insertAction,
					key:   4,
					child: &sampleLeaves[3],
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
			actions: []nodeAction[string]{
				{
					kind:  insertAction,
					key:   3,
					child: &sampleLeaves[0],
				},
				{
					kind:  insertAction,
					key:   2,
					child: &sampleLeaves[1],
				},
				{
					kind:  insertAction,
					key:   4,
					child: &sampleLeaves[1],
				},
				{
					kind:  insertAction,
					key:   1,
					child: &sampleLeaves[2],
				},
				{
					kind: removeAction,
					key:  2,
				},
				{
					kind: removeAction,
					key:  4,
				},
				{
					kind:  insertAction,
					key:   2,
					child: &sampleLeaves[3],
				},
				{
					kind:  insertAction,
					key:   4,
					child: &sampleLeaves[3],
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
			actions: []nodeAction[string]{
				{
					kind:  insertAction,
					key:   3,
					child: &sampleLeaves[0],
				},
				{
					kind:  insertAction,
					key:   2,
					child: &sampleLeaves[1],
				},
				{
					kind: removeAction,
					key:  2,
				},
				{
					kind:  insertAction,
					key:   4,
					child: &sampleLeaves[3],
				},
				{
					kind:  insertAction,
					key:   1,
					child: &sampleLeaves[2],
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
			n256 := newNode[string](KindNode256)
			ctx := context.Background()
			// perform actions
			for _, action := range tc.actions {
				if action.kind == insertAction {
					err := n256.addChild(ctx, action.key, action.child)
					assert.NoError(t, err)
				} else {
					err := n256.removeChild(ctx, action.key)
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
