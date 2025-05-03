package internal

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_insertAndRemoveChildren_str(t *testing.T) {
	type param struct {
		desc                 string
		actions              []nodeAction[string]
		expectedKeys         [Node4KeysMax]byte
		expectedChildren     [Node4PointersLen]*INode[string]
		expectedChildrenLen  uint8
		expectedAscChildren  []*INode[string]
		expectedDescChildren []*INode[string]
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
			expectedKeys: [Node4KeysMax]byte{0, 1, 2, 3},
			expectedChildren: [Node4PointersLen]*INode[string]{
				nil,
				&sampleLeaves[0],
				&sampleLeaves[1],
				&sampleLeaves[2],
			},
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
			expectedKeys: [Node4KeysMax]byte{0, 0, 0, 1},
			expectedChildren: [Node4PointersLen]*INode[string]{
				nil,
				nil,
				nil,
				&sampleLeaves[3],
			},
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
			expectedKeys: [Node4KeysMax]byte{0, 0, 0, 0},
			expectedChildren: [Node4PointersLen]*INode[string]{
				nil,
				nil,
				nil,
				nil,
			},
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
			expectedKeys: [Node4KeysMax]byte{0, 1, 3, 4},
			expectedChildren: [Node4PointersLen]*INode[string]{
				nil,
				&sampleLeaves[0],
				&sampleLeaves[2],
				&sampleLeaves[3],
			},
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
			expectedKeys: [Node4KeysMax]byte{0, 1, 3, 4},
			expectedChildren: [Node4PointersLen]*INode[string]{
				nil,
				&sampleLeaves[2],
				&sampleLeaves[0],
				&sampleLeaves[3],
			},
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
			expectedKeys: [Node4KeysMax]byte{1, 2, 3, 4},
			expectedChildren: [Node4PointersLen]*INode[string]{
				&sampleLeaves[2],
				&sampleLeaves[3],
				&sampleLeaves[0],
				&sampleLeaves[3],
			},
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
			expectedKeys: [Node4KeysMax]byte{0, 1, 3, 4},
			expectedChildren: [Node4PointersLen]*INode[string]{
				nil,
				&sampleLeaves[2],
				&sampleLeaves[0],
				&sampleLeaves[3],
			},
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
			n4 := newNode[string](KindNode4)
			ctx := context.Background()
			// perform actions
			for _, action := range tc.actions {
				if action.kind == insertAction {
					err := n4.addChild(ctx, action.key, action.child)
					assert.NoError(t, err)
				} else {
					err := n4.removeChild(ctx, action.key)
					assert.NoError(t, err)
				}
			}
			// verify output
			n4o, ok := n4.(*Node4[string])
			assert.True(t, ok, "can not cast to Node4[string]")
			assert.Equal(t, tc.expectedKeys, n4o.keys, "node keys is different")
			assert.Equal(t, tc.expectedChildren, n4o.children, "node children is different")
			assert.Equal(t, tc.expectedChildrenLen, n4o.getChildrenLen(ctx), "node children length is different")
			assert.Equal(t, tc.expectedAscChildren, n4o.getAllChildren(ctx, AscOrder), "node children in ASC is different")
			assert.Equal(t, tc.expectedDescChildren, n4o.getAllChildren(ctx, DescOrder), "node children in DESC is different")
			for k, expectedChild := range tc.expectedGetChild {
				child, err := n4o.getChild(ctx, k)
				assert.NoError(t, err)
				assert.Equal(t, expectedChild, child)
			}
		})
	}
}
