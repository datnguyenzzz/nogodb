package go_adaptive_radix_tree

import (
	"context"
	"fmt"
	"testing"

	"github.com/datnguyenzzz/nogodb/lib/go-adaptive-radix-tree/internal"
	"github.com/stretchr/testify/assert"
)

func Test_art_str_InsertAndRemoveNode_sync(t *testing.T) {
	type param struct {
		desc             string
		actions          []internal.TreeAction[string]
		expectedPreorder []internal.INode[string]
	}

	var dicts []string
	for i := 0; i < 20; i++ {
		dicts = append(dicts, internal.RandomQuote())
	}

	testList := []param{
		{
			desc: "Happy case #1 - Insert exactly 1 key",
			actions: []internal.TreeAction[string]{
				{
					Kind:  internal.InsertAction,
					Key:   []byte("Hello Go Lovers!!"),
					Value: dicts[0],
				},
			},
			expectedPreorder: []internal.INode[string]{
				internal.NewLeafWithKV[string](
					context.Background(),
					[]byte("Hello Go Lovers!!"),
					dicts[0],
				),
			},
		},
		{
			desc: "Happy case #2 - Insert a key and remove it",
			actions: []internal.TreeAction[string]{
				{
					Kind:  internal.InsertAction,
					Key:   []byte("Hello Go Lovers!!"),
					Value: dicts[0],
				},
				{
					Kind: internal.RemoveAction,
					Key:  []byte("Hello Go Lovers!!"),
				},
			},
			expectedPreorder: []internal.INode[string]{nil},
		},
		{
			desc: "Happy case #3 - Insert only",
			actions: []internal.TreeAction[string]{
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/fileA"),
					Value: dicts[0],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir3/fileA"),
					Value: dicts[1],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir2/fileA"),
					Value: dicts[2],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir1/fileA"),
					Value: dicts[6],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir1/fileB"),
					Value: dicts[7],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir2/dir3/fileA"),
					Value: dicts[3],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir2/dir3/fileB"),
					Value: dicts[4],
				},
			},
			expectedPreorder: []internal.INode[string]{
				internal.SeedNode4[string](context.Background(), []byte("root/")),
				internal.SeedNode4[string](context.Background(), []byte("ir")),
				internal.SeedNode4[string](context.Background(), []byte("/file")),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/dir1/fileA"), dicts[6]),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/dir1/fileB"), dicts[7]),
				internal.SeedNode4[string](context.Background(), []byte("/")),
				internal.SeedNode4[string](context.Background(), []byte("ir3/file")),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/dir2/dir3/fileA"), dicts[3]),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/dir2/dir3/fileB"), dicts[4]),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/dir2/fileA"), dicts[2]),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/dir3/fileA"), dicts[1]),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/fileA"), dicts[0]),
			},
		},
		{
			desc: "Happy case #4 - Insert only same as case #3 but with the shuffled the action orders, however the output should be remain",
			actions: []internal.TreeAction[string]{
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir1/fileB"),
					Value: dicts[7],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir3/fileA"),
					Value: dicts[1],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/fileA"),
					Value: dicts[0],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir2/fileA"),
					Value: dicts[2],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir2/dir3/fileB"),
					Value: dicts[4],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir2/dir3/fileA"),
					Value: dicts[3],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir1/fileA"),
					Value: dicts[6],
				},
			},
			expectedPreorder: []internal.INode[string]{
				internal.SeedNode4[string](context.Background(), []byte("root/")),
				internal.SeedNode4[string](context.Background(), []byte("ir")),
				internal.SeedNode4[string](context.Background(), []byte("/file")),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/dir1/fileA"), dicts[6]),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/dir1/fileB"), dicts[7]),
				internal.SeedNode4[string](context.Background(), []byte("/")),
				internal.SeedNode4[string](context.Background(), []byte("ir3/file")),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/dir2/dir3/fileA"), dicts[3]),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/dir2/dir3/fileB"), dicts[4]),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/dir2/fileA"), dicts[2]),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/dir3/fileA"), dicts[1]),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/fileA"), dicts[0]),
			},
		},
		{
			desc: "Happy case #5 - Insert only same as case #4 but with the shuffled the action orders, however the output should be remain",
			actions: []internal.TreeAction[string]{
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir2/dir3/fileA"),
					Value: dicts[3],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir1/fileA"),
					Value: dicts[6],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir1/fileB"),
					Value: dicts[7],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir3/fileA"),
					Value: dicts[1],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/fileA"),
					Value: dicts[0],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir2/dir3/fileB"),
					Value: dicts[4],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir2/fileA"),
					Value: dicts[2],
				},
			},
			expectedPreorder: []internal.INode[string]{
				internal.SeedNode4[string](context.Background(), []byte("root/")),
				internal.SeedNode4[string](context.Background(), []byte("ir")),
				internal.SeedNode4[string](context.Background(), []byte("/file")),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/dir1/fileA"), dicts[6]),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/dir1/fileB"), dicts[7]),
				internal.SeedNode4[string](context.Background(), []byte("/")),
				internal.SeedNode4[string](context.Background(), []byte("ir3/file")),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/dir2/dir3/fileA"), dicts[3]),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/dir2/dir3/fileB"), dicts[4]),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/dir2/fileA"), dicts[2]),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/dir3/fileA"), dicts[1]),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/fileA"), dicts[0]),
			},
		},
		{
			desc: "Happy case #6 - Insert and Delete",
			actions: []internal.TreeAction[string]{
				// insert and remove
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/fileC"),
					Value: dicts[0],
				},
				{
					Kind: internal.RemoveAction,
					Key:  []byte("root/fileC"),
				},
				//
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/fileA"),
					Value: dicts[0],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir3/fileA"),
					Value: dicts[1],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir2/fileA"),
					Value: dicts[2],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir1/fileA"),
					Value: dicts[0],
				},
				{
					Kind: internal.RemoveAction,
					Key:  []byte("root/dir1/fileA"),
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir1/fileB"),
					Value: dicts[7],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir2/fileB"),
					Value: dicts[11],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir1/fileC"),
					Value: dicts[10],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir1/fileA"),
					Value: dicts[6],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir2/dir3/fileA"),
					Value: dicts[3],
				},
				{
					Kind: internal.RemoveAction,
					Key:  []byte("root/dir2/fileB"),
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir2/dir3/fileB"),
					Value: dicts[4],
				},
				{
					Kind: internal.RemoveAction,
					Key:  []byte("root/dir1/fileC"),
				},
			},
			expectedPreorder: []internal.INode[string]{
				internal.SeedNode4[string](context.Background(), []byte("root/")),
				internal.SeedNode4[string](context.Background(), []byte("ir")),
				internal.SeedNode4[string](context.Background(), []byte("/file")),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/dir1/fileA"), dicts[6]),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/dir1/fileB"), dicts[7]),
				internal.SeedNode4[string](context.Background(), []byte("/")),
				internal.SeedNode4[string](context.Background(), []byte("ir3/file")),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/dir2/dir3/fileA"), dicts[3]),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/dir2/dir3/fileB"), dicts[4]),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/dir2/fileA"), dicts[2]),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/dir3/fileA"), dicts[1]),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/fileA"), dicts[0]),
			},
		},
		{
			desc: "Happy case #7 - Insert and Delete, same as #6 but with shuffled the action orders, however the output should be remain",
			actions: []internal.TreeAction[string]{
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir2/dir3/fileB"),
					Value: dicts[4],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/fileC"),
					Value: dicts[0],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/fileA"),
					Value: dicts[0],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir3/fileA"),
					Value: dicts[1],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir2/fileA"),
					Value: dicts[2],
				},
				{
					Kind: internal.RemoveAction,
					Key:  []byte("root/fileC"),
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir2/fileB"),
					Value: dicts[11],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir1/fileA"),
					Value: dicts[0],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir1/fileC"),
					Value: dicts[10],
				},
				{
					Kind: internal.RemoveAction,
					Key:  []byte("root/dir1/fileA"),
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir1/fileB"),
					Value: dicts[7],
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir1/fileA"),
					Value: dicts[6],
				},
				{
					Kind: internal.RemoveAction,
					Key:  []byte("root/dir2/fileB"),
				},
				{
					Kind:  internal.InsertAction,
					Key:   []byte("root/dir2/dir3/fileA"),
					Value: dicts[3],
				},
				{
					Kind: internal.RemoveAction,
					Key:  []byte("root/dir1/fileC"),
				},
			},
			expectedPreorder: []internal.INode[string]{
				internal.SeedNode4[string](context.Background(), []byte("root/")),
				internal.SeedNode4[string](context.Background(), []byte("ir")),
				internal.SeedNode4[string](context.Background(), []byte("/file")),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/dir1/fileA"), dicts[6]),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/dir1/fileB"), dicts[7]),
				internal.SeedNode4[string](context.Background(), []byte("/")),
				internal.SeedNode4[string](context.Background(), []byte("ir3/file")),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/dir2/dir3/fileA"), dicts[3]),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/dir2/dir3/fileB"), dicts[4]),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/dir2/fileA"), dicts[2]),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/dir3/fileA"), dicts[1]),
				internal.SeedNodeLeaf[string](context.Background(), []byte("root/fileA"), dicts[0]),
			},
		},
	}

	for _, tc := range testList {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := context.Background()
			art := NewTree[string](ctx)

			for idx, action := range tc.actions {
				if action.Kind == internal.InsertAction {
					_, err := art.Insert(ctx, action.Key, action.Value)
					assert.NoError(t, err, fmt.Sprintf("shouldn't fail to insert new key, at action: %v-th", idx))
				} else {
					_, err := art.Delete(ctx, action.Key)
					assert.NoError(t, err, fmt.Sprintf("shouldn't fail to delete new key, at action: %v-th", idx))
				}
			}

			//verify the pre-ordered nodes in the tree
			internal.PreorderTraverseAndValidate[string](
				t, ctx, art.root,
				tc.expectedPreorder, 0, 0,
			)
		})
	}
}
