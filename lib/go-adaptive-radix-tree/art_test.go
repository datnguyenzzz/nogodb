package go_adaptive_radix_tree

import (
	"context"
	"fmt"
	"testing"

	"github.com/datnguyenzzz/nogodb/lib/go-adaptive-radix-tree/internal"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

func Test_preorder_art_str_InsertAndRemoveNode_sync(t *testing.T) {
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
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("Hello Go Lovers!!"),
						Value: dicts[0],
					},
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
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("Hello Go Lovers!!"),
						Value: dicts[0],
					},
				},
				{
					Kind: internal.RemoveAction,
					KV: internal.KV[string]{
						Key: []byte("Hello Go Lovers!!"),
					},
				},
			},
			expectedPreorder: []internal.INode[string]{nil},
		},
		{
			desc: "Happy case #3 - Insert only",
			actions: []internal.TreeAction[string]{
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/fileA"),
						Value: dicts[0],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir3/fileA"),
						Value: dicts[1],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir2/fileA"),
						Value: dicts[2],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir1/fileA"),
						Value: dicts[6],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir1/fileB"),
						Value: dicts[7],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir2/dir3/fileA"),
						Value: dicts[3],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir2/dir3/fileB"),
						Value: dicts[4],
					},
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
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir1/fileB"),
						Value: dicts[7],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir3/fileA"),
						Value: dicts[1],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/fileA"),
						Value: dicts[0],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir2/fileA"),
						Value: dicts[2],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir2/dir3/fileB"),
						Value: dicts[4],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir2/dir3/fileA"),
						Value: dicts[3],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir1/fileA"),
						Value: dicts[6],
					},
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
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir2/dir3/fileA"),
						Value: dicts[3],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir1/fileA"),
						Value: dicts[6],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir1/fileB"),
						Value: dicts[7],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir3/fileA"),
						Value: dicts[1],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/fileA"),
						Value: dicts[0],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir2/dir3/fileB"),
						Value: dicts[4],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir2/fileA"),
						Value: dicts[2],
					},
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
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/fileC"),
						Value: dicts[0],
					},
				},
				{
					Kind: internal.RemoveAction,
					KV: internal.KV[string]{
						Key: []byte("root/fileC"),
					},
				},
				//
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/fileA"),
						Value: dicts[0],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir3/fileA"),
						Value: dicts[1],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir2/fileA"),
						Value: dicts[2],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir1/fileA"),
						Value: dicts[0],
					},
				},
				{
					Kind: internal.RemoveAction,
					KV: internal.KV[string]{
						Key: []byte("root/dir1/fileA"),
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir1/fileB"),
						Value: dicts[7],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir2/fileB"),
						Value: dicts[11],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir1/fileC"),
						Value: dicts[10],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir1/fileA"),
						Value: dicts[6],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir2/dir3/fileA"),
						Value: dicts[3],
					},
				},
				{
					Kind: internal.RemoveAction,
					KV: internal.KV[string]{
						Key: []byte("root/dir2/fileB"),
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir2/dir3/fileB"),
						Value: dicts[4],
					},
				},
				{
					Kind: internal.RemoveAction,
					KV: internal.KV[string]{
						Key: []byte("root/dir1/fileC"),
					},
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
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir2/dir3/fileB"),
						Value: dicts[4],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/fileC"),
						Value: dicts[0],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/fileA"),
						Value: dicts[0],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir3/fileA"),
						Value: dicts[1],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir2/fileA"),
						Value: dicts[2],
					},
				},
				{
					Kind: internal.RemoveAction,
					KV: internal.KV[string]{
						Key: []byte("root/fileC"),
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir2/fileB"),
						Value: dicts[11],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir1/fileA"),
						Value: dicts[0],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir1/fileC"),
						Value: dicts[10],
					},
				},
				{
					Kind: internal.RemoveAction,
					KV: internal.KV[string]{
						Key: []byte("root/dir1/fileA"),
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir1/fileB"),
						Value: dicts[7],
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir1/fileA"),
						Value: dicts[6],
					},
				},
				{
					Kind: internal.RemoveAction,
					KV: internal.KV[string]{
						Key: []byte("root/dir2/fileB"),
					},
				},
				{
					Kind: internal.InsertAction,
					KV: internal.KV[string]{
						Key:   []byte("root/dir2/dir3/fileA"),
						Value: dicts[3],
					},
				},
				{
					Kind: internal.RemoveAction,
					KV: internal.KV[string]{
						Key: []byte("root/dir1/fileC"),
					},
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

func Test_search_art_str_InsertAndRemoveNode_async(t *testing.T) {
	type param struct {
		desc string
		size int
		// from insertActionsCount - len(expectedFinalKVMap) -> len(expectedFinalKVMap) deletion actions
		deleteActionsCount int
		// expectedTotalKVMap has size of "size"
		expectedTotalKVMap []internal.KV[string]
	}

	testList := []param{
		{
			desc:               "small-size test p1, 10 totals, 4 removes",
			size:               10,
			deleteActionsCount: 4,
			expectedTotalKVMap: internal.SeedMapKVString(10),
		},
		{
			desc:               "small-size test p2, 20 totals, 6 removes",
			size:               20,
			deleteActionsCount: 6,
			expectedTotalKVMap: internal.SeedMapKVString(20),
		},
		{
			desc:               "small-size test p3, 50 totals, 20 removes",
			size:               50,
			deleteActionsCount: 20,
			expectedTotalKVMap: internal.SeedMapKVString(50),
		},
		{
			desc:               "medium-size test p1, 100 totals, 40 removes",
			size:               100,
			deleteActionsCount: 40,
			expectedTotalKVMap: internal.SeedMapKVString(100),
		},
		{
			desc:               "medium-size test p2, 1000 totals, 400 removes",
			size:               1000,
			deleteActionsCount: 400,
			expectedTotalKVMap: internal.SeedMapKVString(1000),
		},
		{
			desc:               "medium-size test p3, 5000 totals, 1500 removes",
			size:               5000,
			deleteActionsCount: 1500,
			expectedTotalKVMap: internal.SeedMapKVString(5000),
		},
		{
			desc:               "large-size test p1, 10000 totals, 4000 removes",
			size:               10000,
			deleteActionsCount: 4000,
			expectedTotalKVMap: internal.SeedMapKVString(10000),
		},
		{
			desc:               "large-size test p2, 100_000 totals, 40_000 removes",
			size:               100_000,
			deleteActionsCount: 40_000,
			expectedTotalKVMap: internal.SeedMapKVString(100_000),
		},
		{
			desc:               "large-size test p3, 250_000 totals, 50_000 removes",
			size:               250_000,
			deleteActionsCount: 50_000,
			expectedTotalKVMap: internal.SeedMapKVString(250_000),
		},
	}

	for _, tc := range testList {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := context.Background()
			art := NewTree[string](ctx)

			// adding all key into the tree
			var eg errgroup.Group
			for _, kv := range tc.expectedTotalKVMap {
				eg.Go(func() error {
					_, err := art.Insert(ctx, kv.Key, kv.Value)
					return err
				})
			}

			err := eg.Wait()
			assert.NoError(t, err, fmt.Sprintf("shouldn't fail to insert new key. Err: %v", err))

			// verify key value
			for i := 0; i < len(tc.expectedTotalKVMap); i++ {
				kv := tc.expectedTotalKVMap[i]
				actualV, err := art.Get(ctx, kv.Key)
				assert.NoError(t, err, fmt.Sprintf("shouldn't fail to get key. Err: %v", err))
				assert.Equal(t, kv.Value, actualV, "value should be equal")
			}

			// delete the target key
			for i := tc.size - tc.deleteActionsCount; i < tc.size; i++ {
				kv := tc.expectedTotalKVMap[i]
				eg.Go(func() error {
					_, err := art.Delete(ctx, kv.Key)
					return err
				})
			}

			err = eg.Wait()
			assert.NoError(t, err, fmt.Sprintf("shouldn't fail to insert new key. Err: %v", err))

			// verify key value after deletion
			for i := 0; i < tc.size-tc.deleteActionsCount; i++ {
				kv := tc.expectedTotalKVMap[i]
				actualV, err := art.Get(ctx, kv.Key)
				assert.NoError(t, err, fmt.Sprintf("shouldn't fail to get key. Err: %v", err))
				assert.Equal(t, kv.Value, actualV, "value should be equal")
			}
		})
	}
}
