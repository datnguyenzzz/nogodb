package go_adaptive_radix_tree

import (
	"context"
	"testing"

	"github.com/datnguyenzzz/nogodb/lib/go-adaptive-radix-tree/internal"
	"github.com/stretchr/testify/require"
)

func BenchmarkInsert(b *testing.B) {
	kvs := internal.SeedMapKVString(10_000_000)
	ctx := context.Background()

	for b.Loop() {
		art := NewTree[string](ctx)
		for _, kv := range kvs {
			_, err := art.Insert(ctx, kv.Key, kv.Value)
			require.NoError(b, err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	kvs := internal.SeedMapKVString(10_000_000)
	ctx := context.Background()
	art := NewTree[string](ctx)
	for _, kv := range kvs {
		_, _ = art.Insert(ctx, kv.Key, kv.Value)
	}

	for b.Loop() {
		for _, kv := range kvs {
			_, err := art.Get(ctx, kv.Key)
			require.NoError(b, err)
		}
	}
}

func BenchmarkInsertAndGet(b *testing.B) {
	kvs := internal.SeedMapKVString(10_000_000)
	ctx := context.Background()
	art := NewTree[string](ctx)

	for b.Loop() {
		for _, kv := range kvs {
			_, err := art.Insert(ctx, kv.Key, kv.Value)
			require.NoError(b, err)
			_, err = art.Get(ctx, kv.Key)
			require.NoError(b, err)
		}
	}
}
