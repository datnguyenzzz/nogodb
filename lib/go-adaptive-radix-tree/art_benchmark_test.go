package go_adaptive_radix_tree

import (
	"context"
	"testing"

	"github.com/datnguyenzzz/nogodb/lib/go-adaptive-radix-tree/internal"
)

func BenchmarkInsert_100000(b *testing.B) {
	kvs := internal.SeedMapKVString(100_000)
	ctx := context.Background()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		art := NewTree[string](ctx)
		for _, kv := range kvs {
			_, _ = art.Insert(ctx, kv.Key, kv.Value)
		}
	}
}

func BenchmarkInsert_250000(b *testing.B) {
	kvs := internal.SeedMapKVString(250_000)
	ctx := context.Background()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		art := NewTree[string](ctx)
		for _, kv := range kvs {
			_, _ = art.Insert(ctx, kv.Key, kv.Value)
		}
	}
}

func BenchmarkInsert_500000(b *testing.B) {
	kvs := internal.SeedMapKVString(500_000)
	ctx := context.Background()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		art := NewTree[string](ctx)
		for _, kv := range kvs {
			_, _ = art.Insert(ctx, kv.Key, kv.Value)
		}
	}
}

func BenchmarkInsert_1000000(b *testing.B) {
	kvs := internal.SeedMapKVString(1_000_000)
	ctx := context.Background()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		art := NewTree[string](ctx)
		for _, kv := range kvs {
			_, _ = art.Insert(ctx, kv.Key, kv.Value)
		}
	}
}

func BenchmarkGet_100000(b *testing.B) {
	kvs := internal.SeedMapKVString(100_000)
	ctx := context.Background()
	art := NewTree[string](ctx)
	for _, kv := range kvs {
		_, _ = art.Insert(ctx, kv.Key, kv.Value)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, kv := range kvs {
			_, _ = art.Get(ctx, kv.Key)
		}
	}
}

func BenchmarkGet_250000(b *testing.B) {
	kvs := internal.SeedMapKVString(250_000)
	ctx := context.Background()
	art := NewTree[string](ctx)
	for _, kv := range kvs {
		_, _ = art.Insert(ctx, kv.Key, kv.Value)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, kv := range kvs {
			_, _ = art.Get(ctx, kv.Key)
		}
	}
}

func BenchmarkGet_500000(b *testing.B) {
	kvs := internal.SeedMapKVString(500_000)
	ctx := context.Background()
	art := NewTree[string](ctx)
	for _, kv := range kvs {
		_, _ = art.Insert(ctx, kv.Key, kv.Value)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, kv := range kvs {
			_, _ = art.Get(ctx, kv.Key)
		}
	}
}

func BenchmarkGet_1000000(b *testing.B) {
	kvs := internal.SeedMapKVString(1_000_000)
	ctx := context.Background()
	art := NewTree[string](ctx)
	for _, kv := range kvs {
		_, _ = art.Insert(ctx, kv.Key, kv.Value)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, kv := range kvs {
			_, _ = art.Get(ctx, kv.Key)
		}
	}
}
