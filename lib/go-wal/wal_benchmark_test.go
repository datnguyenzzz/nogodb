package go_wal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

const (
	CommonDirPath = "./wal"
)

func BenchmarkWrite(b *testing.B) {
	ctx := context.Background()
	wal := New(
		WithDirPath(CommonDirPath),
	)
	_ = wal.Open(context.Background())

	fileSizes := []int{1 * 1024, 4 * 1024, 512 * 1024, 1024 * 1024}
	batchSizes := []int{1_000_000, 500_000, 10_000, 5_000}

	for i := 0; i < len(fileSizes); i++ {
		batchSize := batchSizes[i]
		fileSize := fileSizes[i]

		b.Run(fmt.Sprintf("counts=%v,size=%vkB", batchSize, fileSize/1024), func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				for t := 0; t < batchSize; t++ {
					_, _ = wal.Write(ctx, generateBytes(fileSize))
				}
			}
		})
	}

	_ = wal.Close(context.Background())

	// Remove all page data files
	files, _ := os.ReadDir(CommonDirPath)
	for _, file := range files {
		if !file.IsDir() {
			filePath := filepath.Join(CommonDirPath, file.Name())
			fmt.Printf("remove file %s", filePath)
			_ = os.Remove(filePath)
		}
	}
}
