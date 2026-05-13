//go:build functional_tests

package functional

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	go_fs "github.com/datnguyenzzz/nogodb/lib/go-fs"
	go_wal "github.com/datnguyenzzz/nogodb/lib/go-wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type WalFSSuite struct {
	suite.Suite
	wal *go_wal.WAL
}

func (w *WalFSSuite) SetupTest() {
	w.wal = go_wal.New(
		go_wal.WithLocation(go_fs.FileSystem),
		go_wal.WithDirPath(CommonDirPath),
		go_wal.WithPageSize(2*1024*1024), // = 2 blocks
		go_wal.WithBytesPerSync(512*1024),
		go_wal.WithSync(true),
	)
	err := w.wal.Open(context.Background())
	require.Nil(w.T(), err)
}

func (w *WalFSSuite) TearDownTest() {
	err := w.wal.Close(context.Background())
	require.Nil(w.T(), err)

	// Remove all page data files
	files, _ := os.ReadDir(CommonDirPath)
	for _, file := range files {
		if !file.IsDir() {
			filePath := filepath.Join(CommonDirPath, file.Name())
			_ = os.Remove(filePath)
		}
	}

	_ = os.Remove(CommonDirPath)
}

func (w *WalFSSuite) Test_ReadAfterWrite_Small_tests() {
	totalTestCases := 20
	minCap := 1
	dataCap := 1024

	data := make([][]byte, totalTestCases)
	pos := make([]*go_wal.Position, totalTestCases)

	ctx := context.Background()
	// Do Write
	for i := range totalTestCases {
		d := generateBytes(minCap + rand.Intn(dataCap))
		// w.T().Logf("Test_ReadAfterWrite_Small_tests: Write data %v-th, len = %v", i, len(d))
		data[i] = d
		p, err := w.wal.Write(ctx, d)
		assert.NoError(w.T(), err, "should be able to write data")
		pos[i] = p
	}
	err := w.wal.Sync(ctx)
	require.Nil(w.T(), err)
	// Test Read
	for i := range totalTestCases {
		d, err := w.wal.Get(ctx, pos[i])
		assert.NoError(w.T(), err, "should be able to read data")
		assert.Equal(w.T(), data[i], d, "data must match")
	}
	// Test Iterator
	reader := w.wal.NewIterator(ctx)
	for ix := range totalTestCases {
		d, p, err := reader.Next(ctx)
		assert.NoError(w.T(), err, "should be able to iterate next block")
		assert.Equal(w.T(), pos[ix], p, "position must match")
		assert.Equal(w.T(), data[ix], d, "data must match")
	}

	err = w.wal.Close(context.Background())
	require.NoError(w.T(), err, "should be able to close wal")
}

func (w *WalFSSuite) Test_ReadAfterWrite_Medium_tests() {
	totalTestCases := 10
	minCap := 2 * 1024
	dataCap := 20 * 1024

	data := make([][]byte, totalTestCases)
	pos := make([]*go_wal.Position, totalTestCases)

	ctx := context.Background()
	for i := range totalTestCases {
		d := generateBytes(minCap + rand.Intn(dataCap))
		// w.T().Logf("Test_ReadAfterWrite_Medium_tests: Write data %v-th, len = %v", i, len(d))
		data[i] = d
		p, err := w.wal.Write(ctx, d)
		assert.NoError(w.T(), err, "should be able to write data")
		pos[i] = p
	}
	err := w.wal.Sync(ctx)
	require.Nil(w.T(), err)
	for i := range totalTestCases {
		d, err := w.wal.Get(ctx, pos[i])
		assert.NoError(w.T(), err, "should be able to read data")
		assert.Equal(w.T(), data[i], d, "data must match")
	}
	// Test Iterator
	reader := w.wal.NewIterator(ctx)
	for ix := range totalTestCases {
		d, p, err := reader.Next(ctx)
		assert.NoError(w.T(), err, "should be able to iterate next block")
		assert.Equal(w.T(), pos[ix], p, "position must match")
		assert.Equal(w.T(), data[ix], d, "data must match")
	}

	err = w.wal.Close(context.Background())
	require.NoError(w.T(), err, "should be able to close wal")
}

func (w *WalFSSuite) Test_ReadAfterWrite_Big_tests() {
	totalTestCases := 100
	minCap := 200 * 1024
	dataCap := 1024 * 1024

	data := make([][]byte, totalTestCases)
	pos := make([]*go_wal.Position, totalTestCases)

	ctx := context.Background()
	for i := range totalTestCases {
		d := generateBytes(minCap + rand.Intn(dataCap))
		// w.T().Logf("Test_ReadAfterWrite_Big_tests: Write data %v-th, len = %v", i, len(d))
		data[i] = d
		p, err := w.wal.Write(ctx, d)
		assert.NoError(w.T(), err, "should be able to write data")
		pos[i] = p
	}
	err := w.wal.Sync(ctx)
	require.Nil(w.T(), err)
	for i := range totalTestCases {
		d, err := w.wal.Get(ctx, pos[i])
		assert.NoError(w.T(), err, "should be able to read data")
		assert.Equal(w.T(), data[i], d, "data must match")
	}
	// Test Iterator
	reader := w.wal.NewIterator(ctx)
	for ix := range totalTestCases {
		d, p, err := reader.Next(ctx)
		assert.NoError(w.T(), err, "should be able to iterate next block")
		assert.Equal(w.T(), pos[ix], p, "position must match")
		assert.Equal(w.T(), data[ix], d, "data must match")
	}

	err = w.wal.Close(context.Background())
	require.NoError(w.T(), err, "should be able to close wal")
}

func TestWalFSSuite(t *testing.T) {
	suite.Run(t, new(WalFSSuite))
}
