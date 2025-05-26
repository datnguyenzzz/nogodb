//go:build functional_tests

package functional

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	go_wal "github.com/datnguyenzzz/nogodb/lib/go-wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type WalSuite struct {
	suite.Suite
	wal *go_wal.WAL
}

func (w *WalSuite) Test_ReadAfterWrite_Small_tests() {
	totalTestCases := 20
	minCap := 0
	dataCap := 1024

	data := make([][]byte, totalTestCases)
	pos := make([]*go_wal.Position, totalTestCases)

	ctx := context.Background()
	for i := 0; i < totalTestCases; i++ {
		d := generateBytes(minCap + rand.Intn(dataCap))
		w.T().Logf("Test_ReadAfterWrite_Small_tests: Write data %v-th, len = %v", i, len(d))
		data[i] = d
		p, err := w.wal.Write(ctx, d)
		assert.NoError(w.T(), err, "should be able to write data")
		pos[i] = p
	}
	for i := 0; i < totalTestCases; i++ {
		d, err := w.wal.Read(ctx, pos[i])
		assert.NoError(w.T(), err, "should be able to read data")
		assert.Equal(w.T(), data[i], d, "data must match")
	}
}

func (w *WalSuite) Test_ReadAfterWrite_Medium_tests() {
	totalTestCases := 10
	minCap := 2 * 1024
	dataCap := 20 * 1024

	data := make([][]byte, totalTestCases)
	pos := make([]*go_wal.Position, totalTestCases)

	ctx := context.Background()
	for i := 0; i < totalTestCases; i++ {
		d := generateBytes(minCap + rand.Intn(dataCap))
		w.T().Logf("Test_ReadAfterWrite_Medium_tests: Write data %v-th, len = %v", i, len(d))
		data[i] = d
		p, err := w.wal.Write(ctx, d)
		assert.NoError(w.T(), err, "should be able to write data")
		pos[i] = p
	}
	for i := 0; i < totalTestCases; i++ {
		d, err := w.wal.Read(ctx, pos[i])
		assert.NoError(w.T(), err, "should be able to read data")
		assert.Equal(w.T(), data[i], d, "data must match")
	}
}

func (w *WalSuite) Test_ReadAfterWrite_Big_tests() {
	totalTestCases := 5
	minCap := 20 * 1024
	dataCap := 40 * 1024

	data := make([][]byte, totalTestCases)
	pos := make([]*go_wal.Position, totalTestCases)

	ctx := context.Background()
	for i := 0; i < totalTestCases; i++ {
		d := generateBytes(minCap + rand.Intn(dataCap))
		w.T().Logf("Test_ReadAfterWrite_Big_tests: Write data %v-th, len = %v", i, len(d))
		data[i] = d
		p, err := w.wal.Write(ctx, d)
		assert.NoError(w.T(), err, "should be able to write data")
		pos[i] = p
	}
	for i := 0; i < totalTestCases; i++ {
		d, err := w.wal.Read(ctx, pos[i])
		assert.NoError(w.T(), err, "should be able to read data")
		assert.Equal(w.T(), data[i], d, "data must match")
	}
}

func (w *WalSuite) SetupSuite() {
	w.T().Logf("SetupSuite")
	w.wal = go_wal.New(
		go_wal.WithDirPath(CommonDirPath),
		go_wal.WithPageSize(4*32*1024), // = 4 blocks
	)
	_ = w.wal.Open(context.Background())
}

func (w *WalSuite) TearDownSuite() {
	w.T().Logf("TearDownSuite")

	_ = w.wal.Close(context.Background())

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

func TestWalSuite(t *testing.T) {
	suite.Run(t, new(WalSuite))
}
