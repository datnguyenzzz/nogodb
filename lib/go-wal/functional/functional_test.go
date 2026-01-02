//go:build functional_tests

package functional

import (
	"context"
	"math/rand"
	"testing"

	go_fs "github.com/datnguyenzzz/nogodb/lib/go-fs"
	go_wal "github.com/datnguyenzzz/nogodb/lib/go-wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type WalSuite struct {
	suite.Suite
}

func (w *WalSuite) Test_ReadAfterWrite_Small_tests() {
	wal := go_wal.New(
		go_wal.WithLocation(go_fs.InMemory),
		go_wal.WithPageSize(2*32*1024), // = 2 blocks
	)
	err := wal.Open(context.Background())
	require.Nil(w.T(), err)

	totalTestCases := 20
	minCap := 1
	dataCap := 1024

	data := make([][]byte, totalTestCases)
	pos := make([]*go_wal.Position, totalTestCases)

	ctx := context.Background()
	// Do Write
	for i := 0; i < totalTestCases; i++ {
		d := generateBytes(minCap + rand.Intn(dataCap))
		//w.T().Logf("Test_ReadAfterWrite_Small_tests: Write data %v-th, len = %v", i, len(d))
		data[i] = d
		p, err := wal.Write(ctx, d)
		assert.NoError(w.T(), err, "should be able to write data")
		pos[i] = p
	}
	// Test Read
	for i := 0; i < totalTestCases; i++ {
		d, err := wal.Get(ctx, pos[i])
		assert.NoError(w.T(), err, "should be able to read data")
		assert.Equal(w.T(), data[i], d, "data must match")
	}
	// Test Iterator
	reader := wal.NewIterator(ctx)
	for ix := 0; ix < totalTestCases; ix++ {
		d, p, err := reader.Next(ctx)
		assert.NoError(w.T(), err, "should be able to iterate next block")
		assert.Equal(w.T(), pos[ix], p, "position must match")
		assert.Equal(w.T(), data[ix], d, "data must match")
	}

	err = wal.Close(context.Background())
	require.NoError(w.T(), err, "should be able to close wal")
}

func (w *WalSuite) Test_ReadAfterWrite_Medium_tests() {
	wal := go_wal.New(
		go_wal.WithLocation(go_fs.InMemory),
		go_wal.WithPageSize(2*32*1024), // = 2 blocks
	)
	err := wal.Open(context.Background())
	require.Nil(w.T(), err)

	totalTestCases := 10
	minCap := 2 * 1024
	dataCap := 20 * 1024

	data := make([][]byte, totalTestCases)
	pos := make([]*go_wal.Position, totalTestCases)

	ctx := context.Background()
	for i := 0; i < totalTestCases; i++ {
		d := generateBytes(minCap + rand.Intn(dataCap))
		//w.T().Logf("Test_ReadAfterWrite_Medium_tests: Write data %v-th, len = %v", i, len(d))
		data[i] = d
		p, err := wal.Write(ctx, d)
		assert.NoError(w.T(), err, "should be able to write data")
		pos[i] = p
	}
	for i := 0; i < totalTestCases; i++ {
		d, err := wal.Get(ctx, pos[i])
		assert.NoError(w.T(), err, "should be able to read data")
		assert.Equal(w.T(), data[i], d, "data must match")
	}
	// Test Iterator
	reader := wal.NewIterator(ctx)
	for ix := 0; ix < totalTestCases; ix++ {
		d, p, err := reader.Next(ctx)
		assert.NoError(w.T(), err, "should be able to iterate next block")
		assert.Equal(w.T(), pos[ix], p, "position must match")
		assert.Equal(w.T(), data[ix], d, "data must match")
	}

	err = wal.Close(context.Background())
	require.NoError(w.T(), err, "should be able to close wal")
}

func (w *WalSuite) Test_ReadAfterWrite_Big_tests() {
	wal := go_wal.New(
		go_wal.WithLocation(go_fs.InMemory),
		go_wal.WithPageSize(2*1024*1024), // = 2 blocks
	)
	err := wal.Open(context.Background())
	require.Nil(w.T(), err)

	totalTestCases := 100
	minCap := 200 * 1024
	dataCap := 1024 * 1024

	data := make([][]byte, totalTestCases)
	pos := make([]*go_wal.Position, totalTestCases)

	ctx := context.Background()
	for i := 0; i < totalTestCases; i++ {
		d := generateBytes(minCap + rand.Intn(dataCap))
		//w.T().Logf("Test_ReadAfterWrite_Big_tests: Write data %v-th, len = %v", i, len(d))
		data[i] = d
		p, err := wal.Write(ctx, d)
		assert.NoError(w.T(), err, "should be able to write data")
		pos[i] = p
	}
	for i := 0; i < totalTestCases; i++ {
		d, err := wal.Get(ctx, pos[i])
		assert.NoError(w.T(), err, "should be able to read data")
		assert.Equal(w.T(), data[i], d, "data must match")
	}
	// Test Iterator
	reader := wal.NewIterator(ctx)
	for ix := 0; ix < totalTestCases; ix++ {
		d, p, err := reader.Next(ctx)
		assert.NoError(w.T(), err, "should be able to iterate next block")
		assert.Equal(w.T(), pos[ix], p, "position must match")
		assert.Equal(w.T(), data[ix], d, "data must match")
	}

	err = wal.Close(context.Background())
	require.NoError(w.T(), err, "should be able to close wal")
}

func TestWalSuite(t *testing.T) {
	suite.Run(t, new(WalSuite))
}
