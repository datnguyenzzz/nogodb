//go:build functional_tests

package functional

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	go_wal "github.com/datnguyenzzz/nogodb/lib/go-wal"
	"github.com/stretchr/testify/suite"
)

type WalSuite struct {
	suite.Suite
	wal *go_wal.WAL
}

func (w *WalSuite) Test_ReadAfterWrite() {
	type param struct {
		testName string
	}

	testCases := []param{
		{
			testName: "#1",
		},
		{
			testName: "#2",
		},
	}

	for _, tc := range testCases {
		w.T().Run(tc.testName, func(t *testing.T) {
			w.setupSubTest(tc.testName)
			t.Cleanup(func() {
				w.tearDownSubTest(tc.testName)
			})

			w.T().Logf("Running test: %s", tc.testName)

			// TODO fill me
			time.Sleep(1 * time.Second)
			w.Require().True(1 == 1)
		})
	}
}

func (w *WalSuite) setupSubTest(testName string) {
	w.T().Logf("Running setupSubTest before test: %s", testName)
	w.wal = go_wal.New(
		go_wal.WithDirPath(CommonDirPath),
		go_wal.WithPageSize(64*1024), // = 2 blocks
	)
	_ = w.wal.Open(context.Background())
}

func (w *WalSuite) tearDownSubTest(testName string) {
	w.T().Logf("Running tearDownSubTest after test: %s", testName)
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
