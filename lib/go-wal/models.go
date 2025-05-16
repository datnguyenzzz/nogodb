package go_wal

import (
	"os"
	"sync"
	"time"
)

type PageID uint32

type syncCfg struct {
	closeCh chan struct{}
	ticker  *time.Ticker
}

type PageAccessMode int8

const (
	PageAccessModeReadOnly PageAccessMode = iota
	PageAccessModeReadWrite
	PageAccessModeReadWriteSync
)

// WAL represents a Write-Ahead Log structure that provides durability and fault-tolerance for incoming writes.
// It consists of an activePage, which is the current segment file used for new incoming writes,
// and olderPages, which is a map of segment files used for read operations.
type WAL struct {
	syncCfg
	opts       options
	activePage *Page            // active page, used for writing
	olderPages map[PageID]*Page // older pages, only used for read.
	mu         sync.RWMutex
}

// Page represents a single log file in WAL. A Page file consists of a sequence of variable length Record.
type Page struct {
	Id              PageID
	F               *os.File
	TotalBlockCount uint32
	LastBlockSize   uint32
}

// Record represents the position of a record in a log file.
type Record struct {
	// PageId represents the ID of the log file where the record is located.
	PageId PageID
	// BlockNumber indicate which block where the record is located
	BlockNumber uint32
	// Offset indicate the starting offset of the record in the log file.
	Offset uint64
	// 	Size How many bytes the record data takes up in the segment file.
	Size uint32
}
