package go_wal

import (
	"errors"
	"sync"
	"time"

	go_fs "github.com/datnguyenzzz/nogodb/lib/go-fs"
)

type PageID uint32

type syncCfg struct {
	closeCh chan struct{}
	ticker  *time.Ticker
}

type PageAccessMode byte

const (
	PageAccessModeReadOnly PageAccessMode = iota
	PageAccessModeReadWrite
)

type RecordType byte

const (
	UnknownRecordType RecordType = iota
	FullType
	FirstType
	MiddleType
	LastType
)

// WAL represents a Write-Ahead Log structure that provides durability and fault-tolerance for incoming writes.
// It consists of an activePage, which is the current segment file used for new incoming writes,
// and olderPages, which is a map of segment files used for read operations.
type WAL struct {
	syncCfg
	opts         options
	activePage   *Page            // active page, used for writing
	olderPages   map[PageID]*Page // older pages, only used for read.
	mu           sync.RWMutex
	notSyncBytes int64
	storage      go_fs.Storage
}

// Page represents a single log file in WAL. A Page file consists of a sequence of variable length Position.
type Page struct {
	Id     PageID
	reader go_fs.Readable
	writer go_fs.Writable
	// TotalBlockCount Number of full blocks
	TotalBlockCount uint32
	// LastBlockSize Size of the last block that is not full
	LastBlockSize uint32
}

// Position represents the position of a record in a log file.
type Position struct {
	// PageId represents the ID of the log file where the record is located.
	PageId PageID
	// BlockNumber indicate which block where the record is located
	BlockNumber uint32
	// Offset indicate the starting offset of the record in the log file.
	Offset uint32
}

type WalIterator struct {
	currentPageId PageID
	pageIter      map[PageID]*PageIterator
}

type PageIterator struct {
	page *Page
	pos  *Position
}

// Errors \\

var (
	ErrDataTooLarge    = errors.New("data is too large")
	ErrPageNotFound    = errors.New("page not found")
	ErrInvalidChecksum = errors.New("invalid checksum")
)
