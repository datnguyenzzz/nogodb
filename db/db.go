package db

import (
	"context"
	"io"
	"sync"

	nogodb_common "github.com/datnguyenzzz/nogodb/lib/common"
)

// IReader is a readable key/value store.
//
// It is safe to call Get and NewIter from concurrent goroutines.
type IReader interface {
	// Get gets the value for the given key. It returns ErrNotFound if the DB
	// does not contain the key. On success, the caller MUST call closer.Close()
	// or a memory leak will occur.
	Get(ctx context.Context, key []byte) (value []byte, closer io.Closer, err error)

	// TODO(datnguyenzzz):
	//.  Enable when ready to support
	// NewIter returns an iterator that is unpositioned (Iterator.Valid() will
	// return false). The iterator can be positioned via a call to SeekGE,
	// SeekLT, First or Last.
	// NewIterWithContext(ctx context.Context) (*Iterator, error)

	// Close closes the Reader. It may or may not close any underlying io.Reader
	// or io.Writer, depending on how the DB was created.
	Close(ctx context.Context) error
}

// IWriter is a writable key/value store.
type IWriter interface {
	// TODO(datnguyenzzz):
	//.  Enable when ready to support
	// Apply the operations contained in the batch to the DB.
	// Apply(batch *Batch) error

	// Set sets the value for the given key. It overwrites any previous value
	// for that key.
	Set(key, value []byte) error

	// Delete deletes the value for the given key. Deletes are blind all will
	// succeed even if the given key does not exist.
	Delete(key []byte) error

	// TODO(datnguyenzzz):
	//. Enable when ready to support
	// Merge merges the value for the given key.
	// https://github.com/facebook/rocksdb/wiki/Merge-Operator
	// Merge(key, value []byte) error

	// TODO(datnguyenzzz):
	//.  Enable when ready to support
	// DeleteRange deletes all of the point keys (and values) in the range
	// [start,end) (inclusive on start, exclusive on end).
	// DeleteRange(start, end []byte) error

	// TODO(datnguyenzzz):
	//.  Enable when ready to support
	// RangeKeySet sets a range key mapping the key range [start, end) at the MVCC
	// timestamp suffix to value. The suffix is optional. If any portion of the key
	// range [start, end) is already set by a range key with the same suffix value,
	// RangeKeySet overrides it.
	// RangeKeySet(start, end, suffix, value []byte) error

	// TODO(datnguyenzzz):
	//.  Enable when ready to support
	// RangeKeyDelete deletes all of the range keys in the range [start,end)
	// (inclusive on start, exclusive on end). RangeKeyDelete removes all
	// range keys within the bounds, including those with or without suffixes.
	// RangeKeyDelete(start, end []byte) error
}

// DB provides a concurrent, persistent ordered key/value store.
type DB struct {
	dirname string
	opts    *Options
	cmp     nogodb_common.IComparer

	// bgCtx is cancelled when the DB is closing. Background goroutines
	// (compactions, table stats loading) use this context
	bgCtx       context.Context
	bgCtxCancel context.CancelFunc
	closedCh    chan struct{}

	// mu is the main mutex protecting internal DB state. This mutex contains many
	// fields because those fields need to be updated atomically. In particular,
	// log.*, mem.*, and snapshot list need to be accessed and updated atomically
	mu struct {
		sync.Mutex
		log struct { // Write ahead log
		}
		mem struct { // Mem table
		}
		compact struct { // Compactions
		}
		snapshot struct { // Snapshots
		}
	}

	// TODO. Write batch with index
	// https://github.com/facebook/rocksdb/wiki/Write-Batch-With-Index

	// TODO. commit pipeline.
	// The commit pipeline manages the steps in committing write batches,
	// such as writing the batch to the WAL and applying its contents to
	// the memtable

	// TODO. compaction scheduler
}

var (
	_ IReader = (*DB)(nil)
	_ IWriter = (*DB)(nil)
)

func Open(dirName string, opt Options) (*DB, error) {
	opt.SetDefault()
	db := &DB{}

	// TODO(high): reads the named database directory and recovers
	// the set of files encoding the database state at the moment the previous
	// process exited.

	return db, nil
}
