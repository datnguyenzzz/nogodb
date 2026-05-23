package common

import nogodb_common "github.com/datnguyenzzz/nogodb/lib/common"

// SSTables are either opened for reading or created for writing but not both.

// InternalWriter defines an interface for sstable writers. Implementations may vary
// depending on the TableFormat being written.
type InternalWriter interface {
	// Add adds a key-value pair to the sstable.
	Add(key nogodb_common.InternalKey, value []byte) error
	// Close finishes writing the table and closes the underlying file that the table was written to.
	Close() error
	// TODO(med): support merge operation (read-modify-write loop)
	// TODO(med): support range query (delete, ...)
}

// Inspired by :
//
//	https://github.com/facebook/rocksdb/wiki/Iterator
//	https://github.com/facebook/rocksdb/wiki/Iterator-Implementation
//
// InternalSeeker defines an interface for seeking within an sstable. Implementations may vary
// depending on the TableFormat being read.
type InternalSeeker interface {
	// SeekGTE moves the iterator to the first key/value pair whose key ≥ to the given key.
	SeekGTE(key []byte) *nogodb_common.InternalKV

	// SeekPrefixGTE moves the iterator to the first key/value pair whose key >= to the given key.
	// that has the defined prefix for faster looking up
	SeekPrefixGTE(prefix, key []byte) *nogodb_common.InternalKV

	// SeekLTE moves the iterator to the last key/value pair whose key ≤ to the given key.
	SeekLTE(key []byte) *nogodb_common.InternalKV
}

// InternalIterator iterates over a DB's key/value pairs in key order. Implementations may vary
// depending on the TableFormat being written.
type InternalIterator interface {
	InternalSeeker

	// First moves the iterator the first key/value pair.
	First() *nogodb_common.InternalKV

	// Last moves the iterator the last key/value pair.
	Last() *nogodb_common.InternalKV

	// Next moves the iterator to the next key/value pair
	Next() *nogodb_common.InternalKV

	// Prev moves the iterator to the previous key/value pair.
	Prev() *nogodb_common.InternalKV

	// Close closes the iterator and returns any accumulated error. Exhausting
	// all the key/value pairs in a table is not considered to be an error.
	Close() error
	IsClosed() bool
}
