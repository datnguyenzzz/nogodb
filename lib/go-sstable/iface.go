package go_sstable

import "github.com/datnguyenzzz/nogodb/lib/go-sstable/common"

// IWriter represent an interface of writer for downstream client to the SSTable
// The data written layout is controlled internally, which caller of this function
// shouldn't worry much about it
type IWriter interface {
	// Set appends key/value pair to the table. It is safe to modify the contents of the arguments after Append returns.
	Set(key, value []byte) error
	// Delete a key within a table
	Delete(key []byte) error
	// Close will finalize the table. Calling Append is not possible after Close
	Close() error
	// TODO(med): support merge operation (read-modify-write loop)
	// TODO(med): support range query (delete, ...)
}

type IIterator interface {
	common.InternalIterator
}

// TODO(med): Support functions can be exposed as a reader IReader,
//  given that most of the cases the outsider caller only use iterator
