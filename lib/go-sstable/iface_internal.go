package go_sstable

import "github.com/datnguyenzzz/nogodb/lib/go-sstable/internal"

// rawWriter defines an interface for sstable writers. Implementations may vary depending on the TableFormat being written.
type rawWriter interface {
	// Error returns the current accumulated error if any.
	Error() error
	// Add adds a key-value pair to the sstable.
	Add(key internal.Key, value []byte) error
	// Close finishes writing the table and closes the underlying file that the table was written to.
	Close() error

	// TODO add more ...
}
