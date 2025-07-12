package common

// SSTables are either opened for reading or created for writing but not both.

// InternalWriter defines an interface for sstable writers. Implementations may vary depending on the TableFormat being written.
type InternalWriter interface {
	// Add adds a key-value pair to the sstable.
	Add(key InternalKey, value []byte) error
	// Close finishes writing the table and closes the underlying file that the table was written to.
	Close() error
}
