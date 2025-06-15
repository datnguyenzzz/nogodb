package common

// SSTables are either opened for reading or created for writing but not both.

// Writable is the handle for an object that is open for writing.
type Writable interface {
	// Write writes len(p) bytes from p to the underlying object. The data is not
	// guaranteed to be durable until Finish is called.
	//
	// Note that Write *is* allowed to modify the slice passed in, whether
	// temporarily or permanently. Callers of Write need to take this into
	// account.
	Write(p []byte) error

	// Finish completes the object and makes the data durable.
	// No further calls are allowed after calling Finish.
	Finish() error

	// Abort gives up on finishing the object. There is no guarantee about whether
	// the object exists after calling Abort.
	// No further calls are allowed after calling Abort.
	Abort()
}

// RawWriter defines an interface for sstable writers. Implementations may vary depending on the TableFormat being written.
type RawWriter interface {
	// Error returns the current accumulated error if any.
	Error() error
	// Add adds a key-value pair to the sstable.
	Add(key InternalKey, value []byte) error
	// Close finishes writing the table and closes the underlying file that the table was written to.
	Close() error

	// TODO add more ...
}
