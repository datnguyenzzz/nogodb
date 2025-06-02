package go_sstable

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

// IWriter represent writer to the SSTable
type IWriter interface {
	// Write appends key/value pair to the table. It is safe to modify the contents of the arguments after Append returns.
	Write(key, value []byte) error
	// Close will finalize the table. Calling Append is not possible after Close
	Close() error
}

// IReader represent reader to the SSTable
type IReader interface {
	//FindGE finds key/value pair whose key is greater than or equal to the
	// given key. It returns ErrNotFound if the table doesn't contain
	// such pair.
	// If filtered is true then the nearest 'block' will be checked against
	// 'filter data' (if present) and will immediately return ErrNotFound if
	// 'filter data' indicates that such pair doesn't exist.
	FindGE(key []byte, filtered bool) ([]byte, []byte, error)

	// Get gets the value for the given key. It returns errors.ErrNotFound
	// if the table does not contain the key.
	Get(key []byte) ([]byte, error)

	// TODO implement iterator through the blocks
}
