package go_fs

import "context"

// Writable is the handle for a storage object that is open for writing.
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

// Readable is the handle for a storage object that is open for reading.
type Readable interface {
	// ReadAt reads len(p) bytes into p starting at offset off.
	//
	// Does not return partial results; if off + len(p) is past the end of the
	// object, an error is returned.
	ReadAt(ctx context.Context, p []byte, off int64) error
	Close() error

	Size() uint64
}
