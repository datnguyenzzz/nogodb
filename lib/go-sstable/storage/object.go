package storage

// Writable is the handle for an storage object that is open for writing.
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
