package go_sstable

// IWriter represent an interface of writer for downstream client to the SSTable
type IWriter interface {
	// Set appends key/value pair to the table. It is safe to modify the contents of the arguments after Append returns.
	Set(key, value []byte) error
	// Delete a key within a table
	Delete(key []byte) error
	// DeleteRange deletes all of the keys (and values) in the range [start,end)
	// (inclusive on start, exclusive on end).
	DeleteRange(start, end []byte) error
	// Merge adds an action to the DB that merges the value at key with the new value.
	Merge(key, value []byte) error
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
