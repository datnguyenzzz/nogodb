package go_wal

import "context"

type IIterator interface {
	// Next returns the next chunk data and its position in the WAL.
	// If there is no data, io.EOF will be returned.
	Next() (*Record, []byte, error)
}

type IWal interface {
	// Open create the directory if not exists, and open all segment files in the directory.
	// If there is no segment file in the directory, it will create a new one.
	Open(context.Context) error

	// Close the current WAL
	Close(context.Context) error

	// Delete deletes all segment files of the WAL
	Delete(context.Context) error

	// Sync syncs the current active file to the stable storage
	Sync(context.Context) error

	// Write writes the data to the WAL. It writes the data to the active Segment file.
	Write(ctx context.Context, data []byte) (*Record, error)

	// Read reads the data from the WAL according to the given record.
	Read(ctx context.Context, r *Record) ([]byte, error)

	IIterator
}
