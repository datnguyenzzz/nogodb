package go_wal

import "context"

type IWal interface {
	// Open create the directory if not exists, and open all segment files in the directory.
	// If there is no segment file in the directory, it will create a new one.
	Open(context.Context) error

	// Close the current WAL
	Close(context.Context) error

	// Sync syncs the current active file to the stable storage
	Sync(context.Context) error
}
