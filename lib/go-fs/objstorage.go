package go_fs

import (
	"io"
)

type ObjectType byte

const (
	TypeManifest ObjectType = iota
	TypeTable
	TypeWAL
)

type Location byte

const (
	InMemory Location = iota
	LocalFile
	Remote
)

type FileDesc struct {
	Type ObjectType
	Num  int64
	Loc  Location
}

// Writable is the handle for a storage object that is open for writing.
type Writable interface {
	// io.Write writes len(p) bytes from p to the underlying object. The data is not
	// guaranteed to be durable until Finish is called.
	//
	// io.Write make sure that the error will be not nil, if n < len(p)

	io.WriteCloser

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
	io.ReaderAt
	io.ReadSeeker

	Size() uint64
}

// Storage is a singleton object used to access and manage objects.
//
// An object is conceptually like a large immutable file. The main use of
// objects is for storing sstables; in the future it could also be used for blob
// storage.
type Storage interface {
	// Open opens an existing object with the given 'file descriptor' read-only.
	Open(objType ObjectType, num int64) (Readable, FileDesc, error)

	// Create creates a new object and opens it for writing.
	//
	// The object is not guaranteed to be durable (accessible in case of crashes)
	// until Sync is called.
	Create(objType ObjectType, num int64) (Writable, FileDesc, error)

	// LookUp returns the metadata of an object that is already exists
	// it doesn't perform any I/O operations
	LookUp(objType ObjectType, num int64) (FileDesc, error)

	Remove(objType ObjectType, num int64) error

	Close() error
}
