package go_fs

import (
	"io"
)

type FileType byte

const (
	TypeManifest FileType = iota
	TypeTable
	TypeWAL
)

type FileDesc struct {
	Type FileType
	Num  int64
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
type Storage interface{}
