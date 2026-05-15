// TODO(low) control by go build tags
package go_fs

import (
	"os"
	"path/filepath"
	"syscall"
)

type unixFile struct {
	*os.File
}

func (f *unixFile) Stat() (FileInfo, error) {
	return f.File.Stat()
}

type defaultUnix struct{}

func NewDefaultUnix() *defaultUnix {
	return &defaultUnix{}
}

func wrapOSFile(f *os.File) unixFile {
	return unixFile{File: f}
}

func (f *defaultUnix) OpenDir(name string) (File, error) {
	file, err := os.OpenFile(name, syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}

	return wrapOSFile(file), nil
}

func (f *defaultUnix) Create(name string, objType ObjectType) (File, error) {
	const openFlags = os.O_RDWR | os.O_CREATE | os.O_EXCL | syscall.O_CLOEXEC
	osFile, err := os.OpenFile(name, openFlags, 0o666)
	// If the file already exists, remove it and try again.
	if os.IsNotExist(err) {
		if err = f.Remove(name); err != nil {
			return nil, err
		}

		osFile, err = os.OpenFile(name, openFlags, 0o666)
	}

	if err != nil {
		return nil, err
	}

	return wrapOSFile(osFile), nil
}

func (f *defaultUnix) Open(name string) (File, error) {
	osFile, err := os.OpenFile(name, os.O_RDONLY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}

	return wrapOSFile(osFile), nil
}

func (f *defaultUnix) Remove(name string) error {
	return os.Remove(name)
}

func (f *defaultUnix) List(dir string) ([]string, error) {
	osFile, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	defer osFile.Close()

	return osFile.Readdirnames(-1)
}

func (f *defaultUnix) PathBase(path string) string {
	return filepath.Base(path)
}

func (f *defaultUnix) Stat(name string) (FileInfo, error) {
	return os.Stat(name)
}

func (f *defaultUnix) PathJoin(elem ...string) string {
	return filepath.Join(elem...)
}

func (f *defaultUnix) MkdirAll(dir string, perm os.FileMode) error {
	return os.MkdirAll(dir, perm)
}

var _ FS = (*defaultUnix)(nil)
