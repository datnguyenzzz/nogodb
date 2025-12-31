package go_fs

import (
	"bytes"
	"io"
	"sync"
)

type fileId int64

type inmemStorage struct {
	files map[fileId]*memFile
	mu    sync.Mutex
}

type memFile struct {
	bytes.Buffer
	// opened either for reading or writing
	open    bool
	storage *inmemStorage
}

type memReader struct {
	*memFile
}

func (mr memReader) ReadAt(p []byte, off int64) (n int, err error) {
	if off > int64(mr.Len()) {
		return 0, io.EOF
	}

	n = copy(p, mr.Bytes()[off:])
	if n < len(p) {
		return n, io.EOF
	}

	return n, err
}

func (mr memReader) Size() uint64 {
	return uint64(mr.Len())
}

func (mr memReader) Close() error {
	return nil
}

type memWriter struct {
	*memFile
}

func (m memWriter) Close() error {
	m.memFile.storage.mu.Lock()
	defer m.memFile.storage.mu.Unlock()
	if !m.open {
		return errFileIsClosed
	}
	m.open = false
	return nil
}

func (m memWriter) Sync() error {
	// no op
	return nil
}

func (m memWriter) Finish() error {
	m.memFile.storage.mu.Lock()
	defer m.memFile.storage.mu.Unlock()
	if !m.open {
		return errFileIsClosed
	}
	m.open = false
	return nil
}

func (m memWriter) Abort() {
	m.memFile.storage.mu.Lock()
	defer m.memFile.storage.mu.Unlock()
	if m.open {
		m.open = false
	}
}

func NewInmemStorage() Storage {
	return &inmemStorage{
		files: make(map[fileId]*memFile),
	}
}

func (i inmemStorage) Open(objType ObjectType, num int64) (Readable, FileDesc, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	if file, ok := i.files[i.toFileId(objType, num)]; ok {
		file.open = true
		return memReader{memFile: file}, i.toFileDesc(objType, num), nil
	}

	return nil, FileDesc{}, errFileNotFound
}

func (i inmemStorage) Create(objType ObjectType, num int64) (Writable, FileDesc, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	fid := i.toFileId(objType, num)
	if _, ok := i.files[fid]; ok {
		return nil, FileDesc{}, errFileExists
	}

	i.files[fid] = &memFile{open: true, storage: &i}

	return memWriter{memFile: i.files[fid]}, i.toFileDesc(objType, num), nil
}

func (i inmemStorage) LookUp(objType ObjectType, num int64) (FileDesc, error) {
	return i.toFileDesc(objType, num), nil
}

func (i inmemStorage) Remove(objType ObjectType, num int64) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	fid := i.toFileId(objType, num)
	if _, ok := i.files[fid]; !ok {
		return errFileNotFound
	}

	delete(i.files, fid)

	return nil
}

func (i inmemStorage) Close() error {
	return nil
}

func (i inmemStorage) toFileId(objType ObjectType, num int64) fileId {
	return fileId(num<<4 | int64(objType))
}

func (i inmemStorage) toFileDesc(objType ObjectType, num int64) FileDesc {
	return FileDesc{Num: num, Type: objType, Loc: InMemory}
}

var _ Storage = (*inmemStorage)(nil)
