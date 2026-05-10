package go_fs

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
)

type vfsProvider struct {
	mu sync.Mutex

	// backed file system
	fs FS

	// knownObjects maintains information about objects that are known to the provider.
	knownObjects map[FileDesc]any

	dirName string
	dirFS   File

	bytesPerSync int64
}

func OpenVfsProvider(opts ...VfsOption) (*vfsProvider, error) {
	p := &vfsProvider{
		fs:           NewDefaultUnix(),
		dirName:      ".nogodb",
		bytesPerSync: 1024 * 1024,
		knownObjects: make(map[FileDesc]any),
	}

	for _, o := range opts {
		o(p)
	}

	if err := p.init(); err != nil {
		return nil, err
	}

	return p, nil
}

func (v *vfsProvider) init() error {
	if _, err := v.fs.Stat(v.dirName); os.IsNotExist(err) {
		if err := v.fs.MkdirAll(v.dirName, os.ModePerm); err != nil {
			return err
		}
	}

	dir, err := v.fs.OpenDir(v.dirName)
	if err != nil {
		return err
	}
	v.dirFS = dir

	filePaths, err := v.fs.List(v.dirName)
	if err != nil {
		return err
	}

	for _, filePath := range filePaths {
		name := v.fs.PathBase(filePath)

		if objectType, fileNum, ok := ParseFileName(name); ok {
			v.knownObjects[FileDesc{
				Type: objectType, Num: fileNum, Loc: FileSystem,
			}] = true
		}
	}

	return nil
}

func ParseFileName(name string) (ObjectType, DiskfileNum, bool) {
	parseDiskFileNum := func(s string) (DiskfileNum, bool) {
		u, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return DiskfileNum(0), false
		}
		return DiskfileNum(u), true
	}

	i := strings.IndexByte(name, '-')
	if i == 0 {
		return 0, 0, false
	}

	dfn, ok := parseDiskFileNum(name[i+1:])
	if !ok {
		return 0, 0, false
	}

	object, ok := ObjectTypeFromString[name[:i]]
	if !ok {
		return 0, 0, false
	}

	return object, dfn, true
}

// Open opens an existing object with the given 'file descriptor' read-only.
func (v *vfsProvider) Open(objType ObjectType, num DiskfileNum) (Readable, FileDesc, error) {
	_, err := v.LookUp(objType, num)
	if err != nil {
		return nil, FileDesc{}, err
	}

	fileName := fmt.Sprintf("%s-%d", ObjectTypeToString[objType], num)
	filePath := v.fs.PathJoin(v.dirName, fileName)
	file, err := v.fs.Open(filePath)
	if err != nil {
		return nil, FileDesc{}, err
	}

	return &wrapperFileReadable{
			File: file,
			fs:   v.fs,
			path: filePath,
		},
		FileDesc{
			Type: objType,
			Num:  num,
			Loc:  FileSystem,
		}, nil
}

// Create creates a new object and opens it for writing.
//
// The object is not guaranteed to be durable (accessible in case of crashes)
// until Sync is called.
func (v *vfsProvider) Create(objType ObjectType, num DiskfileNum) (Writable, FileDesc, error) {
	fileName := fmt.Sprintf("%s-%d", ObjectTypeToString[objType], num)
	filePath := v.fs.PathJoin(v.dirName, fileName)
	file, err := v.fs.Create(filePath, objType)
	if err != nil {
		return nil, FileDesc{}, err
	}

	writable := NewBufferedFileWriable(
		NewSyncableFile(file, v.bytesPerSync),
	)

	v.addMeta(objType, num)

	return writable, FileDesc{
		Type: objType,
		Num:  num,
		Loc:  FileSystem,
	}, nil
}

func (v *vfsProvider) addMeta(objType ObjectType, num DiskfileNum) {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.knownObjects[FileDesc{Type: objType, Num: num, Loc: FileSystem}] = true
}

func (v *vfsProvider) removeMeta(objType ObjectType, num DiskfileNum) {
	v.mu.Lock()
	defer v.mu.Unlock()

	delete(v.knownObjects, FileDesc{Type: objType, Num: num, Loc: FileSystem})
}

// LookUp returns the metadata of an object that is already exists
// it doesn't perform any I/O operations
func (v *vfsProvider) LookUp(objType ObjectType, num DiskfileNum) (FileDesc, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	fd := FileDesc{
		Type: objType, Num: num, Loc: FileSystem,
	}
	if _, ok := v.knownObjects[fd]; !ok {
		return FileDesc{}, errFileNotFound
	}

	return fd, nil
}

func (v *vfsProvider) Remove(objType ObjectType, num DiskfileNum) error {
	_, err := v.LookUp(objType, num)
	if err != nil {
		return err
	}
	fileName := fmt.Sprintf("%s-%d", ObjectTypeToString[objType], num)
	filePath := v.fs.PathJoin(v.dirName, fileName)
	err = v.fs.Remove(filePath)
	if err != nil {
		return err
	}

	v.removeMeta(objType, num)

	return nil
}

func (v *vfsProvider) List(objType ObjectType) []FileDesc {
	v.mu.Lock()
	defer v.mu.Unlock()

	res := make([]FileDesc, 0, len(v.knownObjects))
	for fd := range v.knownObjects {
		if fd.Type == objType {
			res = append(res, fd)
		}
	}

	slices.SortFunc(res, func(x, y FileDesc) int {
		if x.Num == y.Num {
			return 0
		}

		if x.Num < y.Num {
			return -1
		}

		return 1
	})

	return res
}

func (v *vfsProvider) Close() error {
	return v.dirFS.Close()
}

var _ Storage = (*vfsProvider)(nil)
