package common

import go_fs "github.com/datnguyenzzz/nogodb/lib/go-fs"

// A DiskFileNum identifies a file or object with exists on disk.
type DiskFileNum uint64

func FromFileDescToFileNum(fd go_fs.FileDesc) DiskFileNum {
	return DiskFileNum(fd.Num<<8 | int64(fd.Type))
}
