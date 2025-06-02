package row_block

import (
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/base"
)

type RowBlockWriter struct{}

func (r RowBlockWriter) Error() error {
	//TODO implement me
	panic("implement me")
}

func (r RowBlockWriter) Add(key base.Key, value []byte) error {
	//TODO implement me
	panic("implement me")
}

func (r RowBlockWriter) Close() error {
	//TODO implement me
	panic("implement me")
}

func NewRowBlockWriter(writable base.Writable, opts base.WriteOpt) *RowBlockWriter {
	// TODO implement me
	return &RowBlockWriter{}
}

var _ base.RawWriter = (*RowBlockWriter)(nil)
