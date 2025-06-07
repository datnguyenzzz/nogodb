package row_block

import (
	"fmt"

	"github.com/datnguyenzzz/nogodb/lib/go-sstable/base"
	"github.com/datnguyenzzz/nogodb/lib/go-sstable/filter"
)

// RowBlockWriter is an implementation of base.RawWriter, which writes SSTables with row-oriented blocks
type RowBlockWriter struct {
	// lastKey information of the most recent previous added key
	lastKey base.InternalKey

	comparer     base.IComparer
	filterWriter filter.IWriter
}

func (rw *RowBlockWriter) Error() error {
	//TODO implement me
	panic("implement me")
}

func (rw *RowBlockWriter) Add(key base.InternalKey, value []byte) error {
	switch key.KeyKind() {
	case base.KeyKindDelete:
		return rw.addTombstone(key, value)
	default:
		return rw.add(key, value)
	}
}

func (rw *RowBlockWriter) Close() error {
	//TODO implement me
	panic("implement me")
}

func (rw *RowBlockWriter) addTombstone(key base.InternalKey, value []byte) error {
	panic("implement me")
}

func (rw *RowBlockWriter) add(key base.InternalKey, value []byte) error {
	if err := rw.validateKey(key); err != nil {
		return err
	}

	if err := rw.doFlush(key, len(value)); err != nil {
		return err
	}

	if rw.filterWriter != nil {
		rw.filterWriter.Add(key.UserKey)
	}

	// TODO Write key/value to the buffer
}

// validateKey ensure the key is added in the asc order.
func (rw *RowBlockWriter) validateKey(key base.InternalKey) error {
	cmp := rw.comparer.Compare(key.UserKey, rw.lastKey.UserKey)
	if cmp < 0 || (cmp == 0 && rw.lastKey.Trailer <= key.Trailer) {
		return fmt.Errorf("%w: keys must be added in strictly increasing order", base.ClientInvalidRequestError)
	}

	return nil
}

// doFlush validate if required or not, if yes then flush the data to the stable storage
func (rw *RowBlockWriter) doFlush(key base.InternalKey, dataLen int) error {
	panic("implement me")
}

func NewRowBlockWriter(writable base.Writable, opts base.WriteOpt) *RowBlockWriter {
	// Use bloom filter as a default method
	bloomFilter := filter.NewFilter(filter.BloomFilter)
	return &RowBlockWriter{
		comparer:     base.NewComparer(),
		filterWriter: bloomFilter.NewWriter(),
	}
}

var _ base.RawWriter = (*RowBlockWriter)(nil)
