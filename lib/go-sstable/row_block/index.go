package row_block

import "github.com/datnguyenzzz/nogodb/lib/go-sstable/common"

type indexWriter struct {
	indexBlock *rowBlockBuf
	comparer   common.IComparer
}

func (w *indexWriter) createKey(prevKey, key *common.InternalKey) *common.InternalKey {
	var sep *common.InternalKey
	if key.UserKey == nil && key.Trailer == 0 {
		sep = prevKey.Successor(w.comparer)
	} else {
		sep = prevKey.Separator(w.comparer, key)
	}

	return sep
}

func (w *indexWriter) add(key *common.InternalKey, bh *common.BlockHandle) error {
	panic("implement me boss!!")
}

func newIndexWriter(comparer common.IComparer) *indexWriter {
	return &indexWriter{
		// The index block also use the row oriented layout.
		// And its restart interval is 1, aka every entry is a restart point.
		indexBlock: newDataBlock(1),
		comparer:   comparer,
	}
}
