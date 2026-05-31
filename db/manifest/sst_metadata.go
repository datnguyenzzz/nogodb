package manifest

// newTableEntry holds the state for a new sstable or one moved from a different level.
type newTableEntry struct {
	level int
	meta  *tableMetadata
}

// tableMetadata is maintained for leveled sstables. TableMetadata does not
// contain the actual level of the sst, since such leveled-ssts can move across
// levels in different versions, while sharing the same TableMetadata.
type tableMetadata struct {
	// TableNum is the table number, unique across the lifetime of a DB.
	//
	TableNum uint64
	// Size is the size of the file, in bytes.
	Size uint64
}

func (t *tableMetadata) Compare(t2 tableMetadata) int {
	return 0
}
