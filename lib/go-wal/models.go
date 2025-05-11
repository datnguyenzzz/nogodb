package go_wal

type SegmentID uint32

// WAL represents a Write-Ahead Segment structure that provides durability and fault-tolerance for incoming writes.
// It consists of an activeSegment, which is the current segment file used for new incoming writes,
// and olderSegments, which is a map of segment files used for read operations.
type WAL struct {
	activeSegment *Segment               // active log file, used for new incoming writes.
	olderSegments map[SegmentID]*Segment // older segment files, only used for read.
	// ...
}

// Segment represents a single log file in WAL. A Segment file consists of a sequence of variable length Record.
type Segment struct {
	Id SegmentID
	// ...
}

// Record represents the position of a record in a log file.
type Record struct {
	// LogId represents the ID of the log file where the record is located.
	LogId SegmentID
	// BlockNumber indicate which block where the record is located
	BlockNumber uint32
	// Offset indicate the starting offset of the record in the log file.
	Offset uint64
	// 	Size How many bytes the record data takes up in the segment file.
	Size uint32
}
