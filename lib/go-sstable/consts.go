package go_sstable

type TableFormat byte

const (
	UnknownTableFormat TableFormat = iota
	RowBlockedBaseTableFormat
	ColumnarBlockedBasedTableFormat
)
