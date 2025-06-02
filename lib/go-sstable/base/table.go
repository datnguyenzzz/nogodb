package base

type TableFormat byte

const (
	UnknownTableFormat TableFormat = iota
	RowBlockedBaseTableFormat
	ColumnarBlockedBasedTableFormat
)
