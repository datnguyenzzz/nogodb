package common

type TableFormat byte

const (
	UnknownTableFormat TableFormat = iota
	RowBlockedBaseTableFormat
	ColumnarBlockedBasedTableFormat
)
