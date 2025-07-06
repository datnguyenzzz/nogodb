package common

import "encoding/binary"

type TableFormat byte

const (
	UnknownTableFormat TableFormat = iota
	RowBlockedBaseTableFormat
	ColumnarBlockedBasedTableFormat
)

type TableVersion byte

const (
	TableV1 TableVersion = iota
)

const (
	TableVersionLen = 4
	MagicNumberLen  = 8
)

var TableFooterSize = map[TableVersion]int{
	TableV1: binary.MaxVarintLen64 + TableVersionLen + MagicNumberLen,
}
