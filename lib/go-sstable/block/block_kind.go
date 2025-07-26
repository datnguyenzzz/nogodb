package block

type BlockKind byte

const (
	BlockKindUnknown BlockKind = iota
	BlockKindData
	BlockKindIndex
	BlockKindFilter
)

var BlockKindStrings = map[BlockKind]string{
	BlockKindData:   "data",
	BlockKindIndex:  "index",
	BlockKindFilter: "filter",
}
