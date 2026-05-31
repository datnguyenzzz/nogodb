package manifest

import (
	"iter"

	// TODO(low): having nogodb_btree
	"github.com/google/btree"
)

const (
	degree = 16
)

// LevelMetadata contains metadata for all of the tables within a level of the LSM.
type levelMetadata struct {
	level int
	tree  *btree.BTreeG[tableMetadata]
}

func NewLevelMetadata(level int) *levelMetadata {
	return &levelMetadata{
		level: level,
		tree: btree.NewG(degree, func(a, b tableMetadata) bool {
			return a.Compare(b) <= 0
		}),
	}
}

// All returns an iterator over all tables in the level.
func (l *levelMetadata) All() iter.Seq[tableMetadata] {
	return func(yield func(tableMetadata) bool) {
		l.tree.Ascend(yield)
	}
}
