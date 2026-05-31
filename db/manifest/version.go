package manifest

import (
	"sync"
	"sync/atomic"

	nogodb_common "github.com/datnguyenzzz/nogodb/lib/common"
)

// NumLevels is the number of levels a Version contains.
const NumLevels = 7

// Version is a collection of SStable metadata for on-disk tables at various
// levels. Memtables are written to level-0 tables, and compactions migrate
// data from level N to level N+1.
type version struct {
	cmp  nogodb_common.IComparer
	refs atomic.Uint32
	// levels contains metadata for all of the tables within a level of the LSM.
	levels     [NumLevels]*levelMetadata
	list       *versionList
	prev, next *version
}

func newVersion(
	comparer nogodb_common.IComparer,
) *version {
	v := &version{
		cmp: comparer,
	}
	for i := range NumLevels {
		v.levels[i] = NewLevelMetadata(i)
	}
	return v
}

// The versions are ordered from oldest to newest.
type versionList struct {
	mu   *sync.Mutex
	root version
}

func (l *versionList) init(mu *sync.Mutex) {
	l.mu = mu
	l.root.next = &l.root
	l.root.prev = &l.root
}

// pushBack adds a _new_ version to the back of the list
func (l *versionList) pushBack(v *version) {
	if v.list != nil || v.next != nil || v.prev != nil {
		panic("versionList.pushBack tried adding an old version ")
	}

	tmp := l.root.prev
	l.root.prev = v
	tmp.next = v
	v.next = &l.root
	v.prev = tmp
	v.list = l
}

func (l *versionList) back() *version {
	return l.root.prev
}
