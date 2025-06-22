package common

import "bytes"

// IComparer defines a total ordering over the space of []byte keys: a 'less than' relationship.
type IComparer interface {
	// Compare returns -1, 0, or +1 depending on whether a is 'less than', 'equal
	// to' or 'greater than' b.
	//
	// A key a is less than b if a's prefix is byte-wise less than b's prefix, or if
	// the prefixes are equal and a's suffix is less than b's suffix
	Compare(a, b []byte) int

	// Separator appends a sequence of bytes x to dst such that a <= x && x < b,
	// where 'less than' is consistent with Compare.
	Separator(dst, a, b []byte) []byte

	// Successor appends a sequence of bytes x to dst such that x >= b, where
	// 'less than' is consistent with Compare. An implementation should return
	// nil if x equal to b.
	Successor(dst, b []byte) []byte
}

type comparer struct{}

func (c comparer) Separator(dst, a, b []byte) []byte {
	var prefixLen int
	n := min(len(a), len(b))
	for prefixLen = 0; prefixLen < n && a[prefixLen] == b[prefixLen]; prefixLen++ {
	}
	if prefixLen >= n || a[prefixLen] >= b[prefixLen] {
		return append(dst, a...)
	} else {
		if a[prefixLen]+1 < b[prefixLen] {
			dst = append(dst, a[:prefixLen+1]...)
			dst[len(dst)-1]++
			return dst
		}
		// At this point, a[prefixLen]+1 == b[prefixLen]
		// So just need to increase the byte after prefixLen-th is sufficient
		for ; prefixLen < len(a); prefixLen++ {
			if a[prefixLen] != 0xff {
				dst = append(dst, a[:prefixLen+1]...)
				dst[len(dst)-1]++
				return dst
			}
		}
	}

	return append(dst, a...)
}

func (c comparer) Successor(dst, b []byte) []byte {
	for i, v := range b {
		// get first byte i'th that < 255 --> append [b[0] ... b[i]+1] to dst
		if v < 0xff {
			dst = append(dst, b[:i+1]...)
			dst[len(dst)-1]++
			return dst
		}
	}
	// if a is full of 0xff, then do nothing
	return append(dst, b...)
}

func (c comparer) Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

func NewComparer() IComparer {
	return &comparer{}
}

var _ IComparer = (*comparer)(nil)
