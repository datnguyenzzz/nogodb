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

	// Separator returns a sequence of bytes x such that a <= x && x < b,
	// where 'less than' is consistent with Compare.
	// Trivial implementation is just simply returns "a", however we try to return
	// a shorter "x" to reduce the SSTable size
	Separator(a, b []byte) []byte

	// Successor returns a sequence of bytes x such that x >= b, where
	// 'less than' is consistent with Compare.
	// Trivial implementation is just simply return "b", however we try to return
	// a shorter "x" to reduce the SSTable size
	Successor(b []byte) []byte

	// Split return the prefix of a given key, that uses to separate
	// the actual user key and MVCC id
	Split(b []byte) int
}

type defaultComparer struct{}

func (c defaultComparer) Separator(a, b []byte) []byte {
	var prefixLen int
	n := min(len(a), len(b))
	for prefixLen = 0; prefixLen < n && a[prefixLen] == b[prefixLen]; prefixLen++ {
	}
	if prefixLen >= n || a[prefixLen] >= b[prefixLen] {
		return a
	} else {
		// If the LCP == len(b)-1 --> a[LCP]+1 < b[LCP]
		// Else just require a[LCP] +1 <= b[LCP]
		isLess := (prefixLen == len(b)-1 && a[prefixLen]+1 < b[prefixLen]) ||
			(prefixLen < len(b)-1 && a[prefixLen]+1 <= b[prefixLen])
		if a[prefixLen] < 0xff && isLess {
			dst := make([]byte, prefixLen+1)
			copy(dst, a[:prefixLen+1])
			dst[len(dst)-1]++
			return dst
		}
		return a
	}
}

func (c defaultComparer) Successor(b []byte) []byte {
	for i, v := range b {
		// get first byte i'th that < 255 --> append [b[0] ... b[i]+1] to dst
		if v < 0xff {
			dst := make([]byte, i+1)
			copy(dst, b[:i+1])
			dst[len(dst)-1]++
			return dst
		}
	}
	return b
}

func (c defaultComparer) Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

func (c defaultComparer) Split(b []byte) int {
	return len(b)
}

func NewComparer() IComparer {
	return &defaultComparer{}
}

var _ IComparer = (*defaultComparer)(nil)
