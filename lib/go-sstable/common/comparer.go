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
	// TODO add more ...
}

type comparer struct{}

func (c comparer) Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

func NewComparer() IComparer {
	return &comparer{}
}

var _ IComparer = (*comparer)(nil)
