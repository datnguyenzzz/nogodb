package common

type BoundaryKind uint8

// The two possible values of BoundaryKind.
//
// Note that we prefer Exclusive to be the zero value, so that zero
// UserKeyBounds are not valid.
const (
	Exclusive BoundaryKind = iota
	Inclusive
)

// UserKeyBound is a user key interval with an inclusive start boundary and
// with an end boundary that can be either inclusive or exclusive.
type UserKeyBound struct {
	Start []byte
	End   UserKeyBoundary
}

// UserKeyBoundary represents the endpoint of a bound which can be exclusive or
// inclusive.
type UserKeyBoundary struct {
	Key  []byte
	Kind BoundaryKind
}
