package base

// KeyKind enumerates the kind of key: a deletion tombstone, a set
// value, a merged value, etc.
type KeyKind byte

const (
	KeyKindDelete KeyKind = iota
	KeyKindSet
	KeyKindMerge
)

// SeqNum is a sequence number defining precedence among identical keys. A key
// with a higher sequence number takes precedence over a key with an equal user
// key of a lower sequence number.
type SeqNum uint64

// InternalKeyTrailer encodes a [SeqNum (7) + InternalKeyKind (1)].
type InternalKeyTrailer uint64

// InternalKey or internal key. Due to the LSM structure, keys are never updated in place, but overwritten with new
// versions. An Internal InternalKey is composed of the user specified key, a sequence number (7 bytes) and a kind (1 byte).
//
//	+-------------+------------+----------+
//	| UserKey (N) | SeqNum (7) | Kind (1) |
//	+-------------+------------+----------+
type InternalKey struct {
	UserKey []byte
	Trailer InternalKeyTrailer
}

func MakeKey(userKey []byte, num SeqNum, kind KeyKind) InternalKey {
	trailer := InternalKeyTrailer((uint64(num) << 8) | uint64(kind))
	return InternalKey{
		UserKey: userKey,
		Trailer: trailer,
	}
}

func (k *InternalKey) SeqNum() SeqNum {
	return SeqNum(k.Trailer >> 8)
}

func (k *InternalKey) KeyKind() KeyKind {
	return KeyKind(k.Trailer & 0xFF) // trailer & (2^8 - 1)
}
