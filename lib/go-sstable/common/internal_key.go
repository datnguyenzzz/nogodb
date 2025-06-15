package common

import "encoding/binary"

// KeyKind enumerates the kind of key: a deletion tombstone, a set
// value, a merged value, etc.
type KeyKind byte

const (
	KeyKindUnknown KeyKind = iota
	KeyKindDelete
	KeyKindSet
	KeyKindMerge
)

// SeqNum is a sequence number defining precedence among identical keys. A key
// with a higher sequence number takes precedence over a key with an equal user
// key of a lower sequence number.
type SeqNum uint64

// InternalKeyTrailer encodes a [SeqNum (7) + InternalKeyKind (1)].
type InternalKeyTrailer uint64

const InternalKeyTrailerLen = 8

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

func (k *InternalKey) Size() int {
	return len(k.UserKey) + InternalKeyTrailerLen
}

// SerializeTo serialise an internal key into give buffer. Caller must ensure buf has enough size to hold
func (k *InternalKey) SerializeTo(buf []byte) {
	i := copy(buf, k.UserKey)
	binary.LittleEndian.PutUint64(buf[i:], uint64(k.Trailer))
}

func DeserializeKey(key []byte) *InternalKey {
	n := len(key) - InternalKeyTrailerLen
	if n >= 0 {
		return &InternalKey{
			UserKey: key[:n:n],
			Trailer: InternalKeyTrailer(binary.LittleEndian.Uint64(key[n:])),
		}
	}

	return &InternalKey{
		Trailer: InternalKeyTrailer(KeyKindUnknown),
	}
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
