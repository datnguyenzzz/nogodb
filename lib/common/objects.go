package common

type DiskfileNum int64

type ObjectType byte

const (
	TypeManifest ObjectType = iota
	TypeTable
	TypeWAL
	TypeLock
)

var (
	ObjectTypeFromString = map[string]ObjectType{
		"manifest": TypeManifest,
		"sst":      TypeTable,
		"wal":      TypeWAL,
		"LOCK":     TypeLock,
	}
	ObjectTypeToString = map[ObjectType]string{
		TypeManifest: "manifest",
		TypeTable:    "sst",
		TypeWAL:      "wal",
		TypeLock:     "LOCK",
	}
)
