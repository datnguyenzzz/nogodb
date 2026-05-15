package db

import (
	nogodb_common "github.com/datnguyenzzz/nogodb/lib/common"
	nogodb_block_cache "github.com/datnguyenzzz/nogodb/lib/go-block-cache"
	nogodb_fs "github.com/datnguyenzzz/nogodb/lib/go-fs"
	"golang.org/x/sync/semaphore"
)

// Options holds the optional parameters for configuring nogodb, at db level
type Options struct {
	// BytesPerSync syncs sstables periodically in order to smooth out writes to disk.
	// This option does not provide any persistency guarantee, but is used to avoid
	// latency spikes if the OS automatically decides to write out a large chunk
	// of dirty filesystem buffers.
	BytesPerSync struct {
		SST int64 // Default: 512 KiB
		WAL int64 // Default: 256 KiB
	}

	// Cache is used to cache uncompressed blocks from sstables.
	Cache struct {
		Type nogodb_block_cache.CacheType
		Size int64
	}

	// LoadBlockSema, if set, is used to limit the number of blocks that can be
	// loaded (i.e. read from the filesystem) in parallel
	LoadBlockSema *semaphore.Weighted

	// Comparer defines a total ordering over the space of []byte keys: a 'less
	// than' relationship. The same comparison algorithm must be used for reads
	// and writes over the lifetime of the DB.
	Comparer nogodb_common.IComparer

	// FS provides the interface for persistent file storage.
	FS nogodb_fs.FS

	// MaxManifestFileSize is the maximum size the MANIFEST file is allowed to
	// become. When the MANIFEST exceeds this size it is rolled over and a new
	// MANIFEST is created.
	MaxManifestFileSize int64

	// MaxOpenFiles is a soft limit on the number of open files that can be
	// used by the DB.
	MaxOpenFiles int

	// The size of a MemTable in steady state. The actual MemTable size starts at
	// min(256KB, MemTableSize) and doubles for each subsequent MemTable up to
	// MemTableSize. This reduces the memory pressure caused by MemTables for
	// short lived (test) DB instances.
	MemTableSize uint64 // The default value is 4MB.
}

func (o *Options) SetDefault() {
	if o.Cache.Type == nogodb_block_cache.Unknown {
		o.Cache.Type = nogodb_block_cache.LRU
	}

	if o.Cache.Size == 0 {
		o.Cache.Size = 8 << 20 // 8 MiB
	}

	if o.Comparer == nil {
		o.Comparer = nogodb_common.NewComparer()
	}

	if o.FS == nil {
		// TODO: At the moment only support Unix FS as a default
		o.FS = nogodb_fs.NewDefaultUnix()
	}

	if o.MaxManifestFileSize == 0 {
		o.MaxManifestFileSize = 128 << 20 // 128 MB
	}
	if o.MaxOpenFiles == 0 {
		o.MaxOpenFiles = 1000
	}
	if o.MemTableSize <= 0 {
		o.MemTableSize = 4 << 20 // 4 MB
	}
}
