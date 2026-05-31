package manifest

import (
	"io"
	"sync"
	"sync/atomic"

	"github.com/datnguyenzzz/nogodb/db/options"
	nogodb_common "github.com/datnguyenzzz/nogodb/lib/common"
	nogodb_record "github.com/datnguyenzzz/nogodb/lib/common/record"
	nogodb_fs "github.com/datnguyenzzz/nogodb/lib/go-fs"
)

// VersionSet manages a list of immutable versions, and manages the
// creation of a new version from the most recent version.
type VersionSet struct {
	dbOpt    *options.DBOption
	versions *versionList
	mu       *sync.Mutex

	// Next seqNum to use for WAL writes.
	logSeqNum nogodb_common.SeqNum

	// The upper bound on sequence numbers that have been assigned so far.
	// This is the sequence number for which records in the database are visible
	// during reads. The visible sequence number exists because committing a
	// batch (eg write the batch to the WAL and applying its contents to the memtable)
	// is an atomic operation, yet adding records to the memtable is done
	// without an exclusive lock (the skiplists used by both Pebble and RocksDB
	// are lock-free).
	visibleSeqNum nogodb_common.SeqNum

	// The next file number. A shared counter is used to assign file
	// numbers for the WAL, MANIFEST, sstable files.
	nextFileNum nogodb_common.DiskfileNum

	manifestWriter   *nogodb_record.Writer
	manifestStorager nogodb_fs.Storage
}

func (vs *VersionSet) init(
	dbOpt *options.DBOption,
	mu *sync.Mutex,
) (err error) {
	vs.dbOpt = dbOpt
	vs.mu = mu
	vs.versions.init(mu)
	atomic.StoreUint64((*uint64)(&vs.logSeqNum), 0)
	atomic.StoreInt64((*int64)(&vs.nextFileNum), 1)
	vs.manifestStorager, err = nogodb_fs.OpenVfsProvider(
		nogodb_fs.WithDirName(dbOpt.Manifest.Dir),
		nogodb_fs.WithFS(dbOpt.FS),
	)
	if err != nil {
		return err
	}

	return nil
}

func (vs *VersionSet) NewDB(
	opt *options.DBOption,
	mu *sync.Mutex,
) error {
	if err := vs.init(opt, mu); err != nil {
		return err
	}

	blankVersion := newVersion(opt.Comparer)
	if blankVersion.refs.Load() > 0 {
		panic("VersionSet tries appending a referenced version")
	}
	vs.versions.pushBack(blankVersion)

	var err error
	if err = vs.createManifest(vs.getNextFileNum()); err != nil {
		return err
	}
	if err = vs.manifestWriter.Flush(); err != nil {
		vs.dbOpt.Logger.Fatalf("manifest flushed failed: %v", err)
	}
	if err = vs.manifestStorager.Sync(nogodb_common.TypeManifest, vs.fileNum()); err != nil {
		vs.dbOpt.Logger.Fatalf("manifest sync failed: %v", err)
	}

	return nil
}

func (vs *VersionSet) createManifest(fileNum nogodb_common.DiskfileNum) error {
	var err error
	var manifestWriter *nogodb_record.Writer

	defer func() {
		if manifestWriter != nil {
			_ = manifestWriter.Close()
		}
		if err != nil {
			_ = vs.manifestStorager.Remove(nogodb_common.TypeManifest, fileNum)
		}
	}()

	var writable io.Writer
	writable, _, err = vs.manifestStorager.Create(nogodb_common.TypeManifest, fileNum)
	if err != nil {
		return err
	}
	manifestWriter = nogodb_record.NewWriter(writable)

	// Add all existing SSTables meta to the current version
	edit := &versionEdit{
		comparerName: vs.dbOpt.Comparer.Name(),
		nextFileNum:  int64(vs.nextFileNum),
	}

	for lvl, levelMeta := range vs.currentVersion().levels {
		for tableMeta := range levelMeta.All() {
			edit.newTables = append(edit.newTables, newTableEntry{
				level: lvl,
				meta:  &tableMeta,
			})
		}
	}

	w, err := manifestWriter.Next()
	if err != nil {
		return err
	}

	if err := edit.Encode(w); err != nil {
		return err
	}

	if vs.manifestWriter != nil {
		if err := vs.manifestWriter.Close(); err != nil {
			return err
		}

		vs.manifestWriter = nil
	}

	vs.manifestWriter, manifestWriter = manifestWriter, vs.manifestWriter

	return nil
}

func (vs *VersionSet) GetLogSeqNum() uint64 {
	return atomic.LoadUint64((*uint64)(&vs.logSeqNum))
}

func (vs *VersionSet) SetVisibleSeqNum(n uint64) {
	atomic.StoreUint64((*uint64)(&vs.visibleSeqNum), n)
}

func (vs *VersionSet) currentVersion() *version {
	return vs.versions.back()
}

func (vs *VersionSet) getNextFileNum() nogodb_common.DiskfileNum {
	return nogodb_common.DiskfileNum(atomic.AddInt64((*int64)(&vs.nextFileNum), 1) - 1)
}

func (vs *VersionSet) fileNum() nogodb_common.DiskfileNum {
	return nogodb_common.DiskfileNum(atomic.LoadInt64((*int64)(&vs.nextFileNum)) - 1)
}
