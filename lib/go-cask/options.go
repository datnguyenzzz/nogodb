package go_cask

import "time"

type CompactionPolicy int8

const (
	// AlwaysMerge no restrictions
	AlwaysMerge CompactionPolicy = iota
	// NeverMerge meaning that merging will never be attempted
	NeverMerge
	// WindowMerge specifying the hours during which merging is permitted between 0 and 23
	WindowMerge
)

type SyncPolicy int8

const (
	// NoneSync which will let the operating system manage syncing writes
	NoneSync SyncPolicy = iota
	// OSync which will uses the O_SYNC flag to force syncs on every write
	OSync
	// IntervalSync by which will force go-cask to sync every configurable seconds.
	IntervalSync
)

type EngineOpts[V any] func(engine *DB[V])

type generalOptions struct {
	// dataRoot The directory under which go-cask will store its data.
	dataRoot string

	// maxFileSize Describes the maximum permitted size for any single data file.
	// If a write operation causes the current file to exceed this size threshold then that file is closed,
	// and a new file is opened for writes.
	maxFileSize uint64

	// foldMaxAge Fold keys thresholds will reuse the index if another fold was
	// started less than foldMaxAge ago and there were fewer than foldMaxPuts updates.
	// Otherwise, it will wait until all current fold keys complete and then start.
	// Set either option to -1 to disable.
	foldMaxAge  int
	foldMaxPuts int

	// openTimeout Specifies the maximum time Bitcask will block on startup while attempting
	// to create or open the data directory. You generally need not change this value.
	// Only should you consider a longer timeout when for some reason the timeout is exceeded
	openTimeout time.Duration
}

var defaultGeneralOptions = generalOptions{
	dataRoot:    "./data/go-cask",
	maxFileSize: 1 * 1024 * 1024 * 1024, // 1GB
	foldMaxAge:  -1,
	foldMaxPuts: 0,
	openTimeout: 5 * time.Second,
}

type expiryOptions struct {
	// expiry By default, go-cask keeps all of your data around.
	// If your data has limited time value, or if you need to purge data
	// for space reasons, you can set the expiry option
	expiry time.Duration

	// graceTime By default, go-cask will trigger a merge whenever
	// a data file contains an expired key. This may result in excessive
	// merging under some usage patterns. To prevent this you can set the
	// graceTime option. go-cask will defer triggering a merge solely for
	// key expiry by the configured number of seconds. For example, setting
	// this to 1h effectively limits each cask to merging for expiry once per hour.
	graceTime time.Duration
}

var defaultExpiryOptions = expiryOptions{
	expiry:    1<<63 - 1, // Max possible duration
	graceTime: 0 * time.Second,
}

type compactionOptions struct {
	// fragmentationThreshold Describes which ratio of dead keys to total keys
	// in a file will cause it to be included in the merge. The value of this
	// setting is a percentage from 0 to 1. For example, if a data file contains
	// 4 dead keys and 6 live keys, it will be included in the merge at the
	// default ratio (which is 0.4). Increasing the value will cause fewer files
	// to be merged, decreasing the value will cause more files to be merged.
	fragmentationThreshold float32

	// deadBytesThreshold Describes the minimum amount of data occupied
	// by dead keys in a file to cause it to be included in the merge.
	// Increasing the value will cause fewer files to be merged, whereas decreasing
	// the value will cause more files to be merged.
	deadBytesThreshold uint

	// smallFileThreshold Describes the minimum size a file must have to be
	// excluded from the merge. Files smaller than the threshold will be included.
	// Increasing the value will cause more files to be merged,
	// whereas decreasing the value will cause fewer files to be merged.
	smallFileThreshold uint

	// deadBytesTriggers Describes how much data stored for dead keys in a single file
	// will trigger merging. If a file meets or exceeds the trigger value for dead bytes,
	// merge will be triggered. Increasing the value will cause merging to occur less often,
	// whereas decreasing the value will cause merging to happen more often.
	// When either of these constraints are met by any file in the directory,
	// go-cask will attempt to merge files.
	deadBytesTriggers uint

	// fragmentationTriggers Describes which ratio of dead keys to total keys in a file
	// will trigger merging. The value of this setting is a percentage from 0 to 1.
	// For example, if a data file contains 6 dead keys and 4 live keys, then
	// merge will be triggered at the default setting. Increasing this value
	// will cause merging to occur less often, whereas decreasing the value will
	// cause merging to happen more often.
	fragmentationTriggers float32

	// policy Lets you specify when during the day merge operations are allowed to be triggered.
	// startWindow and endWindow are needed for the window merge policy. If merging has a significant impact
	// on performance of your cluster, or your cluster has quiet periods in which little storage activity occurs,
	// you may want to change this setting from the default.
	policy      CompactionPolicy
	startWindow int
	endWindow   int

	// checkInterval go-cask periodically runs checks to determine whether
	// compactions are necessary. This parameter determines how often those checks take place.
	checkInterval time.Duration

	// checkJitter In order to prevent compaction operations from taking place on
	// different nodes at the same time, it can apply random variance to the compaction times,
	// expressed as a percentage of checkInterval.
	checkJitter float32

	// maxMergeSize Maximum amount of data to merge in one go
	maxMergeSize uint64
}

var defaultCompactionOptions = compactionOptions{
	fragmentationThreshold: 0.4,
	deadBytesThreshold:     128 * 1024 * 1024, //128MB
	smallFileThreshold:     10 * 1024 * 1024,  //10MB
	deadBytesTriggers:      512 * 1024 * 1024, //512MB
	fragmentationTriggers:  0.6,
	policy:                 AlwaysMerge,
	startWindow:            0,
	endWindow:              23,
	checkInterval:          3 * time.Minute,
	checkJitter:            0.3,
	maxMergeSize:           100 * 1024 * 1024 * 1024, // 100GB
}

type syncOptions struct {
	// strategy Changes the durability of writes by specifying when to synchronize data to disk.
	// The default setting protects against data loss in the event of application failure (process death) but
	// leaves open a small window in which data could be lost in the event of complete system failure
	// (e.g. hardware, OS, or power). The default mode, SyncPolicy.NoneSync, writes data into operating system buffers
	// which will be written to the disks when those buffers are flushed by the operating system.
	// If the system fails, e.g. due power loss or crash, that data is lost before those buffers are flushed
	// to stable storage. This is prevented by the setting SyncPolicy.OSync, which forces the operating system
	// to flush to stable storage at every write. The effect of flushing each write is better durability,
	// however write throughput will suffer as each write will have to wait for the write to complete.
	strategy SyncPolicy
	interval time.Duration
}

var defaultSyncOptions = syncOptions{
	strategy: NoneSync,
	interval: 0 * time.Second,
}

type options struct {
	generalOptions
	expiryOptions
	compactionOptions
	syncOptions
}
