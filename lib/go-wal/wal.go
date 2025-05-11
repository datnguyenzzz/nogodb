package go_wal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
)

func New(opts ...OptionFn) *WAL {
	wal := &WAL{
		opts:          defaultOptions,
		olderSegments: make(map[SegmentID]*Segment),
		syncCfg: syncCfg{
			closeCh: make(chan struct{}),
		},
	}

	for _, o := range opts {
		o(wal)
	}

	return wal
}

// Core functions \\

func (w *WAL) Open(ctx context.Context) error {
	// create new main directory if not exists
	if _, err := os.Stat(w.opts.dirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(w.opts.dirPath, os.ModePerm); err != nil {
			zap.L().Error("Failed to create dir", zap.String("dirPath", w.opts.dirPath), zap.Error(err))
			return err
		}
	}

	// loads all existing segments file
	segmentEntries, err := os.ReadDir(w.opts.dirPath)
	if err != nil {
		zap.L().Error("Failed to read dir", zap.String("dirPath", w.opts.dirPath), zap.Error(err))
		return err
	}

	var segmentIDs []SegmentID
	for _, entry := range segmentEntries {
		// segment file should not be a directory
		if entry.IsDir() {
			continue
		}
		// segment file has name of format "id.<opts.fileExt>"
		var id SegmentID
		_, err := fmt.Sscanf(entry.Name(), "%d"+w.opts.fileExt, &id)
		if err != nil {
			zap.L().Warn("Failed to parse fileExt", zap.String("fileExt", w.opts.fileExt))
			continue
		}

		segmentIDs = append(segmentIDs, id)
	}

	// attempt to open all existing segment files, or open a new one if none exists
	if len(segmentIDs) == 0 {
		newSegmentFile := getSegmentFilePath(w.opts.dirPath, w.opts.fileExt, firstSegmentId)
		segment, err := openSegmentByPath(newSegmentFile)
		if err != nil {
			zap.L().Error("Failed to open segment", zap.String("segmentFilePath", newSegmentFile), zap.Error(err))
			return err
		}
		w.activeSegment = segment
	} else {
		var latestSegmentId SegmentID = 0
		for _, id := range segmentIDs {
			latestSegmentId = max(latestSegmentId, id)
		}

		for _, id := range segmentIDs {
			segmentFile := getSegmentFilePath(w.opts.dirPath, w.opts.fileExt, id)
			segment, err := openSegmentByPath(segmentFile)
			if err != nil {
				zap.L().Error("Failed to open segment", zap.String("segmentFilePath", segmentFile), zap.Error(err))
				return err
			}

			if id == latestSegmentId {
				w.activeSegment = segment
			} else {
				w.olderSegments[segment.Id] = segment
			}
		}
	}

	// Init the background job to periodically sync the files
	if w.opts.syncInterval > 0 {
		w.syncCfg.ticker = time.NewTicker(w.opts.syncInterval)
		newCtx := context.WithoutCancel(ctx)
		go func(ctx context.Context) {
			for {
				select {
				case <-ctx.Done():
					zap.L().Info("Context canceled")
					return
				case <-w.closeCh:
					w.syncCfg.ticker.Stop()
					return
				case <-w.syncCfg.ticker.C:
					err := w.Sync(ctx)
					if err != nil {
						zap.L().Warn("Failed to sync file", zap.Error(err))
					}
				}
			}
		}(newCtx)
	}

	return nil
}

func (w *WAL) Close(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (w *WAL) Delete(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (w *WAL) Sync(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (w *WAL) Write(ctx context.Context, data []byte) (*Record, error) {
	//TODO implement me
	panic("implement me")
}

func (w *WAL) Read(ctx context.Context, r *Record) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

// Iterator \\

func (w *WAL) Next() (*Record, []byte, error) {
	//TODO implement me
	panic("implement me")
}

func getSegmentFilePath(dirPath, ext string, segmentID SegmentID) string {
	return filepath.Join(dirPath, fmt.Sprintf("%d%s", segmentID, ext))
}

var _ IWal = (*WAL)(nil)
