package go_wal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
)

func New(opts ...OptionFn) *WAL {
	wal := &WAL{
		opts:       defaultOptions,
		olderPages: make(map[PageID]*Page),
		syncCfg: syncCfg{
			closeCh: make(chan struct{}),
		},
		mu: sync.RWMutex{},
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

	// loads all existing pages file
	pageEntries, err := os.ReadDir(w.opts.dirPath)
	if err != nil {
		zap.L().Error("Failed to read dir", zap.String("dirPath", w.opts.dirPath), zap.Error(err))
		return err
	}

	var pageIDs []PageID
	for _, entry := range pageEntries {
		// page file should not be a directory
		if entry.IsDir() {
			continue
		}
		// page file has name of format "id.<opts.fileExt>"
		var id PageID
		_, err := fmt.Sscanf(entry.Name(), "%d"+w.opts.fileExt, &id)
		if err != nil {
			zap.L().Warn("Failed to parse fileExt", zap.String("fileExt", w.opts.fileExt))
			continue
		}

		pageIDs = append(pageIDs, id)
	}

	// attempt to open all existing page files, or open a new one if none exists
	if len(pageIDs) == 0 {
		newPageFile := getPageFilePath(w.opts.dirPath, w.opts.fileExt, firstPageId)
		page, err := openPageByPath(newPageFile, firstPageId, PageAccessModeReadWrite)
		if err != nil {
			zap.L().Error("Failed to open page", zap.String("pageFilePath", newPageFile), zap.Error(err))
			return err
		}
		w.activePage = page
	} else {
		var latestPageId PageID = 0
		for _, id := range pageIDs {
			latestPageId = max(latestPageId, id)
		}

		for _, id := range pageIDs {

			mode := PageAccessModeReadOnly
			if id != latestPageId {
				// for an active page, we can read and write
				mode = PageAccessModeReadWrite
				if w.opts.sync {
					mode = PageAccessModeReadWriteSync
				}
			}

			pageFile := getPageFilePath(w.opts.dirPath, w.opts.fileExt, id)
			page, err := openPageByPath(pageFile, id, mode)
			if err != nil {
				zap.L().Error("Failed to open page", zap.String("pageFilePath", pageFile), zap.Error(err))
				return err
			}

			if id == latestPageId {
				w.activePage = page
			} else {
				w.olderPages[page.Id] = page
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
					if err := w.Sync(ctx); err != nil {
						zap.L().Warn("Failed to sync file to the stable storage", zap.Error(err))
					}
				}
			}
		}(newCtx)
	}

	return nil
}

func (w *WAL) Close(ctx context.Context) error {
	// attempt to lock to avoid data racing, ie close during Write(), ...
	w.mu.Lock()
	defer w.mu.Unlock()

	// send a signal to stop a background job for sync-ing files to the stable storage
	w.closeCh <- struct{}{}

	// close all pages file that are in-open
	for id, page := range w.olderPages {
		if err := page.Close(ctx); err != nil {
			zap.L().Error("Failed to close page", zap.String("pageId", strconv.Itoa(int(id))), zap.Error(err))
			return err
		}
	}

	w.olderPages = nil
	err := w.activePage.Close(ctx)
	if err != nil {
		zap.L().Error("Failed to close page", zap.String("pageId", strconv.Itoa(int(w.activePage.Id))), zap.Error(err))
		return err
	}
	w.activePage = nil

	close(w.closeCh)
	return nil
}

func (w *WAL) Write(ctx context.Context, data []byte) (*Record, error) {
	//TODO implement me
	panic("implement me")
}

func (w *WAL) Read(ctx context.Context, r *Record) ([]byte, error) {
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

// Iterator \\

func (w *WAL) Next() (*Record, []byte, error) {
	//TODO implement me
	panic("implement me")
}

func getPageFilePath(dirPath, ext string, pageID PageID) string {
	return filepath.Join(dirPath, fmt.Sprintf("%d%s", pageID, ext))
}

var _ IWal = (*WAL)(nil)
