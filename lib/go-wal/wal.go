package go_wal

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
)

// TODO refactor to use the shared go-fs instead of using os.FS

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
		page, err := w.openPage(firstPageId, PageAccessModeReadWrite)
		if err != nil {
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
			page, err := w.openPage(id, mode)
			if err != nil {
				return err
			}

			if id == latestPageId {
				w.activePage = page
			} else {
				w.olderPages[page.Id] = page
			}
		}
	}

	// Init the background job to periodically sync the files to the stable storage
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

	select {
	case <-w.closeCh:
	default:
		close(w.closeCh)
	}

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

	return nil
}

// Write the data to the OS buffer, and return Position of where the data is written
func (w *WAL) Write(ctx context.Context, data []byte) (*Position, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if int64(len(data))+headerSize > w.opts.pageSize {
		zap.L().Error(fmt.Sprintf("Data size too big to fit into a page, %d > %d", len(data), w.opts.pageSize))
		return nil, ErrDataTooLarge
	}

	// if the active page doesn't have enough space to hold the data
	if w.activePage.Size()+int64(estimateNeededSpaces(data)) > w.opts.pageSize {
		// TODO sync the current active page, move it to immutable, and create new one
		if err := w.activePage.Sync(ctx); err != nil {
			zap.L().Error("Failed to sync file to the stable storage", zap.Error(err))
			return nil, err
		}

		// open a new mutable file and move the current active pages to immutable
		mode := PageAccessModeReadWrite
		if w.opts.sync {
			mode = PageAccessModeReadWriteSync
		}
		newPage, err := w.openPage(w.activePage.Id+1, mode)
		if err != nil {
			return nil, err
		}

		currActivePageId := w.activePage.Id
		if err := w.activePage.Close(ctx); err != nil {
			return nil, err
		}

		oldPage, err := w.openPage(currActivePageId, PageAccessModeReadOnly)
		if err != nil {
			return nil, err
		}
		w.olderPages[w.activePage.Id] = oldPage
		w.activePage = newPage
	}

	pos, size, err := w.activePage.Write(ctx, data)
	if err != nil {
		zap.L().Error("Failed to write data to page", zap.Error(err))
		return nil, err
	}

	w.notSyncBytes += size
	needSync := w.opts.sync && w.notSyncBytes > int64(w.opts.bytesPerSync)
	if needSync {
		if err := w.Sync(ctx); err != nil {
			zap.L().Error("Failed to sync file to the stable storage", zap.Error(err))
			return nil, err
		}
	}

	return pos, nil
}

func (w *WAL) Get(ctx context.Context, r *Position) ([]byte, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	pid := r.PageId
	var page *Page
	if pid == w.activePage.Id {
		page = w.activePage
	} else {
		page = w.olderPages[pid]
	}

	if page == nil {
		zap.L().Error(fmt.Sprintf("Page not found for page id %d", pid))
		return nil, ErrPageNotFound
	}

	data, _, err := page.Read(ctx, r)
	return data, err
}

func (w *WAL) Delete(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, page := range w.olderPages {
		if err := page.Delete(ctx); err != nil {
			return err
		}
	}

	w.olderPages = nil
	return w.activePage.Delete(ctx)
}

func (w *WAL) Sync(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.activePage.Sync(ctx); err != nil {
		return err
	}
	w.notSyncBytes = 0
	return nil
}

func (w *WAL) openPage(id PageID, mode PageAccessMode) (*Page, error) {
	pageFile := getPageFilePath(w.opts.dirPath, w.opts.fileExt, id)
	return openPageByPath(pageFile, id, mode)
}

var _ IWal = (*WAL)(nil)

// Iterator \\

func (w *WAL) NewIterator(ctx context.Context) *WalIterator {
	w.mu.RLock()
	defer w.mu.RUnlock()
	// As data always written in the asc sorted order of pageID
	pageIter := map[PageID]*PageIterator{}
	for _, page := range w.olderPages {
		pageIter[page.Id] = page.NewIterator(ctx)
	}

	pageIter[w.activePage.Id] = w.activePage.NewIterator(ctx)

	return &WalIterator{
		pageIter:      pageIter,
		currentPageId: 0,
	}
}

func (i *WalIterator) Next(ctx context.Context) ([]byte, *Position, error) {
	if int(i.currentPageId) >= len(i.pageIter) {
		return nil, nil, io.EOF
	}

	data, pos, err := i.pageIter[i.currentPageId].Next(ctx)
	if err == io.EOF {
		i.currentPageId++
		return i.Next(ctx)
	}

	return data, pos, err
}

var _ IIterator = (*WalIterator)(nil)
