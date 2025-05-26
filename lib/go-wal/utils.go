package go_wal

import (
	"fmt"
	"path/filepath"
)

func getPageFilePath(dirPath, ext string, pageID PageID) string {
	return filepath.Join(dirPath, fmt.Sprintf("%d%s", pageID, ext))
}

func estimateNeededSpaces(data []byte) int {
	// estimateNeededSpaces = len(data) + number_of_header * headerSize
	// number_of_header = [len(data)/defaultBlockSize] Middle Blocks + First + Last
	if len(data) <= defaultBlockSize {
		return len(data) + headerSize
	}
	return len(data) + (len(data)/defaultBlockSize+2)*headerSize
}
