package go_wal

func estimateNeededSpaces(data []byte) int {
	// estimateNeededSpaces = len(data) + number_of_header * headerSize
	// number_of_header = [len(data)/defaultBlockSize] Middle Blocks + First + Last
	if len(data) <= defaultBlockSize {
		return len(data) + headerSize
	}
	return len(data) + (len(data)/defaultBlockSize+2)*headerSize
}
