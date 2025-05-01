package internal

import "bytes"

func findLCP(key1 []byte, key2 []byte, offset uint8) uint8 {
	var i = offset
	for ; int(i) < min(len(key1), len(key2)); i++ {
		if key1[i] != key2[i] {
			break
		}
	}

	return i - offset
}

func isExactMatch(key1 []byte, key2 []byte) bool {
	return len(key1) == len(key2) && bytes.Equal(key1, key2)
}
