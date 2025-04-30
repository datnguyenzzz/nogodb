package internal

func findLCP(key1 []byte, key2 []byte, offset uint8) uint8 {
	var i = offset
	for ; int(i) < min(len(key1), len(key2)); i++ {
		if key1[i] != key2[i] {
			break
		}
	}

	return i - offset
}
