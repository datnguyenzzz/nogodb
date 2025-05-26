package functional

import "crypto/rand"

const (
	CommonDirPath = "./wal"
)

func generateBytes(n int) []byte {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		return nil
	}
	return b
}
