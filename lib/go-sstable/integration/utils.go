package integration

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/go-faker/faker/v4"
)

type kvType struct {
	key   []byte
	value []byte
}

func generateBytes(lo, hi []byte) []byte {
	if len(lo) == 0 {
		return []byte{0}
	}
	res := make([]byte, len(lo))
	copy(res, lo)

	for i := len(res) - 1; i >= 0; i-- {
		if res[i] < byte(255) {
			res[i] += 1
			for j := i + 1; j < len(res); j++ {
				res[j] = 0
			}
			break
		}
	}
	return res
}

// generateKV Generate list of kvType in an increasing order of key
func generateKV(size int, isUnique bool) []kvType {
	res := make([]kvType, 0, size)

	// generate a list of key–value pairs such that adjacent keys share some common bytes.
	for i := 0; i < size; i++ {
		res = append(res, kvType{[]byte(randomQuote()), []byte(randomQuote())})
		if i == 0 {
			continue
		}

		if !isUnique {
			res[i].key = generateKeyFromAnotherKey(res[i-1].key)
		}
	}

	sort.Slice(res, func(i, j int) bool {
		return bytes.Compare(res[i].key, res[j].key) < 0
	})

	return res
}

// generateKeyFromAnotherKey generate a random key that share some first bytes of the given key
func generateKeyFromAnotherKey(key []byte) []byte {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	mutualLength := r.Intn(len(key)-1) + 1

	randomBytes := []byte(randomQuote())

	newKey := make([]byte, mutualLength+len(randomBytes))
	copy(newKey, key)
	copy(newKey[mutualLength:], randomBytes)

	return newKey
}

func randomQuote() string {
	quote := struct {
		Sentence string `faker:"sentence"`
	}{}

	err := faker.FakeData(&quote)
	if err != nil {
		fmt.Println(err)
		return ""
	}

	return quote.Sentence
}
