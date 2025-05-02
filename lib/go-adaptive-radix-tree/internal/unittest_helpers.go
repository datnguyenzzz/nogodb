package internal

import (
	"context"
	"crypto/rand"
	"fmt"

	"github.com/go-faker/faker/v4"
)

type actionType uint8

const (
	insertAction actionType = iota
	removeAction
)

type nodeAction[V any] struct {
	kind  actionType
	key   byte
	child *INode[V]
}

func randomByte() byte {
	randomByte := make([]byte, 1)

	// Read random data into the byte slice
	_, err := rand.Read(randomByte)
	if err != nil {
		fmt.Println("Error generating random byte:", err)
		return 0
	}

	return randomByte[0]
}

func randomBytes(num uint8) []byte {
	res := make([]byte, num)
	for i := uint8(0); i < num; i++ {
		res[i] = randomByte()
	}
	return res
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

func generateStringLeaves(sz int) []INode[string] {
	res := make([]INode[string], sz)

	for i := 0; i < sz; i++ {
		res[i] = newLeafWithKV[string](context.Background(), randomBytes(5), randomQuote())
	}

	return res
}
