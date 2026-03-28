package prefixbytescodex

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/go-faker/faker/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Encoder_Handcrafted(t *testing.T) {
	input := [][]byte{
		{0xa, 0xa, 0xa, 0xa, 0xa, 0xa, 0xa},
		{0xa, 0xa, 0xa, 0xa, 0xa, 0xa, 0xb},
		{0xa, 0xa, 0xa, 0xa, 0xa, 0xb, 0xb},
		{0xa, 0xa, 0xa, 0xa, 0xa, 0xb, 0xa},
		{0xa, 0xa, 0xb, 0xb, 0xb, 0xc},
		{0xa, 0xa, 0xb, 0xb, 0xb, 0xc, 0xc},
		{0xa, 0xa, 0xb, 0xb, 0xb, 0xc, 0xd},
		{0xa, 0xa, 0xb, 0xb, 0xc, 0xe},
		{0xa, 0xa, 0xb, 0xb, 0xe},
		{0xa, 0xa, 0xb, 0xb, 0xe},
		{0xa, 0xa, 0xb, 0xb, 0xe, 0xf},
		{0xa, 0xa, 0xb, 0xb, 0xe, 0xe},
		{0xa, 0xa, 0xc, 0xd, 0xe},
		{0xa, 0xa, 0xc, 0xd, 0xf},
		{0xa, 0xa, 0xc, 0xd, 0xf},
		{0xa, 0xa, 0xc, 0xd, 0x10, 0x10},
	}

	expectedOut := []byte{
		0x4, /* bundle size */
		/* offset encoded */
		0x1,                                    /* type */
		0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, /* min value */
		0x0, 0x3, 0x5, 0x7, 0x9, 0xb, 0xd, 0xf, 0x12, 0x15, 0x17, 0x1a, 0x1a, 0x1a, 0x1b, 0x1c, 0x1e, 0x1f, 0x20, 0x21, 0x23, /* 21 offsets */
		/* compressed values */
		0xa, 0xa,
		0xa, 0xa, 0xa,
		0xa, 0xa,
		0xa, 0xb,
		0xb, 0xb,
		0xb, 0xa,
		0xb, 0xb,
		0xb, 0xc,
		0xb, 0xc, 0xc,
		0xb, 0xc, 0xd,
		0xc, 0xe,
		0xb, 0xb, 0xe,
		/* nil */
		/* nil */
		0xf,
		0xe,
		0xc, 0xd,
		0xe,
		0xf,
		0xf,
		0x10, 0x10,
	}

	enc := &PrefixBytesEncoder{
		bundleSize: 4,
	}

	enc.Init()
	for _, v := range input {
		enc.Append(v)
	}
	offset := uint32(0)
	buf := make([]byte, enc.Size(uint32(offset)))

	offset = enc.Finish(uint32(len(input)), uint32(offset), buf)
	buf = buf[:offset]

	assert.True(t, bytes.Equal(expectedOut, buf))
}

func Test_Codex(t *testing.T) {
	type param struct {
		desc   string
		unique bool
		size   int
	}

	testCases := []param{
		{
			desc:   "unique, small size",
			size:   20,
			unique: true,
		},
		{
			desc:   "unique, big size",
			size:   10_000,
			unique: true,
		},
		{
			desc: "not unique, small size",
			size: 20,
		},
		{
			desc: "not unique, big size",
			size: 10_000,
		},
	}

	enc := &PrefixBytesEncoder{
		bundleSize: 16,
	}
	enc.Init()

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			enc.Reset()
			input := generateSortedBytes(tc.size, tc.unique)
			for _, v := range input {
				enc.Append(v)
			}

			// encode
			offset := uint32(0)
			buf := make([]byte, enc.Size(uint32(offset)))

			offset = enc.Finish(uint32(len(input)), uint32(offset), buf)
			buf = buf[:offset]

			// decode
			offset = 0
			dec, offset := NewPrefixBytesDecoder(uint32(len(input)), offset, buf)

			for i := 0; i < tc.size; i++ {
				out := dec.Get(uint32(i))
				require.True(t, bytes.Equal(out, input[i]))
			}
		})
	}
}

// generateSortedBytes Generate list of []byte in an increasing order of key
func generateSortedBytes(size int, isUnique bool) [][]byte {
	res := make([][]byte, 0, size)

	// generate a list of key–value pairs such that adjacent keys share some common bytes.
	for i := range size {
		res = append(res, randomByte())
		if i == 0 {
			continue
		}

		if !isUnique {
			res[i] = generateKeyFromAnotherKey(res[i-1])
		}
	}

	sort.Slice(res, func(i, j int) bool {
		return bytes.Compare(res[i], res[j]) < 0
	})

	return res
}

// generateKeyFromAnotherKey generate a random key that share some first bytes of the given key
func generateKeyFromAnotherKey(key []byte) []byte {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	mutualLength := r.Intn(len(key)-1) + 1

	randomBytes := randomByte()

	newKey := make([]byte, mutualLength+len(randomBytes))
	copy(newKey, key)
	copy(newKey[mutualLength:], randomBytes)

	return newKey
}

func randomByte() []byte {
	quote := struct {
		FixedByteList []byte `faker:"slice_len=100"`
	}{}

	err := faker.FakeData(&quote)
	if err != nil {
		fmt.Println(err)
		return nil
	}

	return quote.FixedByteList
}
