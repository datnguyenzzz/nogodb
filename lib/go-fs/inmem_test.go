package go_fs

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

func Test_Create_And_Open(t *testing.T) {
	writeFunc := func(writable Writable, b []byte) error {
		n, err := writable.Write(b)
		assert.NoError(t, err)
		assert.Equal(t, len(b), n, "Can not write fully")
		return err
	}

	type param struct {
		name             string
		fileCountPerType map[ObjectType]int
		fileSize         int
		async            bool
	}

	dummyByte := []byte{0x3A, 0x29}

	cases := []param{
		{
			name:  "async",
			async: true,
			fileCountPerType: map[ObjectType]int{
				TypeManifest: 1,
				TypeTable:    3,
				TypeWAL:      2,
			},
			fileSize: 5,
		},
		{
			name:  "sync",
			async: true,
			fileCountPerType: map[ObjectType]int{
				TypeManifest: 1,
				TypeTable:    3,
				TypeWAL:      2,
			},
			fileSize: 5,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			storage := NewInmemStorage()
			writers := make(map[FileDesc]Writable)
			// create files
			for fileType, num := range tc.fileCountPerType {
				for i := 0; i < num; i++ {
					writer, fd, err := storage.Create(fileType, int64(i))
					assert.NoError(t, err, "can not create file")
					writers[fd] = writer
				}
			}

			// write to file
			eg := errgroup.Group{}
			if tc.async {
				eg.SetLimit(10)
			} else {
				eg.SetLimit(1)
			}

			for fileType, num := range tc.fileCountPerType {
				for i := 0; i < num; i++ {
					fd, err := storage.LookUp(fileType, int64(i))
					assert.NoError(t, err, "can not look up file")
					eg.Go(func() error {
						writer, ok := writers[fd]
						assert.True(t, ok, fmt.Sprintf("can not find writer for %#v", fd))
						err := writeFunc(writer, bytes.Repeat(dummyByte, tc.fileSize))
						assert.NoError(t, err)
						if err != nil {
							return err
						}

						err = writer.Close()
						assert.NoError(t, err, "can not close writer")
						if err != nil {
							return err
						}

						return nil
					})
				}
			}

			err := eg.Wait()
			assert.NoError(t, err)

			// assert file content
			for fileType, num := range tc.fileCountPerType {
				for i := 0; i < num; i++ {
					reader, fd, err := storage.Open(fileType, int64(i), 0)
					assert.NoError(t, err, "can not open file")
					_, ok := writers[fd]
					assert.True(t, ok, fmt.Sprintf("can not find writer for %#v", fd))

					var buf bytes.Buffer
					_, err = buf.ReadFrom(reader)
					assert.NoError(t, err)

					expectedBytes := bytes.Repeat(dummyByte, tc.fileSize)
					assert.Equal(t, expectedBytes, buf.Bytes())
				}
			}
		})
	}
}
