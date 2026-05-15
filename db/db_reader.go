package db

import (
	"context"
	"io"
)

func (d *DB) Close(ctx context.Context) error {
	panic("unimplemented")
}

func (d *DB) Get(ctx context.Context, key []byte) (value []byte, closer io.Closer, err error) {
	panic("unimplemented")
}
