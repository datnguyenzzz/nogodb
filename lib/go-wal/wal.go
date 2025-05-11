package go_wal

import "context"

func (W *WAL) Open(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (W *WAL) Close(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (W *WAL) Delete(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (W *WAL) Sync(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (W *WAL) Write(ctx context.Context, data []byte) (*Record, error) {
	//TODO implement me
	panic("implement me")
}

func (W *WAL) Read(ctx context.Context, r *Record) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

var _ IWal = (*WAL)(nil)
