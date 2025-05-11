package go_wal

import "context"

const (
	firstSegmentId SegmentID = 0
)

func openSegmentByPath(path string) (*Segment, error) {
	return nil, nil
}

func (s *Segment) Close(ctx context.Context) error {
	return nil
}
