package go_sstable

type Writer struct {
	opts *writeOpt
	rw   *rawWriter
}

func (w Writer) Write(key, value []byte) error {
	//TODO implement me
	panic("implement me")
}

func (w Writer) Close() error {
	//TODO implement me
	panic("implement me")
}

func NewWriter(opts ...WriteOptFn) *Writer {
	w := &Writer{}

	for _, o := range opts {
		o(w)
	}

	return w
}

var _ IWriter = (*Writer)(nil)
