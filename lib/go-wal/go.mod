module github.com/datnguyenzzz/nogodb/lib/go-wal

go 1.24.3

replace github.com/datnguyenzzz/nogodb/lib/go-bytesbufferpool => ../go-bytesbufferpool

require (
	github.com/datnguyenzzz/nogodb/lib/go-bytesbufferpool v0.0.0-00010101000000-000000000000
	go.uber.org/zap v1.27.0
)

require go.uber.org/multierr v1.11.0 // indirect
