module github.com/datnguyenzzz/nogodb/tree/master/db

go 1.26.0

replace github.com/datnguyenzzz/nogodb/lib/go-block-cache => ../lib/go-block-cache

replace github.com/datnguyenzzz/nogodb/lib/go-fs => ../lib/go-fs

replace github.com/datnguyenzzz/nogodb/lib/common => ../lib/common

require (
	github.com/datnguyenzzz/nogodb/lib/common v0.0.0-00010101000000-000000000000
	github.com/datnguyenzzz/nogodb/lib/go-block-cache v0.0.0-00010101000000-000000000000
	github.com/datnguyenzzz/nogodb/lib/go-fs v0.0.0-00010101000000-000000000000
	golang.org/x/sync v0.19.0
)

require (
	github.com/twmb/murmur3 v1.1.8 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
)
