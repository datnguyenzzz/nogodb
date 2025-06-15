module github.com/datnguyenzzz/nogodb/lib/go-sstable

go 1.24.3

replace github.com/datnguyenzzz/nogodb/lib/go-blocked-bloom-filter => ../go-blocked-bloom-filter

require (
	github.com/datnguyenzzz/nogodb/lib/go-blocked-bloom-filter v0.0.0-00010101000000-000000000000
	github.com/datnguyenzzz/nogodb/lib/go-bytesbufferpool v0.0.0-20250609152930-352a93d7ed86
)

require (
	github.com/DataDog/zstd v1.5.7 // indirect
	github.com/golang/snappy v1.0.0 // indirect
)
