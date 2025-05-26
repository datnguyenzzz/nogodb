module github.com/datnguyenzzz/nogodb/lib/go-wal

go 1.24.3

replace github.com/datnguyenzzz/nogodb/lib/go-bytesbufferpool => ../go-bytesbufferpool

require (
	github.com/datnguyenzzz/nogodb/lib/go-bytesbufferpool v0.0.0-00010101000000-000000000000
	github.com/stretchr/testify v1.8.1
	go.uber.org/zap v1.27.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
