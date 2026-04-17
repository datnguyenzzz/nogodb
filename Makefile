fmt:
	@gofumpt -l -w .

functional-test:
	- go clean -cache

	- echo "functional tests for lib/go-wal"
	- cd lib/go-wal/functional && go test -v -timeout=30m -tags=functional_tests ./...

	- echo "functional tests for lib/go-sstable"
	- cd lib/go-sstable/functional && go test -v -timeout=30m -tags=functional_tests ./...