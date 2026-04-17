fmt:
	@gofumpt -l -w .

functional-test:
	- echo "functional tests for lib/go-sstable"
	- cd lib/go-sstable/functional && go test -v -timeout=30m -tags=functional_tests ./...