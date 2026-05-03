GOLANGCI_LINT_VERSION := 2.9.0

golangci-lint: install-golangci-lint
	@for dir in $$(find lib -maxdepth 2 -name go.mod -exec dirname {} \;); do \
		echo "Linting $$dir..."; \
		(cd $$dir && golangci-lint run --config $(CURDIR)/.golangci.yaml) || exit 1; \
	done

install-golangci-lint:
	which golangci-lint && (golangci-lint --version | grep -q $(GOLANGCI_LINT_VERSION)) || curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(shell go env GOPATH)/bin v$(GOLANGCI_LINT_VERSION)

fmt:
	@gofumpt -l -w .

functional-test:
	- go clean -cache

	- echo "functional tests for lib/go-wal"
	- cd lib/go-wal/functional && go test -v -timeout=30m -tags=functional_tests ./...

	- echo "functional tests for lib/go-sstable"
	- cd lib/go-sstable/functional && go test -v -timeout=30m -tags=functional_tests ./...