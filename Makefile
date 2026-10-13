#########################
# Testing
#########################
.PHONY: test
test:
	go test -race -count=1 ./...

.PHONY: test-cov
test-cov:
	@mkdir -p bin
	go test -race -count=1 -coverprofile=bin/coverage.out ./...
	go tool cover -html=bin/coverage.out -o bin/coverage.html
	@echo "Coverage report saved to bin/coverage.html"


########################
# Lint
########################
.PHONY: lint
lint:
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0 run ./... --config .golangci.yml

#########################
# Vulnerability check
#########################
.PHONY: vuln
vuln:
	go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...
