#!/usr/bin/make -f

# Common variables
REPO ?= $(shell go list -m)
VERSION ?= $(shell git describe --tags --always 2>/dev/null || cat VERSION 2>/dev/null || echo "develop")
BINDIR = bin
DIRS = $(BINDIR)
BINS = "$(BINDIR)/neofs-sftp-gw"

.PHONY: help all dep clean format test cover lint

# Make all binaries
all: $(BINS)

$(BINS): $(BINDIR) dep
	@echo "⇒ Build $@"
	CGO_ENABLED=0 \
	go build -v -trimpath \
	-ldflags "-X $(REPO)/internal/version.Version=$(VERSION)" \
	-o $@ ./

$(BINDIR):
	@echo "⇒ Ensure dir: $@"
	@mkdir -p $@

# Pull go dependencies
dep:
	@printf "⇒ Download requirements: "
	@CGO_ENABLED=0 \
	go mod download && echo OK
	@printf "⇒ Tidy requirements: "
	@CGO_ENABLED=0 \
	go mod tidy -v && echo OK

# Run tests
test:
	@go test ./... -cover

# Run tests with race detection and produce coverage output
cover:
	@go test -v -race ./... -coverprofile=coverage.txt -covermode=atomic
	@go tool cover -html=coverage.txt -o coverage.html

# Reformat code
format:
	@echo "⇒ Processing gofmt check"
	@gofmt -s -w ./
	@echo "⇒ Processing goimports check"
	@goimports -w ./

# Run linters
lint:
	@golangci-lint --timeout=5m run


# Show current version
version:
	@echo $(VERSION)

# Show this help prompt
help:
	@echo '  Usage:'
	@echo ''
	@echo '    make <target>'
	@echo ''
	@echo '  Targets:'
	@echo ''
	@awk '/^#/{ comment = substr($$0,3) } comment && /^[a-zA-Z][a-zA-Z0-9_-]+ ?:/{ print "   ", $$1, comment }' $(MAKEFILE_LIST) | column -t -s ':' | grep -v 'IGNORE' | sort -u

# Clean up
clean:
	rm -rf $(BINDIR)

