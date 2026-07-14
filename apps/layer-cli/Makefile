.PHONY: build install clean test vet lint help

BINARY := layer
BINDIR := $(shell go env GOBIN)
ifeq ($(BINDIR),)
BINDIR := $(shell go env GOPATH)/bin
endif
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -X main.version=$(VERSION)

build:
	go build -ldflags '$(LDFLAGS)' -o $(BINARY) .

install:
	go build -ldflags '$(LDFLAGS)' -o $(BINDIR)/$(BINARY) .

test:
	go test ./... -race -count=1
	go test -C ../../clients/go ./... -race -count=1

vet:
	go vet ./...
	go vet -C ../../clients/go ./...

lint:
	golangci-lint run ./...

clean:
	rm -f $(BINARY)

help:
	@echo "Available targets:"
	@echo "  build      - Build the layer binary"
	@echo "  install    - Install to GOPATH/bin"
	@echo "  test       - Run unit tests with race detector (CLI + Go client)"
	@echo "  vet        - Run go vet (CLI + Go client)"
	@echo "  lint       - Run golangci-lint"
	@echo "  clean      - Remove build artifacts"
