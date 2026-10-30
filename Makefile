GO ?= go
BIN_DIR ?= bin
BIN := $(BIN_DIR)/mptcp-proxy

.PHONY: all proxy test test-race vet clean

all: proxy

proxy: $(BIN)

$(BIN): $(shell find cmd internal -name '*.go') go.mod
	$(GO) build -trimpath -o $@ ./cmd/mptcp-proxy

test:
	$(GO) test ./... -count=1

test-race:
	$(GO) test -race ./... -count=1

vet:
	$(GO) vet ./...

clean:
	rm -rf $(BIN_DIR)
