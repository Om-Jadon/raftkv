.PHONY: build test race lint vet cover chaos proto help

GO ?= go
SEEDS ?= 200
PACKAGES := ./...
PROTOC ?= protoc

help:
	@echo "Targets: build test race lint vet cover chaos proto"

build:
	$(GO) build $(PACKAGES)

test:
	$(GO) test $(PACKAGES)

race:
	$(GO) test -race -count=1 $(PACKAGES)

vet:
	$(GO) vet $(PACKAGES)

lint:
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not installed; skipping lint (install: https://golangci-lint.run/welcome/install/)"; \
	fi

cover:
	$(GO) test -coverprofile=coverage.out $(PACKAGES)
	$(GO) tool cover -func=coverage.out

proto:
	$(PROTOC) --go_out=. --go_opt=module=github.com/Om-Jadon/raftkv \
		--go-grpc_out=. --go-grpc_opt=module=github.com/Om-Jadon/raftkv \
		proto/raft.proto

# Chaos suite lands in Phase 7; placeholder keeps the target stable.
chaos:
	@echo "chaos suite not implemented yet (Phase 7); SEEDS=$(SEEDS)"
	$(GO) test -count=1 $(PACKAGES)
