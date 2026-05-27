COMPOSE ?= docker compose
POSTGRES_SERVICE ?= postgres
POSTGRES_DB ?= gotaskqueue
POSTGRES_USER ?= gotaskqueue
CACHE_DIR ?= /tmp/gotaskqueue-cache
GO_ENV ?= GOCACHE=$(CACHE_DIR)/go-build
LINT_ENV ?= GOCACHE=$(CACHE_DIR)/go-build GOLANGCI_LINT_CACHE=$(CACHE_DIR)/golangci-lint

.PHONY: up down migrate-up run test vet lint check verify integration-test

up:
	$(COMPOSE) up -d redis postgres prometheus

down:
	$(COMPOSE) down

migrate-up:
	COMPOSE="$(COMPOSE)" POSTGRES_SERVICE="$(POSTGRES_SERVICE)" POSTGRES_DB="$(POSTGRES_DB)" POSTGRES_USER="$(POSTGRES_USER)" bash scripts/migrate-up.sh

run:
	$(GO_ENV) go run ./cmd/gotaskqueue

test:
	$(GO_ENV) go test ./...

vet:
	$(GO_ENV) go vet ./...

lint:
	$(LINT_ENV) golangci-lint run ./...

check: test vet lint

verify: check

integration-test: migrate-up
	$(GO_ENV) go test -tags=integration ./internal/integration
