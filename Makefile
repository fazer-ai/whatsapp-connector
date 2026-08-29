# Development entry points. `make check` is what CI runs and what the git hooks
# and the agent stop hook fall back to.

GO ?= go
GOLANGCI_LINT ?= golangci-lint
PACKAGES ?= ./...

.DEFAULT_GOAL := help
.PHONY: help setup deps hooks fmt lint test test-postgres test-cover contract tidy check check-postgres clean

help: ## List the available targets
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

setup: hooks deps ## Prepare a fresh clone for development

deps: ## Download the module dependencies
	$(GO) mod download

hooks: ## Point git at the versioned hooks in .githooks
	git config core.hooksPath .githooks
	@echo "git hooks installed (core.hooksPath=.githooks)"

fmt: ## Format the code
	$(GOLANGCI_LINT) fmt

lint: ## Run the linters (formatting included)
	$(GOLANGCI_LINT) run

test: ## Run the test suite with the race detector
	$(GO) test -race $(PACKAGES)

test-postgres: ## Run the test suite against a PostgreSQL server (WAC_TEST_DATABASE_URL)
ifndef WAC_TEST_DATABASE_URL
	$(error WAC_TEST_DATABASE_URL is unset. It names the server this pass runs against, e.g. \
	  docker run -d --rm -p 5432:5432 -e POSTGRES_USER=wac -e POSTGRES_PASSWORD=wac -e POSTGRES_DB=wac postgres:18-alpine \
	  then WAC_TEST_DATABASE_URL=postgres://wac:wac@localhost:5432/wac?sslmode=disable make test-postgres)
endif
	$(GO) test -count=1 $(PACKAGES)

test-cover: ## Run the test suite and write coverage.txt
	$(GO) test -race -coverprofile=coverage.txt -covermode=atomic $(PACKAGES)

contract: ## Check the Go protocol binding against contract/
	$(GO) test ./internal/protocol -run 'Contract|Fixture|Type|Version|ErrorCodes'

tidy: ## Fail when go.mod/go.sum are not tidy
	$(GO) mod tidy -diff

check: lint test check-postgres ## Everything CI enforces

# The dialect pass, when there is a server to run it against. Not a hard dependency:
# `check` is what the git hooks and the stop hook fall back to, so requiring a running
# PostgreSQL would fail every commit made without one, for a reason that is not the
# commit's. Conditional, it runs for anyone who has a server and prints its own absence
# for everyone else, which is the part `make test` alone never says out loud.
check-postgres:
ifdef WAC_TEST_DATABASE_URL
	$(GO) test -count=1 $(PACKAGES)
else
	@echo "skipped the PostgreSQL pass: WAC_TEST_DATABASE_URL is unset (see 'make test-postgres')"
endif

clean: ## Remove build and coverage output
	rm -rf bin dist coverage.txt
