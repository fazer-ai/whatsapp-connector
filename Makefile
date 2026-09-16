# Development entry points. `make check` is the whole of what CI enforces and needs both
# servers; `make check-offline` is the part that needs nothing running, and is what the
# git hooks and the agent stop hook fall back to.

GO ?= go
GOLANGCI_LINT ?= golangci-lint
PACKAGES ?= ./...

.DEFAULT_GOAL := help
.PHONY: help setup deps hooks fmt lint test test-postgres test-redis test-cover contract tidy check check-offline clean

help: ## List the available targets
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

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

# The variable is cleared rather than merely not set: it is inherited from whatever
# shell runs this, so a developer who exported it -- or `make check`, which passes its
# own environment to both passes -- would otherwise get PostgreSQL here and never run
# SQLite at all. Two passes that are the same pass is the one failure this whole
# arrangement cannot notice.
test: ## Run the test suite with the race detector, against SQLite
	WAC_TEST_DATABASE_URL= $(GO) test -race $(PACKAGES)

# The guard is a shell test and not $(error) so that `make -n check` prints this recipe
# instead of dying while expanding it. What that dry run shows is the promise itself: the
# whole of $(PACKAGES) and no -run. A pass narrowed to the package that holds SQL today
# would miss the defect this target exists to catch, which arrives through a helper three
# calls below a test that mentions no SQL at all.
#
# The port is not 5432 on purpose. It is an example, and an example that collides with
# something already listening is a first instruction that fails, which is how an explicit
# error becomes a target people route around.
test-postgres: ## Run the test suite against a PostgreSQL server (WAC_TEST_DATABASE_URL)
	@test -n "$(WAC_TEST_DATABASE_URL)" || { \
	  echo "WAC_TEST_DATABASE_URL is unset or empty. It names the server this pass runs against:"; \
	  echo "  docker run -d --rm -p 55432:5432 -e POSTGRES_USER=wac -e POSTGRES_PASSWORD=wac -e POSTGRES_DB=wac postgres:18-alpine"; \
	  echo "  WAC_TEST_DATABASE_URL=postgres://wac:wac@localhost:55432/wac?sslmode=disable make test-postgres"; \
	  echo "(any free port will do; 55432 only avoids whatever is already on 5432)"; \
	  exit 1; }
	$(GO) test -count=1 $(PACKAGES)

test-redis: ## Run the pass that needs a real Redis (WAC_TEST_REDIS_URL)
	@test -n "$(WAC_TEST_REDIS_URL)" || { \
	  echo "WAC_TEST_REDIS_URL is unset or empty. It names the server this pass runs against:"; \
	  echo "  docker run -d --rm -p 56379:6379 redis:8-alpine"; \
	  echo "  WAC_TEST_REDIS_URL=redis://localhost:56379/0 make test-redis"; \
	  echo "(any free port will do; 56379 only avoids whatever is already on 6379)"; \
	  exit 1; }
	$(GO) test -count=1 ./internal/transport/redisstream

test-cover: ## Run the test suite and write coverage.txt
	WAC_TEST_DATABASE_URL= $(GO) test -race -coverprofile=coverage.txt -covermode=atomic $(PACKAGES)

# The whole package, and not a -run of the tests whose names sounded like the contract:
# the filter that used to be here missed the RPC classification and both enum checks, so
# a fixture that broke one of them left this target green. A list of name fragments is a
# list somebody has to remember to extend, and the package is already under a second.
contract: ## Check the Go protocol binding against contract/
	$(GO) test ./internal/protocol

tidy: ## Fail when go.mod/go.sum are not tidy
	$(GO) mod tidy -diff

# Everything CI enforces, and it fails when it cannot run all of it.
#
# It used to depend on two conditional targets that printed their own absence and exited
# 0. The notice was true and useless: the exit code is what every script reads, and what
# it said was "CI will accept this" on evidence it had not collected. A single test file
# with no production lines, green under the race detector, aborted the whole package under
# PostgreSQL and passed here as merge-ready.
#
# The way out is a target rather than a variable, and that is the point: a variable can be
# exported from a shell profile, a direnv file or an agent's configuration and then never
# appear again in any command anybody typed or any round recorded. A target has to be
# named where it is run.
check: check-offline test-postgres test-redis ## Everything CI enforces; needs both servers (see check-offline)

# The half that needs nothing running, which is what the git hooks and the agent stop hook
# fall back to: requiring a server there would fail every commit made without one, for a
# reason that is not the commit's.
#
# `tidy` belongs here and was missing from `check` altogether: CI's lint job runs
# `go mod tidy -diff`, and an untidy go.sum passed `check` green with both servers up and
# nothing skipped. A target that promises everything has to be told when the list grows.
check-offline: lint tidy test ## Lint, tidy and the SQLite pass: everything that needs no server

clean: ## Remove build and coverage output
	rm -rf bin dist coverage.txt
