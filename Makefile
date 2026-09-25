# Development entry points. `make check` is the whole of what CI enforces and needs both
# servers; `make check-offline` is the part that needs nothing running, and is what the
# agent stop hook falls back to. The versioned pre-commit hook runs neither: it does gofmt
# on the staged files, `go vet` and the contract test, and is meant to stay under a second.

GO ?= go
GOLANGCI_LINT ?= golangci-lint
PACKAGES ?= ./...

# The passes that need a server, and the variable that names each one. One list: the
# dependency list of `check` and its preflight are both generated from it.
#
# Defined here, above every rule, because make expands a rule's prerequisites when it reads
# the rule. Defined further down, this arrives empty and `check` quietly loses both passes
# -- which is this issue's own defect, produced by its fix, and caught by `make -n check`
# printing a recipe with one `go test` in it instead of three.
#
# `_RUN` and `_URL` are here and not in each recipe for the same reason as `_VAR`: the
# preflight has to name the server that is actually missing. Spelled per recipe, a shell
# with PostgreSQL up and no Redis was told to start a PostgreSQL, and an instruction whose
# first line is already done is one people stop reading.
SERVER_PASSES := test-postgres test-redis
test-postgres_VAR := WAC_TEST_DATABASE_URL
test-postgres_RUN := docker run -d --rm -p 55432:5432 -e POSTGRES_USER=wac -e POSTGRES_PASSWORD=wac -e POSTGRES_DB=wac postgres:18-alpine
test-postgres_URL := postgres://wac:wac@localhost:55432/wac?sslmode=disable
test-redis_VAR := WAC_TEST_REDIS_URL
test-redis_RUN := docker run -d --rm -p 56379:6379 redis:8-alpine
test-redis_URL := redis://localhost:56379/0

.DEFAULT_GOAL := help
.PHONY: help setup deps hooks fmt lint test test-postgres test-redis test-cover contract tidy check check-offline check-servers offline-passes bench-fleet clean

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

# Both variables are cleared rather than merely not set: they are inherited from whatever
# shell runs this, so a developer who exported one -- or `make check`, which passes its own
# environment to every pass and now requires both to be set -- would otherwise get
# PostgreSQL and a real Redis here and never run the doubles at all. Two passes that are
# the same pass is the one failure this whole arrangement cannot notice.
#
# The Redis half of that was missed when `check` stopped being optional, and it reaches
# further than the duplicated pass: `check-offline` promises to need nothing running, and
# with an exported WAC_TEST_REDIS_URL and no server it went looking for one. The stop hook
# falls back to that target, so the promise is what keeps an agent able to stop.
test: ## Run the test suite with the race detector, against SQLite and the doubles
	WAC_TEST_DATABASE_URL= WAC_TEST_REDIS_URL= $(GO) test -race $(PACKAGES)

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
	  echo "$(test-postgres_VAR) is unset or empty. It names the server this pass runs against:"; \
	  echo "  $(test-postgres_RUN)"; \
	  echo "  $(test-postgres_VAR)=$(test-postgres_URL) make test-postgres"; \
	  echo "(any free port will do; 55432 only avoids whatever is already on 5432)"; \
	  exit 1; }
	$(GO) test -count=1 $(PACKAGES)

test-redis: ## Run the passes that need a real Redis (WAC_TEST_REDIS_URL)
	@test -n "$(WAC_TEST_REDIS_URL)" || { \
	  echo "$(test-redis_VAR) is unset or empty. It names the server this pass runs against:"; \
	  echo "  $(test-redis_RUN)"; \
	  echo "  $(test-redis_VAR)=$(test-redis_URL) make test-redis"; \
	  echo "(any free port will do; 56379 only avoids whatever is already on 6379)"; \
	  exit 1; }
	$(GO) test -count=1 ./internal/transport/redisstream ./internal/cluster

test-cover: ## Run the test suite and write coverage.txt
	WAC_TEST_DATABASE_URL= WAC_TEST_REDIS_URL= $(GO) test -race -coverprofile=coverage.txt -covermode=atomic $(PACKAGES)

# The whole package, and not a -run of the tests whose names sounded like the contract:
# the filter that used to be here missed the RPC classification and both enum checks, so
# a fixture that broke one of them left this target green. A list of name fragments is a
# list somebody has to remember to extend, and the package is already under a second.
contract: ## Check the Go protocol binding against contract/
	$(GO) test ./internal/protocol

tidy: ## Fail when go.mod/go.sum are not tidy
	$(GO) mod tidy -diff

# Two real processes against a real fleet, under load, with ownership forced to move.
#
# Deliberately outside `check`, and the reason is in `internal/toolchain` rather than only
# here: it builds a binary, starts processes and waits on real clocks, which is minutes
# rather than seconds, and `check` runs on every change. A comment is not an assertion, so
# the exemption is written where the suite reads it and fails when it stops being true.
#
# It refuses SQLite on purpose. Two processes do not share an SQLite file, and the fencing
# operational invariant 1 promises lives in the store, so a run against SQLite would be two
# connectors on two databases calling themselves a fleet.
# The bench answers in four exit codes, and two layers would swallow them.
#
# MEASURED on go1.26.6 and GNU Make 3.81: `go run` reports any non-zero status as 1 of its
# own (a program exiting 3 comes back as 1, with `exit status 3` on stderr), and `make`
# exits 2 for any recipe that fails,
# whatever the recipe's own code was. Read through both, a broken invariant, a measurement
# outside its range and a machine that was not ready are one number -- which is exactly
# what having three codes exists to prevent.
#
# The `go run` layer is removable and is removed here: the target builds the binary and
# runs it, so the recipe sees the real code and names it. The `make` layer is not: GNU make
# exits 2 on any failure by design. So a script reads the binary, and the line below says
# so and prints the code it got.
BENCH_BIN ?= $(CURDIR)/bin/fleetbench

bench-fleet: ## Fleet bench with WAC_ENGINE=fake: 2+ connector processes on real PostgreSQL and Redis, invariants under an ownership change. Minutes, not seconds, so it is outside `check`. Does NOT cover whatsmeow under handover (needs a real account). Needs WAC_TEST_DATABASE_URL and WAC_TEST_REDIS_URL. A script wanting the four exit codes apart runs bin/fleetbench: make reports any failure as 2
	@mkdir -p $(dir $(BENCH_BIN))
	@$(GO) build -o $(BENCH_BIN) ./cmd/fleetbench
	@$(BENCH_BIN) $(BENCH_FLAGS); code=$$?; \
	  if [ $$code -ne 0 ]; then \
	    echo ""; \
	    echo "a bancada saiu com o codigo $$code (1 invariante quebrada, 2 setup incompleto, 3 medida fora da faixa)."; \
	    echo "make responde 2 para qualquer receita que falhe, seja qual for o codigo dela: um script que"; \
	    echo "precise dos tres separados roda $(BENCH_BIN) direto."; \
	  fi; \
	  exit $$code

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
check: check-servers offline-passes $(SERVER_PASSES) ## Everything CI enforces; needs both servers (see check-offline)

# Every missing server at once, before anything runs.
#
# Without this, make stops at the first prerequisite that fails and reports one variable.
# Whoever starts a PostgreSQL on that advice gets to the same wall again, one pass later
# and several minutes in, which is the shape of an instruction people stop following.
check-servers:
	@missing=""; remedy=""; setup=""; \
	$(foreach t,$(SERVER_PASSES),test -n "$($($(t)_VAR))" || { \
	  missing="$${missing} $($(t)_VAR)"; \
	  remedy="$${remedy}  $($(t)_RUN)\n"; \
	  setup="$${setup}$($(t)_VAR)=$($(t)_URL) "; };) \
	if [ -n "$$missing" ]; then \
	  echo "make check runs every pass CI enforces, and these are unset or empty:$$missing"; \
	  echo; \
	  printf "$$remedy"; \
	  echo "  $${setup}make check"; \
	  echo; \
	  echo "(any free port will do; the ports above only avoid whatever is usually listening)"; \
	  echo "For the half that needs nothing running: make check-offline"; \
	  exit 1; \
	fi

# The passes that need no server, as a target of their own so that two callers can want
# them without wanting the same thing said about them.
offline-passes: lint tidy test

# The half that needs nothing running, which is what the agent stop hook falls back to:
# requiring a server there would fail every turn ended without one, for a reason that is
# not the turn's. The versioned pre-commit hook runs neither target, and this comment said
# it did until the verifier read `.githooks/pre-commit` instead of believing it.
#
# It says which passes it did not run, and the notice is unconditional because it belongs
# to the recipe of the target somebody asked for. `check` depends on `offline-passes` and
# never comes through here, so there is nothing to suppress and no variable deciding when.
#
# Two versions of this read a variable instead, and both were silenceable from a shell
# profile or an agent's configuration -- including `MAKECMDGOALS`, which make does not
# rewrite when the environment already defines it: `MAKECMDGOALS=check make check-offline`
# printed nothing. Choosing between them was choosing the least bad one, which is the
# shape of answer this issue exists to stop taking.
#
# `tidy` belongs in the offline half and was missing from `check` altogether: CI's lint job
# runs `go mod tidy -diff`, and an untidy go.sum passed `check` green with both servers up
# and nothing skipped. A target that promises everything has to be told when the list grows.
check-offline: offline-passes ## Lint, tidy and the SQLite pass: everything that needs no server
	@echo
	@echo "check-offline is done. It does not run the passes that need a server:"
	@$(foreach t,$(SERVER_PASSES),echo "  $(t) ($($(t)_VAR))"; )
	@echo
	@echo "make check runs those too, and says how to start each server."

clean: ## Remove build and coverage output
	rm -rf bin dist coverage.txt
