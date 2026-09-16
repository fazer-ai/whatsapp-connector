// Package toolchain has no runtime code and is not imported by any. It holds the fences
// about how this repository is built and verified: statements that are true of the
// Makefile and of the CI workflow rather than of the connector, and that no compiler and
// no linter looks at.
//
// They live in a Go test because that is what runs them. `make test` is reached from both
// `make check` and `make check-offline`, so a fence written here is enforced by the git
// hooks, by the agent stop hook and by CI, without any of the three being told about it.
package toolchain
