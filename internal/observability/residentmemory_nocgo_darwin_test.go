//go:build darwin && !cgo

package observability_test

// residentMemoryIsReported is false on a cgo-less darwin build.
//
// MEASURED on darwin/arm64 with `CGO_ENABLED=0`: the pinned client_golang compiles
// `process_collector_mem_nocgo_darwin.go`, whose memory reader returns
// `errNotImplemented`, so `process_resident_memory_bytes` is absent while
// `process_open_fds` and the CPU series are there. The collector is registered and
// reporting; it is this one number it cannot read.
//
// A build tag rather than a runtime check, because the condition is decided at compile
// time and a test that skips on what it happens to find would go green the day the
// collector stopped reporting anything at all.
const residentMemoryIsReported = false
