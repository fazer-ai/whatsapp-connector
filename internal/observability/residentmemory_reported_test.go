//go:build !darwin || cgo

package observability_test

// residentMemoryIsReported says whether this build's process collector can read resident
// memory, which is a property of the build and not of this repository.
//
// True here: every platform except darwin, and darwin with cgo. A deployment of this
// connector is Linux, so this is the case that matters operationally.
const residentMemoryIsReported = true
