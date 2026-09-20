// Package testwait holds the one budget every polling helper in this repository's tests
// waits out, and the interval they poll at.
//
// It exists because two of them disagreed and one of them was wrong. `internal/session`
// had six helpers that poll a condition and fail the test when it never comes true, three
// budgeted at five seconds and three at two, and two of those three sat in the same file
// as each other. `internal/app` had ten and two. Nothing decided which; each was whatever
// the test that needed one first happened to write, and #298 is the bill: a composed path
// that takes a fifth of a second here took more than two seconds on a CI runner under the
// race detector and coverage instrumentation, and the helper with the short budget called
// that a defect.
//
// What a budget like this measures is wall clock, and on a saturated runner wall clock is
// not work: a goroutine that is not scheduled spends the budget without the path moving.
// So the number is not derived from how long the path takes -- measured at 0.18..0.35s for
// the path in #298, even with sixteen of this machine's eighteen cores busy -- but from
// how much suspension a test should tolerate before it accuses the code of a defect it
// does not have. Five seconds is the value the fleet has never seen reported as a false
// failure, which is the only evidence any of these numbers ever had.
//
// A package rather than a constant per test package because Go gives test packages no
// other way to share one: `package session` and `package session_test` are compiled
// separately, and `internal/app` shares nothing with either. Nothing imports this outside
// `_test.go` files, so it never reaches a binary.
package testwait

import "time"

// Budget is how long a polling helper waits before it calls the condition a failure.
const Budget = 5 * time.Second

// Poll is how long it sleeps between reads.
//
// It is the smaller decision of the two. What a shorter interval buys is a faster failure
// message on a test that was going to fail anyway; what it costs is one more read of
// whatever the condition touches, which on the helpers here is a mutex or a Redis double.
const Poll = 5 * time.Millisecond
