package redisstream

import "time"

// ReadBackScript is the hash of the script a read runs to read this consumer's history
// back, for the tests that have to slow that one trip and nothing else.
var ReadBackScript = pendingPastScript.Hash()

// Backdate has a transport count its uptime from age ago, for a test that loses and
// recovers an answer moments after starting one. Called before its first read.
func Backdate(s *Streams, age time.Duration) {
	s.started = time.Now().Add(-age)
}
