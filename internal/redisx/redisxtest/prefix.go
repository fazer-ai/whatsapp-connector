package redisxtest

import (
	"crypto/rand"
	"encoding/hex"
	"testing"
)

// Prefix is what a test sets its keys on a real Redis apart with: the test's name, for
// whoever reads the keys, and 128 random bits, for everybody else.
//
// Not the clock. Two processes running the same test against the same database -- two
// worktrees pointed at one server, or a bench that runs a package four times at once -- start
// it within the same microsecond often enough, and the clock resolves nothing finer here, so
// they shared their keys and a test with nothing wrong in it came out red (#345). The random
// part is hex of a fixed length ending in a colon, so no prefix is the start of another and
// none carries a glob character: a cleanup that deletes `KEYS <prefix>*` reaches only its own.
func Prefix(t testing.TB) string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("draw a key prefix: %v", err)
	}
	return "wactest:" + t.Name() + ":" + hex.EncodeToString(b[:]) + ":"
}
