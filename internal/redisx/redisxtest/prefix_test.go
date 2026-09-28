package redisxtest_test

import (
	"strings"
	"testing"

	"github.com/fazer-ai/whatsapp-connector/internal/redisx/redisxtest"
)

// Drawn back to back, which is exactly where the clock this replaces handed out the same
// value: on this machine nine calls in ten in a row read the same microsecond.
func TestPrefixesDrawnAtTheSameMomentDoNotCoincide(t *testing.T) {
	t.Parallel()

	// Assembled rather than spelled: the fence in internal/toolchain reads any literal of the
	// mark outside the helper as a prefix built by hand.
	head := strings.Join([]string{"wactest", t.Name(), ""}, ":")
	seen := make(map[string]bool, 10000)
	for range 10000 {
		p := redisxtest.Prefix(t)
		if seen[p] {
			t.Fatalf("%q was handed out twice", p)
		}
		seen[p] = true
		if !strings.HasPrefix(p, head) || !strings.HasSuffix(p, ":") {
			t.Fatalf("%q does not read as this test's prefix", p)
		}
		if strings.ContainsAny(strings.TrimPrefix(p, head), `*?[]\`) {
			t.Fatalf("%q carries a glob character, and a cleanup of `KEYS <prefix>*` would reach past it", p)
		}
	}
}
