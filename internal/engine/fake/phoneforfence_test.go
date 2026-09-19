package fake_test

import (
	"fmt"
	"testing"

	"github.com/fazer-ai/whatsapp-connector/internal/engine/fake"
)

// One account per session, measured over the sid shapes a bench actually uses.
//
// A collision here is not two sessions sharing a number, it is one of them being deleted:
// `wac_session_device` is unique over the account and `Container.bind` matches on the
// account, so the second session to pair takes the first one's row with it. The first then
// disappears from `store.Wanted` with nothing logged anywhere, and what it disappears from
// is exactly the mass-adoption measurement this engine exists to make.
//
// It is a real pair and not a hypothetical: with the eight varying digits this encoding
// used to have, `PhoneFor("s28693")` and `PhoneFor("s29980")` both came out 5511994750542.
// Both are in the table below, as the anchor for the round that found it.
//
// What this measures and what it does not: a hash into thirteen digits cannot promise no
// collision, and this test does not claim one. It fixes a sample -- the shapes benches
// generate, at a size past where the old encoding failed -- and fails if the encoding ever
// narrows again.
func TestTheFakeGivesOneAccountPerSession(t *testing.T) {
	t.Parallel()

	if fake.PhoneFor("s28693") == fake.PhoneFor("s29980") {
		t.Fatalf("the two sids that collided under the old encoding still share %s",
			fake.PhoneFor("s28693"))
	}

	for _, shape := range []struct {
		name string
		of   func(int) string
	}{
		{"s<n>, what a unit test names its sessions", func(i int) string { return fmt.Sprintf("s%d", i) }},
		{"the uuid a client sends", func(i int) string {
			return fmt.Sprintf("2f1c6f0e-0000-4000-8000-%012d", i)
		}},
		{"a bench's own prefix", func(i int) string { return fmt.Sprintf("wac264-session-%06d", i) }},
	} {
		t.Run(shape.name, func(t *testing.T) {
			t.Parallel()

			const sessions = 200_000
			seen := make(map[string]string, sessions)
			for i := range sessions {
				sid := shape.of(i)
				phone := fake.PhoneFor(sid)
				if other, taken := seen[phone]; taken {
					t.Fatalf("%s and %s are both %s.\n"+
						"They are one account, so the second to pair deletes the first's row and "+
						"the first stops being a session the sweep can bring back -- silently, "+
						"which is the whole difficulty.", other, sid, phone)
				}
				seen[phone] = sid
			}
			if len(seen) != sessions {
				t.Fatalf("counted %d accounts for %d sessions", len(seen), sessions)
			}
		})
	}
}
