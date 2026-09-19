package whatsmeow

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/store"
)

// A connect that has to rebuild the device keeps the record of what the client asked for.
//
// The record lives above this engine now: the session layer writes it and then calls
// `Connect`. A connect that finds its device unusable deletes it to build a replacement,
// and the delete used to take the record with it, because one `Forget` did both. That
// would have the layer recording a request and the connect carrying it out throwing it
// away, which leaves the account unresumable for exactly the reason #266 is about: no
// error, no warning, and a sweep with nothing to read.
//
// Measured before this test existed: making `repairDevice` use the wider door again
// leaves `go test ./...` green, exit 0. Nothing else in the tree looks here.
//
// The session reaches the repair through `markStale` rather than through a second pairing,
// and that is a limit written down rather than hidden: `replacePairing` only tears
// something down when there is a pairing conversation up, and a bench with no socket never
// has one. Two connects in a row leave the session unstale and the repair never runs.
//
// The assertion is on `wac_session_desired` and not on `store.Wanted`, because the repair
// unbinds the device on the way through and `Wanted` joins the two tables: it would read
// empty here for a reason that is not this one.
//
// A resume, and only a resume. This test used to run the same body for `qr` as well, and
// that subtest talked to WhatsApp: a `qr` connect reaches the same repair and then goes on
// to `pairWithQR`, which opens the pairing channel and dials, and the test binary was
// measured holding an established socket to Meta on port 443 for the length of it. It also
// bought nothing, because the repair runs before the pairing mode is looked at: the mode
// is not what this test varies.
func TestARepairedDeviceKeepsWhatTheClientAsked(t *testing.T) {
	t.Parallel()

	session, container := newTestSession(t, "5511999990001")
	ctx := t.Context()

	// Seeded, and that is the point rather than a shortcut: what is under test is not who
	// writes the row -- the session layer does, and its own test covers that -- but what
	// is left of it after a connect that rebuilds the device.
	if err := session.store.PutDesiredConnected(ctx, store.Wants{Groups: true}); err != nil {
		t.Fatalf("PutDesiredConnected: %v", err)
	}
	session.markStale()

	// The error is not the assertion: a resume with nothing to resume is refused, and the
	// repair has already run by the time `Connect` returns.
	_ = session.Connect(ctx, engine.ConnectRequest{Pairing: "resume", Groups: true})

	// Read off the table rather than through a store helper, the way
	// `logout_unanswered_test.go` does: the sid is interpolated because `container.DB()`
	// is the raw handle and the placeholder differs per dialect, and it is
	// `"sid-" + t.Name()`, not anything a caller supplies.
	// The row being gone and the row saying something else are the same failure here --
	// both mean the repair took the request with it -- so they get the same message rather
	// than a bare `sql: no rows in result set`, which names the query and not the defect.
	var wanted string
	switch err := container.DB().QueryRowContext(ctx,
		`SELECT desired FROM wac_session_desired WHERE sid = '`+session.sid+`'`).Scan(&wanted); {
	case errors.Is(err, sql.ErrNoRows):
		wanted = "no row at all"
	case err != nil:
		t.Fatalf("read the desired state back: %v", err)
	}
	if wanted != "connected" {
		t.Fatalf("after a connect that rebuilt the device, the desired state of %s reads %q, not %q.\n"+
			"The client asked for this session and the repair threw the record away, so nothing "+
			"anywhere says it should be in the air: the sweep reads an empty list and the account "+
			"stays down until somebody opens the inbox and asks again. That is #266 rebuilt by the "+
			"fix for it.", session.sid, wanted, "connected")
	}
}
