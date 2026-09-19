package whatsmeow

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	wm "go.mau.fi/whatsmeow"
	waAdv "go.mau.fi/whatsmeow/proto/waAdv"
	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/store"
	"github.com/fazer-ai/whatsapp-connector/internal/store/storetest"
)

// A store that will not answer, in a way both dialects feel.
//
// The instrument the suite already had for this holds the store's one connection
// (`session_test.go`), and that only stalls SQLite: `store.Open` gives PostgreSQL a pool
// of twenty, so the held connection is replaced and the write goes through. Measured on
// the base before this was written: the same construction that never comes back under
// SQLite comes back in 8ms under PostgreSQL, which would have made every assertion below
// vacuous on the pass this deployment actually runs.
//
// `MaxConns: 1` is what closes that. It is not a knob invented for the test: it is the
// deployment option the pool already takes, set to the number SQLite has by its nature.
func stalledStore(t *testing.T) (*store.Container, *Session) {
	t.Helper()

	container, err := store.OpenWith(t.Context(), storetest.New(t).URL, store.AlwaysOwned,
		zerolog.Nop(), store.Options{MaxConns: 1})
	if err != nil {
		t.Fatalf("Open the store: %v", err)
	}
	t.Cleanup(func() { _ = container.Close() })

	session := sessionOnStore(t, container, "5511999990001")
	session.setConnected(true)
	return container, session
}

// sessionOnStore is newTestSession against a container the caller opened, which is the
// only thing it adds: the shared one opens its own and there is no way to hand it one.
func sessionOnStore(t *testing.T, container *store.Container, phone string) *Session {
	t.Helper()

	sid := "sid-" + t.Name()
	scoped := container.For(sid)
	device, err := scoped.Device(t.Context())
	if err != nil {
		t.Fatalf("Device: %v", err)
	}
	jid, err := waTypes.ParseJID(phone + ":12@" + waTypes.DefaultUserServer)
	if err != nil {
		t.Fatalf("ParseJID: %v", err)
	}
	device.ID = &jid
	device.Account = &waAdv.ADVSignedDeviceIdentity{
		Details: make([]byte, 32), AccountSignature: make([]byte, 64),
		AccountSignatureKey: make([]byte, 32), DeviceSignature: make([]byte, 64),
	}
	if err := device.Save(t.Context()); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := scoped.Bind(t.Context(), jid); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	session := newSession(t.Context(), sid, wm.NewClient(device, nil), scoped, MediaOptions{}, nil,
		zerolog.Nop(), newLibraryLogger(zerolog.Nop(), sid))
	t.Cleanup(func() { _ = session.Close() })
	return session
}

// hold takes the store's only connection, so every query after it waits.
func hold(t *testing.T, container *store.Container) {
	t.Helper()

	held, err := container.DB().Conn(t.Context())
	if err != nil {
		t.Fatalf("take the store's connection: %v", err)
	}
	t.Cleanup(func() { _ = held.Close() })
}

// answerWithin runs a command and fails rather than hanging when nothing comes back.
//
// Every test here is about a command that used to never return, so calling `Execute`
// straight would make the base -- where it does not return -- hang the package until the
// go test timeout kills it, with a stack dump instead of a named failure. A test that can
// hang is a bad test even on the run where it passes.
func answerWithin(ctx context.Context, t *testing.T, session *Session, command *protocol.Command, window time.Duration) error {
	t.Helper()

	answered := make(chan error, 1)
	go func() {
		_, err := session.Execute(ctx, command)
		answered <- err
	}()
	select {
	case err := <-answered:
		return err
	case <-time.After(window):
		t.Fatalf("the command never came back in %s, so the account's executor is held by a "+
			"store that will not answer and nothing else will free it", window)
		return nil
	}
}

const aCreate = `{"subject":"Obras","participants":[{"kind":"phone","id":"5511999990002"}]}`

// #284: the intent is written before anything is asked of WhatsApp, and it used to be
// written under whatever context the caller brought. `bound` gives a command a deadline
// only when the caller named one, so a `group.create` carrying neither field reached that
// write with no ceiling at all and stayed there for as long as the store did not answer,
// holding the account's serial executor with it.
func TestACreationGivesUpOnAStoreThatWillNotAnswer(t *testing.T) {
	t.Parallel()

	container, session := stalledStore(t)
	session.storeLimit = 200 * time.Millisecond

	asked := 0
	session.createTheGroup = func(context.Context, *wm.Client, keyedCreate) (*waTypes.GroupInfo, error) {
		asked++
		return nil, errors.New("WhatsApp should not have been asked")
	}
	hold(t, container)

	began := time.Now()
	err := answerWithin(context.Background(), t, session, namedCreate("c1", "once", aCreate), 30*time.Second)
	if spent := time.Since(began); spent > 40*session.storeLimit {
		t.Fatalf("the creation took %s to give up on a store bound of %s", spent, session.storeLimit)
	}
	if err == nil {
		t.Fatal("a creation whose intent was never written answered as though it had been")
	}
	if code := string(codeOf(err)); code == "" {
		t.Fatalf("the refusal carries no contract code: %v", err)
	}
	if asked != 0 {
		t.Fatalf("WhatsApp was asked %d times for a creation whose intent could not be written; "+
			"the refusal has to happen before the irreversible half", asked)
	}
}

// The ceiling is this connector's, and it composes with the caller's rather than replacing
// it: whichever is shorter decides. The pair matters more than either half -- a ceiling
// that ignored a shorter caller would be a connector deciding how long a client waits.
func TestTheShorterOfTheCallersCeilingAndOursDecidesTheIntentWrite(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		caller time.Duration // zero means the caller named nothing
		cancel bool
		want   time.Duration
	}{
		"ours, when the caller named nothing": {want: 2 * time.Second},
		"the caller's, when it is shorter":    {caller: 200 * time.Millisecond, want: 200 * time.Millisecond},
		"a cancel, which is not a deadline":   {cancel: true, want: 200 * time.Millisecond},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			container, session := stalledStore(t)
			session.storeLimit = 2 * time.Second
			session.createTheGroup = func(context.Context, *wm.Client, keyedCreate) (*waTypes.GroupInfo, error) {
				return nil, errors.New("WhatsApp should not have been asked")
			}
			hold(t, container)

			ctx := context.Background()
			switch {
			case tc.cancel:
				var stop context.CancelFunc
				ctx, stop = context.WithCancel(context.Background())
				time.AfterFunc(200*time.Millisecond, stop)
			case tc.caller > 0:
				var stop context.CancelFunc
				ctx, stop = context.WithTimeout(context.Background(), tc.caller)
				defer stop()
			}

			began := time.Now()
			if err := answerWithin(ctx, t, session, namedCreate("c1", "once", aCreate), 30*time.Second); err == nil {
				t.Fatal("the creation answered as though the intent had been written")
			}
			spent := time.Since(began)
			// Half the wanted window is the floor: anything faster means the ceiling that
			// decided was not the one this case is about.
			if spent < tc.want/2 {
				t.Fatalf("the creation came back in %s, too soon for the %s this case is about",
					spent, tc.want)
			}
			if spent > tc.want+3*time.Second {
				t.Fatalf("the creation came back in %s, want about %s: the ceiling that decided "+
					"was not the shorter one", spent, tc.want)
			}
		})
	}
}

// The ceiling stops at the store. #163 decided a creation keeps its full wait on WhatsApp,
// because a group WhatsApp made is not unmade by this side giving up, and
// `TestTheTwoThatCannotBeRepeatedArriveWithNoCeiling` watches for that at the seam. This
// is the same claim from the other direction: widening the new ceiling to cover the
// function would put a deadline on that request, and this fails when it does.
func TestTheIntentCeilingDoesNotReachTheRequest(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)

	var set bool
	var left time.Duration
	session.createTheGroup = func(ctx context.Context, _ *wm.Client, _ keyedCreate) (*waTypes.GroupInfo, error) {
		if until, ok := ctx.Deadline(); ok {
			left, set = time.Until(until), true
		}
		return aMadeGroup("120363041234567890", "Obras",
			waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)), nil
	}

	if _, err := session.Execute(context.Background(), namedCreate("c1", "once", aCreate)); err != nil {
		t.Fatalf("the happy path stopped working: %v", err)
	}
	if set {
		t.Fatalf("the request to WhatsApp arrived with %s to run in, and a creation is the one "+
			"command that keeps its full wait: a group made after this side gave up is a group "+
			"the caller cannot tell from the one a retry would make", left)
	}
}

// The second store write in the same function, after the group exists. Closing only the
// first leaves the command held here instead, which is what #284's holdout measured on
// the base: stall the store from inside the seam and the creation still never comes back.
//
// Bounded off the session's context rather than the caller's, so this asserts the giving
// up and not the caller's clock: the context passed in is never cancelled.
func TestACreationGivesUpOnAStoreThatWillNotRecordWhichGroupItMade(t *testing.T) {
	t.Parallel()

	container, session := stalledStore(t)
	session.storeLimit = 200 * time.Millisecond
	session.createTheGroup = func(context.Context, *wm.Client, keyedCreate) (*waTypes.GroupInfo, error) {
		// The store answers up to here and stops afterwards, so the only unanswered call
		// is the one that records which group was made.
		hold(t, container)
		return aMadeGroup("120363041234567890", "Obras",
			waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)), nil
	}

	_ = answerWithin(context.Background(), t, session, namedCreate("c1", "once", aCreate), 30*time.Second)
}

// What the client is told, and it is not what the shared mapping would have said. A
// deadline reached over this write is neither of the two the table in `notsettled_test.go`
// already separates: the store did not error, and the caller's clock did not run out.
func TestAStoreTooSlowToRecordAnIntentIsNotToldAsTheCallersClock(t *testing.T) {
	t.Parallel()

	container, session := stalledStore(t)
	session.storeLimit = 200 * time.Millisecond
	session.createTheGroup = func(context.Context, *wm.Client, keyedCreate) (*waTypes.GroupInfo, error) {
		return nil, errors.New("WhatsApp should not have been asked")
	}
	hold(t, container)

	err := answerWithin(context.Background(), t, session, namedCreate("c1", "once", aCreate), 30*time.Second)
	if err == nil {
		t.Fatal("the creation answered as though the intent had been written")
	}
	if got := string(codeOf(err)); got != string(protocol.ErrorInternal) {
		t.Fatalf("a creation cut by this connector's own ceiling answered %q, want %q: the store "+
			"is what stopped answering, and it settles when somebody fixes the store rather than "+
			"when the caller asks WhatsApp again", got, protocol.ErrorInternal)
	}
	if message := err.Error(); containsAny(message, "did not go out", "deadline") {
		t.Fatalf("the refusal says %q, and both halves of that are false here: nothing went out, "+
			"because the intent is written before any request, and the deadline that passed is "+
			"this connector's rather than the caller's", message)
	}
}

// And the caller's own clock keeps the word it had, because that case is the one the
// shared mapping was written for.
func TestACallersOwnClockOverTheIntentWriteIsStillTimeout(t *testing.T) {
	t.Parallel()

	container, session := stalledStore(t)
	session.storeLimit = time.Minute
	session.createTheGroup = func(context.Context, *wm.Client, keyedCreate) (*waTypes.GroupInfo, error) {
		return nil, errors.New("WhatsApp should not have been asked")
	}
	hold(t, container)

	ctx, stop := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer stop()
	err := answerWithin(ctx, t, session, namedCreate("c1", "once", aCreate), 30*time.Second)
	if err == nil {
		t.Fatal("the creation answered as though the intent had been written")
	}
	if got := string(codeOf(err)); got != string(protocol.ErrorTimeout) {
		t.Fatalf("a creation cut by the caller's own clock answered %q, want %q", got, protocol.ErrorTimeout)
	}
}

func containsAny(haystack string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(haystack, needle) {
			return true
		}
	}
	return false
}

// The deployment's own stall, which the pool one is not.
//
// Holding the store's single connection makes the wait happen in `database/sql`'s queue
// for a free connection, and that wait returns the context's own error. What a deployment
// produces instead is a query already in flight against a row somebody else has locked:
// PostgreSQL cancels the statement when the deadline passes and `lib/pq` reports
// `canceling statement due to user request`, an error that wraps neither
// `context.DeadlineExceeded` nor `context.Canceled`.
//
// The first version of `intentFailure` read the error, so it was green over the pool stall
// and took this path -- the only one that happens outside a test -- down the branch for an
// ordinary store failure, losing the line that says which ceiling fired. Reading the
// contexts instead is what this pins, and reverting that one line is what turns this red
// while every other test in this file stays green.
func TestTheCeilingIsNamedEvenWhenTheDriverSwallowsTheCause(t *testing.T) {
	t.Parallel()

	target := storetest.New(t)
	if !target.Postgres() {
		// SQLite has no second connection to hold a row lock with, and one writer is its
		// nature rather than a setting. The claim is about what a driver does to a
		// cancelled in-flight query, and SQLite never gets one.
		t.Skip("the in-flight cancellation this is about only exists on the PostgreSQL pass")
	}

	container, err := store.Open(t.Context(), target.URL, store.AlwaysOwned, zerolog.Nop())
	if err != nil {
		t.Fatalf("Open the store: %v", err)
	}
	t.Cleanup(func() { _ = container.Close() })

	session := sessionOnStore(t, container, "5511999990001")
	session.setConnected(true)
	session.storeLimit = 400 * time.Millisecond
	session.createTheGroup = func(context.Context, *wm.Client, keyedCreate) (*waTypes.GroupInfo, error) {
		return nil, errors.New("WhatsApp should not have been asked")
	}

	command := namedCreate("c1", "once", aCreate)
	attempt := attemptName(command)
	// The row has to exist for a lock on it to be what the second write waits for:
	// `ON CONFLICT DO UPDATE` is the statement that blocks, and there is no conflict
	// without a row.
	if _, _, err := session.store.BeginGroupCreate(t.Context(), attempt, "k1", "Obras", time.Now()); err != nil {
		t.Fatalf("write the row this locks: %v", err)
	}

	locker, err := sql.Open("postgres", target.URL)
	if err != nil {
		t.Fatalf("a second pool: %v", err)
	}
	t.Cleanup(func() { _ = locker.Close() })
	tx, err := locker.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	var locked string
	if err := tx.QueryRowContext(t.Context(),
		`SELECT attempt FROM wac_group_create WHERE sid = $1 AND attempt = $2 FOR UPDATE`,
		session.sid, attempt).Scan(&locked); err != nil {
		t.Fatalf("hold the row's lock: %v", err)
	}

	began := time.Now()
	err = answerWithin(context.Background(), t, session, command, 30*time.Second)
	if err == nil {
		t.Fatal("the creation answered as though the intent had been written")
	}
	if spent := time.Since(began); spent > 20*session.storeLimit {
		t.Fatalf("the creation took %s to give up on a store bound of %s", spent, session.storeLimit)
	}
	if got := string(codeOf(err)); got != string(protocol.ErrorInternal) {
		t.Fatalf("a creation cut on a locked row answered %q, want %q", got, protocol.ErrorInternal)
	}
	if message := err.Error(); !containsAny(message, "did not answer in time") {
		t.Fatalf("the refusal says %q, which is the generic store failure and not this connector's "+
			"own ceiling: reading the driver's error instead of the context is how that happens, "+
			"and `lib/pq` reports a cancelled statement as an error wrapping nothing", message)
	}
}
