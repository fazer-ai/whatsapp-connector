package whatsmeow

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	wm "go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// namedCreate is the `group.create` a client sends, with the name it gives the request.
func namedCreate(id, key, payload string) *protocol.Command {
	return &protocol.Command{
		V: protocol.Version, ID: id, Type: protocol.CommandGroupCreate,
		SID: "s1", IdempotencyKey: key, Payload: json.RawMessage(payload),
	}
}

// aMadeGroup is what WhatsApp answers a creation with, made at the instant it is asked for.
func aMadeGroup(jid, subject string, owner waTypes.JID) *waTypes.GroupInfo {
	return &waTypes.GroupInfo{
		JID:          waTypes.NewJID(jid, waTypes.GroupServer),
		OwnerJID:     owner,
		GroupName:    waTypes.GroupName{Name: subject},
		GroupCreated: time.Now(),
	}
}

// whatWhatsAppSaysAboutTheGroup is the notification WhatsApp sends about a group that was
// just made, carrying back the key the creation went out with. The one redelivered to the
// next instance after a crash has exactly this shape, which is what `probe131b` measured.
func whatWhatsAppSaysAboutTheGroup(key string, made *waTypes.GroupInfo) *waEvents.JoinedGroup {
	return &waEvents.JoinedGroup{Type: "new", CreateKey: key, GroupInfo: *made}
}

// theKeyOnRecord is what an attempt was filed under, which is the key its creation travels
// with and the one WhatsApp echoes back.
func theKeyOnRecord(t *testing.T, session *Session, attempt string) string {
	t.Helper()
	began, found, err := session.store.GroupCreation(t.Context(), attempt)
	if err != nil {
		t.Fatalf("read the attempt %s: %v", attempt, err)
	}
	if !found {
		t.Fatalf("no attempt on record under %s", attempt)
	}
	return began.Key
}

// impatient makes the wait for WhatsApp's notification short enough to test. What it bounds
// is the two arriving in the wrong order, so a test that means to reach the end of it
// should not spend the production window doing so.
func impatient(session *Session) { session.createWait = 100 * time.Millisecond }

// A redelivered creation is the defect this covers: `group.create` is the one command whose
// second run makes a second thing rather than converging, and the ledger in `internal/session`
// records a command only after it ran. A delivery that arrives with no record -- because the
// record was lost, or because it was never written -- used to reach WhatsApp again.
func TestARedeliveredCreationAnswersTheGroupTheFirstOneMade(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	self := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)
	made := aMadeGroup("120363041234567890", "Obras", self)

	asked := 0
	session.createTheGroup = func(context.Context, *wm.Client, keyedCreate) (*waTypes.GroupInfo, error) {
		asked++
		return made, nil
	}
	session.groupInfo = func(context.Context, *wm.Client, waTypes.JID) (*waTypes.GroupInfo, error) {
		return made, nil
	}

	command := namedCreate("c1", "once", `{"subject":"Obras","participants":[]}`)
	first, err := session.Execute(t.Context(), command)
	if err != nil {
		t.Fatalf("the first delivery: %v", err)
	}
	again, err := session.Execute(t.Context(), command)
	if err != nil {
		t.Fatalf("the redelivery: %v", err)
	}
	if asked != 1 {
		t.Fatalf("WhatsApp was asked to make %d groups, want 1: a redelivery made a second group", asked)
	}
	if namedGroup(t, first) != namedGroup(t, again) {
		t.Fatalf("the redelivery answered %s, want the group the first one made, %s",
			namedGroup(t, again), namedGroup(t, first))
	}
}

// The window the issue names, in its own shape: the group was made and the instance that
// asked for it died before writing down which one. The notification WhatsApp redelivers to
// whoever holds the session next carries the key the creation went out with, so the group
// is named exactly rather than guessed at -- which is what separates a creation from a send,
// and what the objection in `carryOut` to reserving before the fact does not cover.
func TestACreationWhoseRecordWasLostIsAnsweredByWhatsAppsOwnNotification(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	self := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)
	made := aMadeGroup("120363041234567890", "Obras", self)

	// The intent, written before WhatsApp was asked, is all the crashed attempt left.
	began := time.Now().Add(-time.Minute)
	if _, _, err := session.store.BeginGroupCreate(
		t.Context(), "idem:once", "WACLOST", "Obras", began); err != nil {
		t.Fatalf("begin the attempt that crashed: %v", err)
	}
	made.GroupCreated = began.Add(time.Second)

	asked := 0
	session.createTheGroup = func(context.Context, *wm.Client, keyedCreate) (*waTypes.GroupInfo, error) {
		asked++
		return aMadeGroup("120363099999999999", "Obras", self), nil
	}
	session.groupInfo = func(context.Context, *wm.Client, waTypes.JID) (*waTypes.GroupInfo, error) {
		return made, nil
	}
	// Redelivered on the new socket, ahead of the command.
	session.joinedAGroup(whatWhatsAppSaysAboutTheGroup("WACLOST", made))

	answer, err := session.Execute(t.Context(),
		namedCreate("c1", "once", `{"subject":"Obras","participants":[]}`))
	if err != nil {
		t.Fatalf("the redelivery: %v", err)
	}
	if asked != 0 {
		t.Fatal("WhatsApp was asked to make a group the earlier attempt had already made")
	}
	if got := namedGroup(t, answer); got != made.JID.String() {
		t.Fatalf("the redelivery answered %s, want the group WhatsApp named, %s", got, made.JID)
	}
}

// The notification and the redelivered command both arrive once the socket is back, and
// nothing orders them. A command that got there first has to wait for the answer rather
// than make a second group.
func TestARedeliveredCreationWaitsForTheNotificationThatNamesItsGroup(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.createWait = 2 * time.Second
	self := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)
	made := aMadeGroup("120363041234567890", "Obras", self)

	began := time.Now().Add(-time.Minute)
	if _, _, err := session.store.BeginGroupCreate(
		t.Context(), "idem:once", "WACLATE", "Obras", began); err != nil {
		t.Fatalf("begin the attempt that crashed: %v", err)
	}
	made.GroupCreated = began.Add(time.Second)

	asked := 0
	session.createTheGroup = func(context.Context, *wm.Client, keyedCreate) (*waTypes.GroupInfo, error) {
		asked++
		return aMadeGroup("120363099999999999", "Obras", self), nil
	}
	session.joinedGroups = func(context.Context, *wm.Client) ([]*waTypes.GroupInfo, error) {
		return []*waTypes.GroupInfo{made}, nil
	}
	session.groupInfo = func(context.Context, *wm.Client, waTypes.JID) (*waTypes.GroupInfo, error) {
		return made, nil
	}
	// The notification lands while the command is already waiting on it.
	go func() {
		time.Sleep(150 * time.Millisecond)
		session.joinedAGroup(whatWhatsAppSaysAboutTheGroup("WACLATE", made))
	}()

	answer, err := session.Execute(t.Context(),
		namedCreate("c1", "once", `{"subject":"Obras","participants":[]}`))
	if err != nil {
		t.Fatalf("the redelivery: %v", err)
	}
	if asked != 0 {
		t.Fatal("a second group was made because the notification had not arrived yet")
	}
	if got := namedGroup(t, answer); got != made.JID.String() {
		t.Fatalf("the redelivery answered %s, want %s", got, made.JID)
	}
}

// An attempt is on record and WhatsApp has not said which group it made. Making another
// would be the duplicate the issue is about, and there is nothing else to answer with, so
// the command is refused -- saying which of the two it is, because the client reads that
// text -- and converges on the next delivery once the notification lands.
func TestARedeliveredCreationRefusesWhileWhatsAppHasNotNamedItsGroup(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	impatient(session)
	self := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)

	began := time.Now().Add(-time.Minute)
	if _, _, err := session.store.BeginGroupCreate(
		t.Context(), "idem:once", "WACOPEN", "Obras", began); err != nil {
		t.Fatalf("begin the attempt that crashed: %v", err)
	}

	asked := 0
	session.createTheGroup = func(context.Context, *wm.Client, keyedCreate) (*waTypes.GroupInfo, error) {
		asked++
		return aMadeGroup("120363099999999999", "Obras", self), nil
	}

	answer, err := session.Execute(t.Context(),
		namedCreate("c1", "once", `{"subject":"Obras","participants":[]}`))
	if err == nil {
		t.Fatalf("the redelivery answered %s over a creation WhatsApp had said nothing about",
			namedGroup(t, answer))
	}
	if !strings.Contains(err.Error(), "not settled yet") {
		t.Fatalf("the refusal reads %q, want the one that says WhatsApp has not named the group yet", err)
	}
	if asked != 0 {
		t.Fatal("a second group was made while which group belonged to this request was in doubt")
	}
}

// A creation that may well have happened keeps its intent. WhatsApp answering nothing is
// exactly the case where a group can exist with nobody having written down which one, so a
// delivery that threw the record away here would have the next one make the second group.
func TestACreationThatMayHaveHappenedKeepsItsIntent(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	impatient(session)

	asked := 0
	session.createTheGroup = func(context.Context, *wm.Client, keyedCreate) (*waTypes.GroupInfo, error) {
		asked++
		// The socket went while the answer was outstanding: the stanza may have reached
		// WhatsApp and the group may exist.
		return nil, &wm.DisconnectedError{Action: "info query"}
	}

	payload := `{"subject":"Obras","participants":[]}`
	if _, err := session.Execute(t.Context(), namedCreate("c1", "once", payload)); err == nil {
		t.Fatal("a creation whose answer was lost was reported as having worked")
	}
	began, found, err := session.store.GroupCreation(t.Context(), "idem:once")
	if err != nil {
		t.Fatalf("read the attempt: %v", err)
	}
	if !found {
		t.Fatal("the intent was thrown away over a failure that says nothing about whether a group was made")
	}
	if began.Done() {
		t.Fatalf("the attempt names %s, and nothing named it", began.JID)
	}
	// And the next delivery is refused rather than making a second group.
	if _, err := session.Execute(t.Context(), namedCreate("c2", "once", payload)); err == nil {
		t.Fatal("the redelivery was answered while which group this request made was unknown")
	}
	if asked != 1 {
		t.Fatalf("WhatsApp was asked for %d groups, want 1: the redelivery made a second one", asked)
	}
}

// whatsmeow retries an IQ whose socket died, and answers `ErrNotConnected` when the
// reconnection does not come -- after the first frame has gone out. This connector reads
// that error as "nothing was sent" and forgets the intent, so with the retry on, a creation
// WhatsApp had accepted would be made again by the next delivery.
func TestTheCreateQueryTurnsOffTheLibrarysOwnRetry(t *testing.T) {
	t.Parallel()

	query := theCreateQuery("Obras", "WACKEY", nil)
	if !query.NoRetry {
		t.Fatal("the creation is sent with the library's retry on, which answers ErrNotConnected after sending")
	}
	if query.Namespace != "w:g2" || query.To != waTypes.GroupServerJID {
		t.Fatalf("the creation goes to %s/%s, want the group server", query.Namespace, query.To)
	}
}

// The creation has to go out carrying the key, or WhatsApp has nothing to echo and the
// notification names no attempt. The key is the one on record, written with the intent
// before anything was sent.
func TestACreationTravelsUnderTheKeyItWasFiledUnder(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	self := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)

	var sent keyedCreate
	session.createTheGroup = func(_ context.Context, _ *wm.Client, req keyedCreate) (*waTypes.GroupInfo, error) {
		sent = req
		return aMadeGroup("120363041234567890", "Obras", self), nil
	}

	if _, err := session.Execute(t.Context(),
		namedCreate("c1", "once", `{"subject":"Obras","participants":[]}`)); err != nil {
		t.Fatalf("the creation: %v", err)
	}
	if sent.Key == "" {
		t.Fatal("the creation went out with no key, so nothing WhatsApp says about the group can name it")
	}
	if filed := theKeyOnRecord(t, session, "idem:once"); sent.Key != filed {
		t.Fatalf("the creation went out under %s and was filed under %s, so the notification would name nothing",
			sent.Key, filed)
	}
}

// The search has to be narrow enough that it does not swallow a creation somebody meant.
// Two requests with names of their own are two requests, however alike their payloads.
func TestTwoCreationsAskedSeparatelyBothHappen(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	impatient(session)
	self := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)

	var joined []*waTypes.GroupInfo
	asked := 0
	session.createTheGroup = func(context.Context, *wm.Client, keyedCreate) (*waTypes.GroupInfo, error) {
		asked++
		made := aMadeGroup("12036304123456789"+string(rune('0'+asked)), "Obras", self)
		joined = append(joined, made)
		return made, nil
	}
	session.joinedGroups = func(context.Context, *wm.Client) ([]*waTypes.GroupInfo, error) {
		return joined, nil
	}

	payload := `{"subject":"Obras","participants":[]}`
	first, err := session.Execute(t.Context(), namedCreate("c1", "first", payload))
	if err != nil {
		t.Fatalf("the first request: %v", err)
	}
	second, err := session.Execute(t.Context(), namedCreate("c2", "second", payload))
	if err != nil {
		t.Fatalf("the second request: %v", err)
	}
	if asked != 2 {
		t.Fatalf("WhatsApp was asked for %d groups, want 2: a request somebody made was answered in silence", asked)
	}
	if namedGroup(t, first) == namedGroup(t, second) {
		t.Fatalf("both requests were answered with %s, want a group each", namedGroup(t, first))
	}
}

// A command refused before anything is asked of WhatsApp must leave nothing behind. An
// attempt filed for it would have the next delivery of the same name wait on a notification
// about a group that was never asked for.
func TestACreationRefusedBeforeTheWireLeavesNoAttempt(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)

	if _, err := session.Execute(t.Context(),
		namedCreate("c1", "once", `{"subject":"","participants":[]}`)); err == nil {
		t.Fatal("a group called nothing was accepted")
	}
	if _, found, err := session.store.GroupCreation(t.Context(), "idem:once"); err != nil {
		t.Fatalf("read the attempt: %v", err)
	} else if found {
		t.Fatal("a creation refused before the wire filed an attempt, which a later delivery would wait on")
	}
}

// A client that stopped waiting and asked again sends the same request under the same name
// with an id of its own. The name the caller gave is what says the two are one request, and
// a record filed under the transport's id instead would make a group for each.
func TestARetryUnderTheSameNameWithANewIDDoesNotMakeASecondGroup(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	self := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)
	made := aMadeGroup("120363041234567890", "Obras", self)

	asked := 0
	session.createTheGroup = func(context.Context, *wm.Client, keyedCreate) (*waTypes.GroupInfo, error) {
		asked++
		return made, nil
	}
	session.groupInfo = func(context.Context, *wm.Client, waTypes.JID) (*waTypes.GroupInfo, error) {
		return made, nil
	}

	payload := `{"subject":"Obras","participants":[]}`
	if _, err := session.Execute(t.Context(), namedCreate("c1", "once", payload)); err != nil {
		t.Fatalf("the first request: %v", err)
	}
	if _, err := session.Execute(t.Context(), namedCreate("c2", "once", payload)); err != nil {
		t.Fatalf("the retry: %v", err)
	}
	if asked != 1 {
		t.Fatalf("WhatsApp was asked for %d groups, want 1: the caller's own name for the request was not what it was filed under", asked)
	}
}

// The two names a command can be filed under come from different places and mean different
// things, so they are kept apart. A client whose idempotency key reads like another command's
// id must not find that command's group.
func TestAKeyAndACommandIDThatReadAlikeAreNotOneAttempt(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	self := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)

	asked := 0
	session.createTheGroup = func(context.Context, *wm.Client, keyedCreate) (*waTypes.GroupInfo, error) {
		asked++
		return aMadeGroup("12036304123456789"+string(rune('0'+asked)), "Obras", self), nil
	}
	session.joinedGroups = func(context.Context, *wm.Client) ([]*waTypes.GroupInfo, error) {
		return nil, nil
	}

	payload := `{"subject":"Obras","participants":[]}`
	// One command carrying no key at all, filed under its id.
	if _, err := session.Execute(t.Context(), namedCreate("shared", "", payload)); err != nil {
		t.Fatalf("the command with no key: %v", err)
	}
	// Another whose key happens to read like that id.
	if _, err := session.Execute(t.Context(), namedCreate("c2", "shared", payload)); err != nil {
		t.Fatalf("the command whose key reads like the other's id: %v", err)
	}
	if asked != 2 {
		t.Fatalf("WhatsApp was asked for %d groups, want 2: a key and an id that read alike were taken for one request", asked)
	}
}

// The record is this session's, and so is the key. Another session's attempt under the same
// name is another account's business, and settling it from here would answer one client's
// command with another's group.
func TestANotificationSettlesOnlyThisSessionsAttempt(t *testing.T) {
	t.Parallel()

	session, container := newTestSession(t, "5511999990001")
	session.setConnected(true)
	self := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)
	made := aMadeGroup("120363041234567890", "Obras", self)

	began := time.Now().Add(-time.Minute)
	if _, _, err := session.store.BeginGroupCreate(
		t.Context(), "idem:once", "WACSHARED", "Obras", began); err != nil {
		t.Fatalf("begin this session's attempt: %v", err)
	}
	// Another session filed an attempt under the same name and the same key.
	elsewhere := container.For("another-session")
	if _, _, err := elsewhere.BeginGroupCreate(
		t.Context(), "idem:once", "WACSHARED", "Obras", began); err != nil {
		t.Fatalf("begin the other session's attempt: %v", err)
	}

	session.joinedAGroup(whatWhatsAppSaysAboutTheGroup("WACSHARED", made))

	mine, _, err := session.store.GroupCreation(t.Context(), "idem:once")
	if err != nil {
		t.Fatalf("read this session's attempt: %v", err)
	}
	if mine.JID != made.JID.String() {
		t.Fatalf("this session's attempt was left at %q, want the group WhatsApp named", mine.JID)
	}
	theirs, _, err := elsewhere.GroupCreation(t.Context(), "idem:once")
	if err != nil {
		t.Fatalf("read the other session's attempt: %v", err)
	}
	if theirs.Done() {
		t.Fatalf("another session's attempt was settled with %s, a group of this account's", theirs.JID)
	}
}

// The notification is the command's answer, not group traffic. A session whose client asked
// for direct chats only still has a `group.create` to settle, and dropping the notification
// with the publication would leave that command waiting for good.
func TestANotificationIsRecordedEvenWhenTheClientWantsNoGroups(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.setGroups(false)
	self := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)
	made := aMadeGroup("120363041234567890", "Obras", self)

	if _, _, err := session.store.BeginGroupCreate(
		t.Context(), "idem:once", "WACQUIET", "Obras", time.Now().Add(-time.Minute)); err != nil {
		t.Fatalf("begin the attempt that crashed: %v", err)
	}

	session.joinedAGroup(whatWhatsAppSaysAboutTheGroup("WACQUIET", made))

	settled, _, err := session.store.GroupCreation(t.Context(), "idem:once")
	if err != nil {
		t.Fatalf("read the attempt: %v", err)
	}
	if settled.JID != made.JID.String() {
		t.Fatalf("the attempt was left at %q, want %s: the command it belongs to would wait for good",
			settled.JID, made.JID)
	}
}

// The intent is the cover, so a creation that could not write one has no cover. Going ahead
// anyway costs the caller a group they cannot tell from the one a redelivery would make;
// refusing costs them a command they can send again.
func TestACreationThatCouldNotWriteItsIntentIsRefused(t *testing.T) {
	t.Parallel()

	session, container := newTestSession(t, "5511999990001")
	session.setConnected(true)

	asked := 0
	session.createTheGroup = func(context.Context, *wm.Client, keyedCreate) (*waTypes.GroupInfo, error) {
		asked++
		return aMadeGroup("120363041234567890", "Obras",
			waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)), nil
	}
	// The store stops answering between the payload being accepted and the intent going
	// down, which is the one instant this refusal is about.
	if err := container.Close(); err != nil {
		t.Fatalf("Close the store: %v", err)
	}

	if _, err := session.Execute(t.Context(),
		namedCreate("c1", "once", `{"subject":"Obras","participants":[]}`)); err == nil {
		t.Fatal("a group was made with nothing written down to stop a redelivery making another")
	}
	if asked != 0 {
		t.Fatal("WhatsApp was asked for a group the connector could not record having asked for")
	}
}

// A refusal from WhatsApp is an answer, and the answer is that no group exists. The intent
// has to go with it: an attempt left open waits on a notification about a group that was
// never made, and it waits until the sweep takes the row.
func TestACreationWhatsAppRefusedDoesNotBlockTheNextRequestForThatName(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	impatient(session)
	self := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)

	refuse := true
	session.createTheGroup = func(context.Context, *wm.Client, keyedCreate) (*waTypes.GroupInfo, error) {
		if refuse {
			return nil, &wm.IQError{Code: 400, Text: "bad-request"}
		}
		return aMadeGroup("120363041234567890", "Obras", self), nil
	}
	session.joinedGroups = func(context.Context, *wm.Client) ([]*waTypes.GroupInfo, error) {
		return nil, nil
	}

	payload := `{"subject":"Obras","participants":[]}`
	if _, err := session.Execute(t.Context(), namedCreate("c1", "refused", payload)); err == nil {
		t.Fatal("a creation WhatsApp refused was answered as if it had worked")
	}
	if _, found, err := session.store.GroupCreation(t.Context(), "idem:refused"); err != nil {
		t.Fatalf("read the attempt: %v", err)
	} else if found {
		t.Fatal("the refused attempt stayed on record, where it waits on a notification that is not coming")
	}

	// And the next request for a group by that name goes through rather than waiting on it.
	refuse = false
	if _, err := session.Execute(t.Context(), namedCreate("c2", "next", payload)); err != nil {
		t.Fatalf("the next request for that name: %v", err)
	}
}

// A creation whose stanza never left is a creation that made nothing, the same as one
// WhatsApp refused. Left on record, it would have every later request under that name wait
// on a notification about a group nobody ever asked for.
func TestACreationThatNeverLeftLeavesNoAttempt(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)

	session.createTheGroup = func(context.Context, *wm.Client, keyedCreate) (*waTypes.GroupInfo, error) {
		return nil, errCreateUnsent
	}

	if _, err := session.Execute(t.Context(),
		namedCreate("c1", "unsent", `{"subject":"Obras","participants":[]}`)); err == nil {
		t.Fatal("a creation that never left was answered as if it had worked")
	}
	if _, found, err := session.store.GroupCreation(t.Context(), "idem:unsent"); err != nil {
		t.Fatalf("read the attempt: %v", err)
	} else if found {
		t.Fatal("a creation that never left stayed on record, where it waits on a notification that is not coming")
	}
}

// namedGroup reads the group id out of a `group.create` answer.
func namedGroup(t *testing.T, answer json.RawMessage) string {
	t.Helper()
	var described groupInfo
	if err := json.Unmarshal(answer, &described); err != nil {
		t.Fatalf("unmarshal the answer: %v", err)
	}
	return described.Group.ID + "@" + waTypes.GroupServer
}

// The stanza is this connector's own now, so what it says is this connector's to prove. The
// key is what WhatsApp echoes and the whole mechanism rests on; the three settings after
// the participants are what whatsmeow's own `CreateGroup` fills in, and a group created
// without them is not the group this command used to make.
func TestTheCreateStanzaCarriesTheKeyAndTheSettingsWhatsmeowWouldHaveSent(t *testing.T) {
	t.Parallel()

	person := waBinary.Node{Tag: "participant", Attrs: waBinary.Attrs{"jid": "5511999990002@s.whatsapp.net"}}
	stanza := theCreateStanza("Obras", "WACKEY", []waBinary.Node{person})

	if stanza.Tag != "create" {
		t.Fatalf("the stanza is a %q, want a create", stanza.Tag)
	}
	if got := stanza.Attrs["key"]; got != "WACKEY" {
		t.Fatalf("the stanza went out under %v, want the key the attempt was filed under", got)
	}
	if got := stanza.Attrs["subject"]; got != "Obras" {
		t.Fatalf("the group would be called %v, want what was asked", got)
	}
	children, ok := stanza.Content.([]waBinary.Node)
	if !ok {
		t.Fatalf("the stanza carries %T, want a list of nodes", stanza.Content)
	}
	if len(children) != 4 || children[0].Tag != "participant" {
		t.Fatalf("the stanza carries %d nodes, want the participant and the three settings", len(children))
	}
	mode, found := stanza.GetOptionalChildByTag("member_add_mode")
	if !found || mode.Content != string(waTypes.GroupMemberAddModeAllMember) {
		t.Fatalf("member_add_mode is %v (found=%v), want every member able to add", mode.Content, found)
	}
	ephemeral, found := stanza.GetOptionalChildByTag("ephemeral")
	if !found || ephemeral.Attrs["expiration"] != 0 {
		t.Fatalf("ephemeral is %v (found=%v), want messages that do not disappear", ephemeral.Attrs, found)
	}
	approval, found := stanza.GetOptionalChildByTag("membership_approval_mode", "group_join")
	if !found || approval.Attrs["state"] != "off" {
		t.Fatalf("group_join is %v (found=%v), want joining without approval", approval.Attrs, found)
	}
}
