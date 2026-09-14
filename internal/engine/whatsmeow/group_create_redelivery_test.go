package whatsmeow

import (
	"context"
	"encoding/json"
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

// A group by this name exists and WhatsApp has not said whose it is. Answering with it would
// hand this request a conversation it did not make, and making another would be the duplicate
// the issue is about, so neither happens: the command is refused and converges on the next
// delivery, once the notification has landed.
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
	ambiguous := aMadeGroup("120363041234567890", "Obras", self)
	ambiguous.GroupCreated = began.Add(time.Second)

	asked := 0
	session.createTheGroup = func(context.Context, *wm.Client, keyedCreate) (*waTypes.GroupInfo, error) {
		asked++
		return aMadeGroup("120363099999999999", "Obras", self), nil
	}
	session.joinedGroups = func(context.Context, *wm.Client) ([]*waTypes.GroupInfo, error) {
		return []*waTypes.GroupInfo{ambiguous}, nil
	}

	answer, err := session.Execute(t.Context(),
		namedCreate("c1", "once", `{"subject":"Obras","participants":[]}`))
	if err == nil {
		t.Fatalf("the redelivery answered %s over a group WhatsApp had not said was its own",
			namedGroup(t, answer))
	}
	if asked != 0 {
		t.Fatal("a second group was made while which group belonged to this request was in doubt")
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

// A redelivery that has to create after all sends the key its first delivery was filed
// under. A fresh key would leave the intent findable under one name and the group announced
// under another, which is the record failing at the one job it has.
func TestARedeliveryThatCreatesUsesTheKeyOnRecord(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	self := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)

	began := time.Now().Add(-time.Minute)
	if _, _, err := session.store.BeginGroupCreate(
		t.Context(), "idem:once", "WACFIRST", "Obras", began); err != nil {
		t.Fatalf("begin the attempt that crashed: %v", err)
	}

	var sent keyedCreate
	session.createTheGroup = func(_ context.Context, _ *wm.Client, req keyedCreate) (*waTypes.GroupInfo, error) {
		sent = req
		return aMadeGroup("120363041234567890", "Obras", self), nil
	}
	// Nothing by that name exists, so the attempt certainly made nothing and this one creates.
	session.joinedGroups = func(context.Context, *wm.Client) ([]*waTypes.GroupInfo, error) {
		return nil, nil
	}

	if _, err := session.Execute(t.Context(),
		namedCreate("c1", "once", `{"subject":"Obras","participants":[]}`)); err != nil {
		t.Fatalf("the redelivery: %v", err)
	}
	if sent.Key != "WACFIRST" {
		t.Fatalf("the redelivery created under %s, want the key the first delivery was filed under, WACFIRST", sent.Key)
	}
}

// Nothing by that name is on WhatsApp, so nothing was made, and the redelivery creates
// without waiting for a notification that is never coming. Waiting here would refuse every
// command whose instance died between writing its intent and sending anything.
func TestARedeliveryWithNothingOnWhatsAppCreatesWithoutWaiting(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	// Long enough that a wait would show up as a test that takes seconds.
	session.createWait = time.Minute
	self := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)

	if _, _, err := session.store.BeginGroupCreate(
		t.Context(), "idem:once", "WACGONE", "Obras", time.Now().Add(-time.Minute)); err != nil {
		t.Fatalf("begin the attempt that crashed: %v", err)
	}

	asked := 0
	session.createTheGroup = func(context.Context, *wm.Client, keyedCreate) (*waTypes.GroupInfo, error) {
		asked++
		return aMadeGroup("120363041234567890", "Obras", self), nil
	}
	session.joinedGroups = func(context.Context, *wm.Client) ([]*waTypes.GroupInfo, error) {
		return nil, nil
	}

	if _, err := session.Execute(t.Context(),
		namedCreate("c1", "once", `{"subject":"Obras","participants":[]}`)); err != nil {
		t.Fatalf("the redelivery: %v", err)
	}
	if asked != 1 {
		t.Fatalf("WhatsApp was asked for %d groups, want 1: a request whose group was never made went unanswered", asked)
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

// A group this account did not create was made by somebody else, so no attempt of this
// session is waiting to be told about it. Treating it as one would hold up a creation that
// never happened until the record is swept.
func TestAGroupThisAccountDidNotMakeDoesNotHoldUpACreation(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.createWait = time.Minute
	somebodyElse := waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)

	began := time.Now().Add(-time.Minute)
	if _, _, err := session.store.BeginGroupCreate(
		t.Context(), "idem:once", "WACMINE", "Obras", began); err != nil {
		t.Fatalf("begin the attempt that crashed: %v", err)
	}
	theirs := aMadeGroup("120363041234567890", "Obras", somebodyElse)
	theirs.GroupCreated = began.Add(time.Second)

	asked := 0
	session.createTheGroup = func(context.Context, *wm.Client, keyedCreate) (*waTypes.GroupInfo, error) {
		asked++
		return aMadeGroup("120363099999999999", "Obras",
			waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)), nil
	}
	session.joinedGroups = func(context.Context, *wm.Client) ([]*waTypes.GroupInfo, error) {
		return []*waTypes.GroupInfo{theirs}, nil
	}

	answer, err := session.Execute(t.Context(),
		namedCreate("c1", "once", `{"subject":"Obras","participants":[]}`))
	if err != nil {
		t.Fatalf("the redelivery: %v", err)
	}
	if asked != 1 {
		t.Fatal("somebody else's group held up a creation that had not happened")
	}
	if got := namedGroup(t, answer); got == theirs.JID.String() {
		t.Fatalf("the redelivery answered with %s, a group this account did not make", got)
	}
}

// An owner this build cannot read is not evidence that somebody else made the group. The
// conservative reading is the one that does not make a second group.
func TestAGroupWithNoReadableOwnerStillHoldsUpACreation(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	impatient(session)

	began := time.Now().Add(-time.Minute)
	if _, _, err := session.store.BeginGroupCreate(
		t.Context(), "idem:once", "WACANON", "Obras", began); err != nil {
		t.Fatalf("begin the attempt that crashed: %v", err)
	}
	anonymous := aMadeGroup("120363041234567890", "Obras", waTypes.EmptyJID)
	anonymous.GroupCreated = began.Add(time.Second)

	asked := 0
	session.createTheGroup = func(context.Context, *wm.Client, keyedCreate) (*waTypes.GroupInfo, error) {
		asked++
		return aMadeGroup("120363099999999999", "Obras",
			waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)), nil
	}
	session.joinedGroups = func(context.Context, *wm.Client) ([]*waTypes.GroupInfo, error) {
		return []*waTypes.GroupInfo{anonymous}, nil
	}

	if _, err := session.Execute(t.Context(),
		namedCreate("c1", "once", `{"subject":"Obras","participants":[]}`)); err == nil {
		t.Fatal("a group whose owner could not be read was taken as somebody else's, and a second group was made")
	}
	if asked != 0 {
		t.Fatalf("WhatsApp was asked for %d groups, want 0", asked)
	}
}

// A group made before the intent was written cannot be this attempt's: the intent goes down
// first, so anything older than it belongs to something else. Without that bound, a request
// for a group by a name the account already uses would never be carried out.
func TestAGroupOlderThanTheIntentDoesNotHoldUpACreation(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.createWait = time.Minute
	self := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)

	began := time.Now()
	if _, _, err := session.store.BeginGroupCreate(
		t.Context(), "idem:once", "WACOLD", "Obras", began); err != nil {
		t.Fatalf("begin the attempt that crashed: %v", err)
	}
	older := aMadeGroup("120363041234567890", "Obras", self)
	older.GroupCreated = began.Add(-time.Hour)

	asked := 0
	session.createTheGroup = func(context.Context, *wm.Client, keyedCreate) (*waTypes.GroupInfo, error) {
		asked++
		return aMadeGroup("120363099999999999", "Obras", self), nil
	}
	session.joinedGroups = func(context.Context, *wm.Client) ([]*waTypes.GroupInfo, error) {
		return []*waTypes.GroupInfo{older}, nil
	}

	answer, err := session.Execute(t.Context(),
		namedCreate("c1", "once", `{"subject":"Obras","participants":[]}`))
	if err != nil {
		t.Fatalf("the redelivery: %v", err)
	}
	if asked != 1 {
		t.Fatal("a group that predates the intent held up the creation the intent was for")
	}
	if got := namedGroup(t, answer); got == older.JID.String() {
		t.Fatalf("the redelivery answered with %s, which existed before the attempt began", got)
	}
}

// WhatsApp dates a group to the second, and the intent carries this process's clock. The
// group a creation makes is stamped in the same second the intent was written, and often at
// a lower millisecond -- which is what made this the commonest timing, not an edge. Read as
// older than its own intent, that group would be passed over and made again.
func TestAGroupStampedInTheSameSecondAsTheIntentStillHoldsUpACreation(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	impatient(session)
	self := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)

	began := time.Now().Add(-time.Minute)
	if _, _, err := session.store.BeginGroupCreate(
		t.Context(), "idem:once", "WACSAME", "Obras", began); err != nil {
		t.Fatalf("begin the attempt that crashed: %v", err)
	}
	// The second the intent fell in, dated the way WhatsApp dates it.
	made := aMadeGroup("120363041234567890", "Obras", self)
	made.GroupCreated = time.Unix(began.Truncate(time.Second).Unix(), 0)
	if !made.GroupCreated.Before(began) {
		t.Fatal("the fixture does not put the group before the intent, so it proves nothing")
	}

	asked := 0
	session.createTheGroup = func(context.Context, *wm.Client, keyedCreate) (*waTypes.GroupInfo, error) {
		asked++
		return aMadeGroup("120363099999999999", "Obras", self), nil
	}
	session.joinedGroups = func(context.Context, *wm.Client) ([]*waTypes.GroupInfo, error) {
		return []*waTypes.GroupInfo{made}, nil
	}

	if _, err := session.Execute(t.Context(),
		namedCreate("c1", "once", `{"subject":"Obras","participants":[]}`)); err == nil {
		t.Fatal("the group was made a second time because its own stamp read as older than its intent")
	}
	if asked != 0 {
		t.Fatalf("WhatsApp was asked for %d groups, want 0", asked)
	}
}

// A group another attempt of this session has already been told is its own answers to that
// attempt's key. Leaving it in the way would hold up a creation that never happened.
func TestAGroupAnotherAttemptWasGivenDoesNotHoldUpACreation(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.createWait = time.Minute
	self := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)

	began := time.Now().Add(-time.Minute)
	// This attempt wrote its intent and never created anything.
	if _, _, err := session.store.BeginGroupCreate(
		t.Context(), "idem:mine", "WACMINE", "Obras", began); err != nil {
		t.Fatalf("begin the attempt that crashed: %v", err)
	}
	// Another request made a group by the same name afterwards, and WhatsApp named it.
	theirs := aMadeGroup("120363041234567890", "Obras", self)
	theirs.GroupCreated = began.Add(time.Second)
	if _, _, err := session.store.BeginGroupCreate(
		t.Context(), "idem:theirs", "WACTHEIRS", "Obras", began); err != nil {
		t.Fatalf("begin the other attempt: %v", err)
	}
	if _, err := session.store.FinishGroupCreateByKey(
		t.Context(), "WACTHEIRS", theirs.JID.String()); err != nil {
		t.Fatalf("record the other attempt's group: %v", err)
	}

	asked := 0
	session.createTheGroup = func(context.Context, *wm.Client, keyedCreate) (*waTypes.GroupInfo, error) {
		asked++
		return aMadeGroup("120363099999999999", "Obras", self), nil
	}
	session.joinedGroups = func(context.Context, *wm.Client) ([]*waTypes.GroupInfo, error) {
		return []*waTypes.GroupInfo{theirs}, nil
	}

	answer, err := session.Execute(t.Context(),
		namedCreate("c1", "mine", `{"subject":"Obras","participants":[]}`))
	if err != nil {
		t.Fatalf("the redelivery: %v", err)
	}
	if asked != 1 {
		t.Fatal("a group another request had already been given held up this one's creation")
	}
	if got := namedGroup(t, answer); got == theirs.JID.String() {
		t.Fatalf("the retry answered with %s, which belongs to another request", got)
	}
}

// The subject is what the attempt was to call the group, and a group by another name is
// another group however well its timing fits. An account that makes groups steadily would
// otherwise have every creation held up by whatever it made last.
func TestAGroupByAnotherNameDoesNotHoldUpACreation(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.createWait = time.Minute
	self := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)

	began := time.Now().Add(-time.Minute)
	if _, _, err := session.store.BeginGroupCreate(
		t.Context(), "idem:once", "WACNAME", "Obras", began); err != nil {
		t.Fatalf("begin the attempt that crashed: %v", err)
	}
	// Made by this account, after the intent, and called something else entirely.
	other := aMadeGroup("120363041234567890", "Churrasco", self)
	other.GroupCreated = began.Add(time.Second)

	asked := 0
	session.createTheGroup = func(context.Context, *wm.Client, keyedCreate) (*waTypes.GroupInfo, error) {
		asked++
		return aMadeGroup("120363099999999999", "Obras", self), nil
	}
	session.joinedGroups = func(context.Context, *wm.Client) ([]*waTypes.GroupInfo, error) {
		return []*waTypes.GroupInfo{other}, nil
	}

	answer, err := session.Execute(t.Context(),
		namedCreate("c1", "once", `{"subject":"Obras","participants":[]}`))
	if err != nil {
		t.Fatalf("the redelivery: %v", err)
	}
	if asked != 1 {
		t.Fatal("a group called something else held up the creation")
	}
	if got := namedGroup(t, answer); got == other.JID.String() {
		t.Fatalf("the retry answered with %s, a group called something else", got)
	}
}

// Whether a group exists is the question, so a reading that failed is not an answer.
// Falling through to a creation here is the duplicate, made deliberately.
func TestACreationThatCouldNotLookForTheGroupDoesNotMakeASecond(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)

	if _, _, err := session.store.BeginGroupCreate(
		t.Context(), "idem:once", "WACBLIND", "Obras", time.Now().Add(-time.Minute)); err != nil {
		t.Fatalf("begin the attempt that crashed: %v", err)
	}

	asked := 0
	session.createTheGroup = func(context.Context, *wm.Client, keyedCreate) (*waTypes.GroupInfo, error) {
		asked++
		return aMadeGroup("120363099999999999", "Obras",
			waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)), nil
	}
	session.joinedGroups = func(context.Context, *wm.Client) ([]*waTypes.GroupInfo, error) {
		return nil, wm.ErrIQDisconnected
	}

	if _, err := session.Execute(t.Context(),
		namedCreate("c1", "once", `{"subject":"Obras","participants":[]}`)); err == nil {
		t.Fatal("the creation went ahead over a reading that failed, which is how the duplicate gets made")
	}
	if asked != 0 {
		t.Fatal("WhatsApp was asked for a group while whether it already existed was unknown")
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
