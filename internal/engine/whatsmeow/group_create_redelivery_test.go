package whatsmeow

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	wm "go.mau.fi/whatsmeow"
	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// namedCreate is the `group.create` a client sends, with the name it gives the request.
func namedCreate(id, key, payload string) *protocol.Command {
	return &protocol.Command{
		V: protocol.Version, ID: id, Type: protocol.CommandGroupCreate,
		SID: "s1", IdempotencyKey: key, Payload: json.RawMessage(payload),
	}
}

// aGroup is what WhatsApp answers a creation with, made at the instant it is asked for.
func aMadeGroup(jid, subject string, owner waTypes.JID) *waTypes.GroupInfo {
	return &waTypes.GroupInfo{
		JID:          waTypes.NewJID(jid, waTypes.GroupServer),
		OwnerJID:     owner,
		GroupName:    waTypes.GroupName{Name: subject},
		GroupCreated: time.Now(),
	}
}

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
	session.createTheGroup = func(context.Context, *wm.Client, wm.ReqCreateGroup) (*waTypes.GroupInfo, error) {
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

// The window the issue names, in its own shape: the group was made and nothing got as far as
// writing down which one. There is no record to answer from, so the group has to be found --
// which is what separates a creation from a send, and what the objection in `carryOut` to
// reserving before the fact does not cover.
func TestACreationWhoseRecordWasLostFindsTheGroupItMade(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	self := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)
	made := aMadeGroup("120363041234567890", "Obras", self)

	// The intent, written before WhatsApp was asked, is all the crashed attempt left.
	began := time.Now().Add(-time.Minute)
	if _, _, err := session.store.BeginGroupCreate(t.Context(), "idem:once", "Obras", began); err != nil {
		t.Fatalf("begin the attempt that crashed: %v", err)
	}
	made.GroupCreated = began.Add(time.Second)

	asked := 0
	session.createTheGroup = func(context.Context, *wm.Client, wm.ReqCreateGroup) (*waTypes.GroupInfo, error) {
		asked++
		return aMadeGroup("120363099999999999", "Obras", self), nil
	}
	session.joinedGroups = func(context.Context, *wm.Client) ([]*waTypes.GroupInfo, error) {
		return []*waTypes.GroupInfo{made}, nil
	}

	answer, err := session.Execute(t.Context(),
		namedCreate("c1", "once", `{"subject":"Obras","participants":[]}`))
	if err != nil {
		t.Fatalf("the redelivery: %v", err)
	}
	if asked != 0 {
		t.Fatal("WhatsApp was asked to make a group the earlier attempt had already made")
	}
	if got := namedGroup(t, answer); got != made.JID.String() {
		t.Fatalf("the redelivery answered %s, want the group that was already there, %s", got, made.JID)
	}
}

// The search has to be narrow enough that it does not swallow a creation somebody meant.
// Two requests with names of their own are two requests, however alike their payloads: the
// group the first one made is older than the second one's intent, so it is out of its window.
func TestTwoCreationsAskedSeparatelyBothHappen(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	self := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)

	var joined []*waTypes.GroupInfo
	asked := 0
	session.createTheGroup = func(context.Context, *wm.Client, wm.ReqCreateGroup) (*waTypes.GroupInfo, error) {
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

// The record is this session's, and so is the search. A group the account is in but did not
// make is somebody else's creation, and answering a command with it would hand a client the
// wrong conversation.
func TestARedeliveredCreationDoesNotAnswerWithAGroupThisAccountDidNotMake(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	somebodyElse := waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)

	began := time.Now().Add(-time.Minute)
	if _, _, err := session.store.BeginGroupCreate(t.Context(), "idem:once", "Obras", began); err != nil {
		t.Fatalf("begin the attempt that crashed: %v", err)
	}
	theirs := aMadeGroup("120363041234567890", "Obras", somebodyElse)
	theirs.GroupCreated = began.Add(time.Second)

	asked := 0
	session.createTheGroup = func(context.Context, *wm.Client, wm.ReqCreateGroup) (*waTypes.GroupInfo, error) {
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
		t.Fatal("the redelivery took somebody else's group for the one it was to make")
	}
	if got := namedGroup(t, answer); got == theirs.JID.String() {
		t.Fatalf("the redelivery answered with %s, a group this account did not make", got)
	}
}

// A group made before the intent was written cannot be this attempt's: the intent goes down
// first, so anything older than it belongs to something else. Without that bound, a second
// request for a group by a name the account already uses would be answered with the old one.
func TestARedeliveredCreationDoesNotAnswerWithAGroupOlderThanItsIntent(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	self := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)

	began := time.Now()
	if _, _, err := session.store.BeginGroupCreate(t.Context(), "idem:once", "Obras", began); err != nil {
		t.Fatalf("begin the attempt that crashed: %v", err)
	}
	older := aMadeGroup("120363041234567890", "Obras", self)
	older.GroupCreated = began.Add(-time.Hour)

	asked := 0
	session.createTheGroup = func(context.Context, *wm.Client, wm.ReqCreateGroup) (*waTypes.GroupInfo, error) {
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
		t.Fatal("the redelivery answered with a group that predates its own intent")
	}
	if got := namedGroup(t, answer); got == older.JID.String() {
		t.Fatalf("the redelivery answered with %s, which existed before the attempt began", got)
	}
}

// Whether the group exists is the question, so a reading that failed is not an answer.
// Falling through to a creation here is the duplicate, made deliberately.
func TestACreationThatCouldNotLookForTheGroupDoesNotMakeASecond(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)

	if _, _, err := session.store.BeginGroupCreate(
		t.Context(), "idem:once", "Obras", time.Now().Add(-time.Minute)); err != nil {
		t.Fatalf("begin the attempt that crashed: %v", err)
	}

	asked := 0
	session.createTheGroup = func(context.Context, *wm.Client, wm.ReqCreateGroup) (*waTypes.GroupInfo, error) {
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
// attempt filed for it would have the next delivery of the same name go looking for a group
// that was never asked for -- and, worse, take the first group it finds by that name.
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
		t.Fatal("a creation refused before the wire filed an attempt, which a later delivery would go looking from")
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
	session.createTheGroup = func(context.Context, *wm.Client, wm.ReqCreateGroup) (*waTypes.GroupInfo, error) {
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
	session.createTheGroup = func(context.Context, *wm.Client, wm.ReqCreateGroup) (*waTypes.GroupInfo, error) {
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

// WhatsApp dates a group to the second, and the intent carries this process's clock. The
// group a creation makes is stamped in the same second the intent was written, and often at
// a lower millisecond -- which is what made this the commonest timing, not an edge.
func TestARedeliveredCreationFindsAGroupStampedInTheSameSecondAsItsIntent(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	self := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)

	began := time.Now().Add(-time.Minute)
	if _, _, err := session.store.BeginGroupCreate(t.Context(), "idem:once", "Obras", began); err != nil {
		t.Fatalf("begin the attempt that crashed: %v", err)
	}
	// The second the intent fell in, dated the way WhatsApp dates it.
	made := aMadeGroup("120363041234567890", "Obras", self)
	made.GroupCreated = time.Unix(began.Truncate(time.Second).Unix(), 0)
	if !made.GroupCreated.Before(began) {
		t.Fatal("the fixture does not put the group before the intent, so it proves nothing")
	}

	asked := 0
	session.createTheGroup = func(context.Context, *wm.Client, wm.ReqCreateGroup) (*waTypes.GroupInfo, error) {
		asked++
		return aMadeGroup("120363099999999999", "Obras", self), nil
	}
	session.joinedGroups = func(context.Context, *wm.Client) ([]*waTypes.GroupInfo, error) {
		return []*waTypes.GroupInfo{made}, nil
	}

	answer, err := session.Execute(t.Context(),
		namedCreate("c1", "once", `{"subject":"Obras","participants":[]}`))
	if err != nil {
		t.Fatalf("the redelivery: %v", err)
	}
	if asked != 0 {
		t.Fatal("the group was made a second time because its own stamp read as older than its intent")
	}
	if got := namedGroup(t, answer); got != made.JID.String() {
		t.Fatalf("the redelivery answered %s, want %s", got, made.JID)
	}
}

// A group another attempt of this session has already recorded as its own is not this
// attempt's to take. Without that, one request is answered with another's conversation and
// the creation it asked for never happens.
func TestARedeliveredCreationDoesNotTakeAGroupAnotherAttemptAlreadyMade(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	self := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)

	began := time.Now().Add(-time.Minute)
	// This attempt wrote its intent and never created anything.
	if _, _, err := session.store.BeginGroupCreate(t.Context(), "idem:mine", "Obras", began); err != nil {
		t.Fatalf("begin the attempt that crashed: %v", err)
	}
	// Another request made a group by the same name afterwards, and recorded it.
	theirs := aMadeGroup("120363041234567890", "Obras", self)
	theirs.GroupCreated = began.Add(time.Second)
	if _, _, err := session.store.BeginGroupCreate(t.Context(), "idem:theirs", "Obras", began); err != nil {
		t.Fatalf("begin the other attempt: %v", err)
	}
	if err := session.store.FinishGroupCreate(t.Context(), "idem:theirs", theirs.JID.String()); err != nil {
		t.Fatalf("record the other attempt's group: %v", err)
	}

	asked := 0
	session.createTheGroup = func(context.Context, *wm.Client, wm.ReqCreateGroup) (*waTypes.GroupInfo, error) {
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
		t.Fatal("the retry took a group another request had already made and recorded, and never made its own")
	}
	if got := namedGroup(t, answer); got == theirs.JID.String() {
		t.Fatalf("the retry answered with %s, which belongs to another request", got)
	}
}

// The subject is what the attempt was to call the group, and a group by another name is
// another group however well its timing fits. An account that makes groups steadily would
// otherwise have a crashed attempt answered with whatever it made next.
func TestARedeliveredCreationDoesNotTakeAGroupByAnotherName(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	self := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)

	began := time.Now().Add(-time.Minute)
	if _, _, err := session.store.BeginGroupCreate(t.Context(), "idem:once", "Obras", began); err != nil {
		t.Fatalf("begin the attempt that crashed: %v", err)
	}
	// Made by this account, after the intent, and called something else entirely.
	other := aMadeGroup("120363041234567890", "Churrasco", self)
	other.GroupCreated = began.Add(time.Second)

	asked := 0
	session.createTheGroup = func(context.Context, *wm.Client, wm.ReqCreateGroup) (*waTypes.GroupInfo, error) {
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
		t.Fatal("the retry took a group by another name for the one it was to make")
	}
	if got := namedGroup(t, answer); got == other.JID.String() {
		t.Fatalf("the retry answered with %s, a group called something else", got)
	}
}

// What another session made is not this one's business, in either direction. The groups a
// search rules out are the ones this session has already filed, and a table read across
// every session would have one account's creations block another's retry.
func TestARedeliveredCreationIsNotBlockedByAnotherSessionsGroup(t *testing.T) {
	t.Parallel()

	session, container := newTestSession(t, "5511999990001")
	session.setConnected(true)
	self := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)

	began := time.Now().Add(-time.Minute)
	if _, _, err := session.store.BeginGroupCreate(t.Context(), "idem:once", "Obras", began); err != nil {
		t.Fatalf("begin the attempt that crashed: %v", err)
	}
	made := aMadeGroup("120363041234567890", "Obras", self)
	made.GroupCreated = began.Add(time.Second)

	// Another session filed the same group under a name of its own. Its table is not this
	// session's, and reading across the two would rule out the group this attempt made.
	elsewhere := container.For("another-session")
	if _, _, err := elsewhere.BeginGroupCreate(t.Context(), "idem:once", "Obras", began); err != nil {
		t.Fatalf("begin the other session's attempt: %v", err)
	}
	if err := elsewhere.FinishGroupCreate(t.Context(), "idem:once", made.JID.String()); err != nil {
		t.Fatalf("record the other session's group: %v", err)
	}

	asked := 0
	session.createTheGroup = func(context.Context, *wm.Client, wm.ReqCreateGroup) (*waTypes.GroupInfo, error) {
		asked++
		return aMadeGroup("120363099999999999", "Obras", self), nil
	}
	session.joinedGroups = func(context.Context, *wm.Client) ([]*waTypes.GroupInfo, error) {
		return []*waTypes.GroupInfo{made}, nil
	}

	answer, err := session.Execute(t.Context(),
		namedCreate("c1", "once", `{"subject":"Obras","participants":[]}`))
	if err != nil {
		t.Fatalf("the redelivery: %v", err)
	}
	if asked != 0 {
		t.Fatal("another session's record blocked this one from finding the group it had already made")
	}
	if got := namedGroup(t, answer); got != made.JID.String() {
		t.Fatalf("the redelivery answered %s, want %s", got, made.JID)
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
	session.createTheGroup = func(context.Context, *wm.Client, wm.ReqCreateGroup) (*waTypes.GroupInfo, error) {
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

// The group an attempt made is the first one this account created after the intent went
// down. Taking the newest match instead would hand back whatever the account made last, which
// on a busy account is somebody else's creation entirely.
func TestARedeliveredCreationTakesTheOldestGroupInItsWindow(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	self := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)

	began := time.Now().Add(-time.Hour)
	if _, _, err := session.store.BeginGroupCreate(t.Context(), "idem:once", "Obras", began); err != nil {
		t.Fatalf("begin the attempt that crashed: %v", err)
	}
	mine := aMadeGroup("120363041111111111", "Obras", self)
	mine.GroupCreated = began.Add(time.Second)
	later := aMadeGroup("120363042222222222", "Obras", self)
	later.GroupCreated = began.Add(30 * time.Minute)

	session.createTheGroup = func(context.Context, *wm.Client, wm.ReqCreateGroup) (*waTypes.GroupInfo, error) {
		t.Error("WhatsApp was asked for a group that was already there")
		return nil, nil
	}
	session.joinedGroups = func(context.Context, *wm.Client) ([]*waTypes.GroupInfo, error) {
		// Newest first, so an answer that takes the first match takes the wrong one.
		return []*waTypes.GroupInfo{later, mine}, nil
	}

	answer, err := session.Execute(t.Context(),
		namedCreate("c1", "once", `{"subject":"Obras","participants":[]}`))
	if err != nil {
		t.Fatalf("the redelivery: %v", err)
	}
	if got := namedGroup(t, answer); got != mine.JID.String() {
		t.Fatalf("the redelivery answered %s, want the oldest group in its window, %s", got, mine.JID)
	}
}

// Two requests for a group by the same name, both still open, make a group that is evidence
// for either and proof for neither. Answering one of them with it would hand that request the
// other's conversation and skip the creation it asked for, in silence.
func TestARedeliveredCreationRefusesWhileAnotherAttemptAtTheSameNameIsOpen(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	self := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)

	began := time.Now().Add(-time.Minute)
	if _, _, err := session.store.BeginGroupCreate(t.Context(), "idem:mine", "Obras", began); err != nil {
		t.Fatalf("begin this attempt: %v", err)
	}
	// Another request for a group by the same name, also unfinished.
	if _, _, err := session.store.BeginGroupCreate(t.Context(), "idem:theirs", "Obras", began); err != nil {
		t.Fatalf("begin the competing attempt: %v", err)
	}
	ambiguous := aMadeGroup("120363041234567890", "Obras", self)
	ambiguous.GroupCreated = began.Add(time.Second)

	asked := 0
	session.createTheGroup = func(context.Context, *wm.Client, wm.ReqCreateGroup) (*waTypes.GroupInfo, error) {
		asked++
		return aMadeGroup("120363099999999999", "Obras", self), nil
	}
	session.joinedGroups = func(context.Context, *wm.Client) ([]*waTypes.GroupInfo, error) {
		return []*waTypes.GroupInfo{ambiguous}, nil
	}

	answer, err := session.Execute(t.Context(),
		namedCreate("c1", "mine", `{"subject":"Obras","participants":[]}`))
	if err == nil {
		t.Fatalf("the redelivery answered %s over a group that is evidence for two requests",
			namedGroup(t, answer))
	}
	if asked != 0 {
		t.Fatal("a second group was made while which group belonged to this request was in doubt")
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
