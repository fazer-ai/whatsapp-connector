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

// namedGroup reads the group id out of a `group.create` answer.
func namedGroup(t *testing.T, answer json.RawMessage) string {
	t.Helper()
	var described groupInfo
	if err := json.Unmarshal(answer, &described); err != nil {
		t.Fatalf("unmarshal the answer: %v", err)
	}
	return described.Group.ID + "@" + waTypes.GroupServer
}
