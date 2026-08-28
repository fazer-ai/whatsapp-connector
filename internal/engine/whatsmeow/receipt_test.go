package whatsmeow

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	wm "go.mau.fi/whatsmeow"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// The four the contract names, from the six whatsmeow shapes that mean them. What is
// being pinned is the mapping and nothing else: it is the only decision in the file,
// and every one of its rows is a claim about what WhatsApp meant.
func TestAReceiptIsPublishedUnderTheNameTheContractHasForIt(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		kind waTypes.ReceiptType
		want string
	}{
		{"reached the device", waTypes.ReceiptTypeDelivered, "delivered"},
		{"the chat was opened", waTypes.ReceiptTypeRead, "read"},
		// The account read it somewhere else, which is the same fact about the same
		// message and the only reason a client can sync an unread count at all.
		{"read from another of this account's devices", waTypes.ReceiptTypeReadSelf, "read"},
		{"a view-once was opened", waTypes.ReceiptTypePlayed, "played"},
		{"a view-once opened from another of this account's devices", waTypes.ReceiptTypePlayedSelf, "played"},
		{"whatsapp could not deliver it", waTypes.ReceiptTypeServerError, "failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			payload := decode(t, receiptPublishedBy(t, session, receiptEvent(tc.kind, "3EB0RECEIPT")).Payload)

			if payload["type"] != tc.want {
				t.Errorf("a %q receipt was published as %v, want %q", tc.kind, payload["type"], tc.want)
			}
			ids, _ := payload["message_ids"].([]any)
			if len(ids) != 1 || ids[0] != "3EB0RECEIPT" {
				t.Errorf("the receipt names %v", payload["message_ids"])
			}
			if payload["timestamp"] != float64(1755440000123) {
				t.Errorf("the receipt is stamped %v, and WhatsApp said 1755440000123", payload["timestamp"])
			}
		})
	}
}

// Not a name the contract has, and the two reasons could not be further apart: one is
// this account's own fan-out and the plumbing under it, the other is a recipient asking
// for the message again -- which whatsmeow answers on its own, and which a client told
// "failed" would show an error for while it is on its way.
func TestAReceiptTheContractDoesNotNameIsAcknowledgedRatherThanPublished(t *testing.T) {
	t.Parallel()

	for _, kind := range []waTypes.ReceiptType{
		waTypes.ReceiptTypeSender,
		waTypes.ReceiptTypeRetry,
		waTypes.ReceiptTypeInactive,
		waTypes.ReceiptTypePeerMsg,
		waTypes.ReceiptTypeHistorySync,
		waTypes.ReceiptType("something-this-build-has-never-seen"),
	} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			session.deliverWait = 50 * time.Millisecond

			acknowledged := make(chan bool, 1)
			go func() { acknowledged <- session.receipt(receiptEvent(kind, "3EB0RECEIPT")) }()
			select {
			case emission := <-session.Events():
				t.Fatalf("something the contract cannot name was published as %s: %s", emission.Type, emission.Payload)
			case got := <-acknowledged:
				if !got {
					t.Fatal("a receipt no build will ever publish was left for WhatsApp to send again")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("the handler never came back")
			}
		})
	}
}

// Who the receipt is from, which is what separates the two things `read` can mean. In a
// direct chat it repeats the chat's own address when the contact reported it, and names
// this account when one of its own devices did.
func TestAReceiptSaysWhoseDeviceReportedIt(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		sender waTypes.JID
		want   map[string]any
	}{
		{"the contact, in their own chat", waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
			map[string]any{"kind": "phone", "id": "5511999990002"}},
		{"this account, from another device", waTypes.NewJID("5511999990001", waTypes.DefaultUserServer),
			map[string]any{"kind": "phone", "id": "5511999990001"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			event := receiptEvent(waTypes.ReceiptTypeRead, "3EB0RECEIPT")
			event.Sender = tc.sender

			payload := decode(t, receiptPublishedBy(t, session, event).Payload)
			participant, _ := payload["participant"].(map[string]any)
			if participant["kind"] != tc.want["kind"] || participant["id"] != tc.want["id"] {
				t.Errorf("the receipt is from %v, want %v", payload["participant"], tc.want)
			}
		})
	}
}

// A failed receipt is the one that carries a reason, and the reason says only what
// whatsmeow knows. Nothing on the event says whether sending it again would work, so
// nothing here claims it would.
func TestAFailedReceiptCarriesWhatWhatsappActuallySaid(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	payload := decode(t, receiptPublishedBy(t, session,
		receiptEvent(waTypes.ReceiptTypeServerError, "3EB0RECEIPT")).Payload)

	failure, _ := payload["error"].(map[string]any)
	if failure["code"] != string(protocol.ErrorWaError) {
		t.Errorf("the failure is coded %v, want %q", failure["code"], protocol.ErrorWaError)
	}
	if failure["message"] == nil || failure["message"] == "" {
		t.Error("the failure says nothing at all")
	}

	delivered := decode(t, receiptPublishedBy(t, session,
		receiptEvent(waTypes.ReceiptTypeDelivered, "3EB0RECEIPT2")).Payload)
	if _, carried := delivered["error"]; carried {
		t.Errorf("a receipt that is not a failure carried a failure: %v", delivered["error"])
	}
}

// A receipt over a chat the contract cannot name, which is every kind of address
// WhatsApp has that the enum does not: a bot, an interop bridge, the Messenger servers.
// Publishing one would hand a client an address it cannot act on.
func TestAReceiptOverAChatTheContractCannotNameIsNotPublished(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.deliverWait = 50 * time.Millisecond
	event := receiptEvent(waTypes.ReceiptTypeRead, "3EB0RECEIPT")
	event.Chat = waTypes.NewJID("12345", "msgr")

	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.receipt(event) }()
	select {
	case emission := <-session.Events():
		t.Fatalf("a receipt over an unnameable chat was published: %s", emission.Payload)
	case <-acknowledged:
	case <-time.After(2 * time.Second):
		t.Fatal("the handler never came back")
	}
}

// A receipt naming no message at all. WhatsApp does not send one, and publishing it
// would put an event on the wire that says something happened to nothing.
func TestAReceiptOverNoMessageIsNotPublished(t *testing.T) {
	t.Parallel()

	event := receiptEvent(waTypes.ReceiptTypeRead)
	if published, ok := receiptOf(event); ok {
		t.Errorf("a receipt over nothing was rendered as %v", published)
	}
}

func receiptEvent(kind waTypes.ReceiptType, ids ...string) *waEvents.Receipt {
	return &waEvents.Receipt{
		MessageSource: waTypes.MessageSource{
			Chat:   waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
			Sender: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
		},
		MessageIDs: ids,
		Timestamp:  time.UnixMilli(1755440000123),
		Type:       kind,
	}
}

func receiptPublishedBy(t *testing.T, session *Session, event *waEvents.Receipt) engine.Emission {
	t.Helper()

	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.receipt(event) }()
	emission := next(t, session)
	if emission.Type != protocol.EventMessageReceipt {
		t.Fatalf("a receipt was published as %s", emission.Type)
	}
	emission.Settle(nil)
	select {
	case got := <-acknowledged:
		if !got {
			t.Fatal("a published receipt was left unacknowledged, so WhatsApp will send it again")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the handler never came back")
	}
	return emission
}

// The command's own refusals. Everything here is rejected before a socket is involved,
// which is the point: a read mark that names nothing, or names a state WhatsApp has no
// receipt for, is a caller error and answering it with a plausible success is how a
// client comes to believe a conversation was marked read when nothing was sent.
func TestAReadMarkThatSaysNothingUsefulIsRefused(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		payload string
	}{
		{"not a payload at all", `[]`},
		{"no messages", `{"chat":{"kind":"phone","id":"5511999990002"},"message_ids":[]}`},
		{"no messages and no field for them", `{"chat":{"kind":"phone","id":"5511999990002"}}`},
		// WhatsApp has eleven receipt types and the contract lets a client ask for two.
		// A third is a caller reaching for something this connector will not invent.
		{"a state that is not a read or a played", `{"chat":{"kind":"phone","id":"5511999990002"},
			"message_ids":["3EB0"],"type":"delivered"}`},
		{"a chat the contract cannot name", `{"chat":{"kind":"bot","id":"5511999990002"},
			"message_ids":["3EB0"]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _, _ := outboundSession(t)
			_, err := session.markRead(t.Context(), &protocol.Command{
				Type:    protocol.CommandMessageMarkRead,
				Payload: json.RawMessage(tc.payload),
			})
			assertCode(t, err, protocol.ErrorInvalidPayload)
		})
	}
}

// `type` is optional on the command, and a mark with none is a read. Decoded any other
// way an ordinary read mark would be refused as an unknown state.
func TestAReadMarkWithNoStateIsARead(t *testing.T) {
	t.Parallel()

	if kind, named := markReadKinds[""]; !named || kind != waTypes.ReceiptTypeRead {
		t.Fatalf("a mark that named no state resolved to %q, %v", kind, named)
	}
	if kind := markReadKinds["played"]; kind != waTypes.ReceiptTypePlayed {
		t.Fatalf("a played mark resolved to %q", kind)
	}
}

// A group receipt names whose message it is on, and whatsmeow puts that on the node only
// when it has one: without it the write succeeds and the mark lands nowhere. A direct
// chat is the opposite -- whatsmeow drops the participant there -- so the requirement is
// the group's alone.
func TestAReadMarkInAGroupHasToSayWhoseMessagesTheyAre(t *testing.T) {
	t.Parallel()

	session, _, _ := outboundSession(t)
	_, err := session.markRead(t.Context(), &protocol.Command{
		Type: protocol.CommandMessageMarkRead,
		Payload: json.RawMessage(`{"chat":{"kind":"group","id":"120363041234567890"},
			"message_ids":["3EB0A1B2C3D4E5F60718"]}`),
	})
	assertCode(t, err, protocol.ErrorInvalidPayload)

	// And a direct chat with no sender is not refused with it: there is nobody else the
	// receipt could be about.
	_, err = session.markRead(t.Context(), &protocol.Command{
		Type: protocol.CommandMessageMarkRead,
		Payload: json.RawMessage(`{"chat":{"kind":"phone","id":"5511999990002"},
			"message_ids":["3EB0A1B2C3D4E5F60718"]}`),
	})
	// It gets no further than the socket it does not have, which is a different answer
	// from being told its payload is wrong.
	assertCode(t, err, protocol.ErrorNotConnected)
}

// The id list is checked for what is in it and not only for how long it is. whatsmeow
// builds the receipt around ids[0] without looking at it, so an empty one goes out as a
// node naming no message, reports no error, and is remembered under its idempotency key.
func TestAReadMarkOverAMessageWithNoIDIsRefused(t *testing.T) {
	t.Parallel()

	session, _, _ := outboundSession(t)
	for _, payload := range []string{
		`{"chat":{"kind":"phone","id":"5511999990002"},"message_ids":[""]}`,
		`{"chat":{"kind":"phone","id":"5511999990002"},"message_ids":["3EB0A1B2C3D4E5F60718",""]}`,
	} {
		_, err := session.markRead(t.Context(), &protocol.Command{
			Type: protocol.CommandMessageMarkRead, Payload: json.RawMessage(payload),
		})
		assertCode(t, err, protocol.ErrorInvalidPayload)
	}
}

// What a caller branches on. Told `wa_error` for a session that is simply not connected,
// a client retries against WhatsApp instead of waiting for the session to come back --
// and the library's own text does not belong in a reply either way.
func TestAReadMarkThatCouldNotGoOutIsNamedInTheContractsOwnWords(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  error
		want protocol.ErrorCode
	}{
		{"no account to mark from", wm.ErrNotLoggedIn, protocol.ErrorNotPaired},
		{"not connected", wm.ErrNotConnected, protocol.ErrorNotConnected},
		{"the command's deadline", context.DeadlineExceeded, protocol.ErrorTimeout},
		{"whatsapp never answered", wm.ErrIQTimedOut, protocol.ErrorTimeout},
		{"anything else", errors.New("a socket write said something about a file descriptor"),
			protocol.ErrorWaError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := markFailure(tc.err)
			assertCode(t, err, tc.want)
			if strings.Contains(err.Error(), "file descriptor") {
				t.Errorf("the library's own words crossed into the reply: %v", err)
			}
		})
	}
}

// The other half of the group case: a client holding an address from an older event may
// well have the namespace the group no longer answers in. Sent as it came, the
// participant is one WhatsApp cannot resolve, and nothing says so. What is asserted here
// is the wiring -- that the read mark asks at all -- because the normalisation itself is
// covered where it lives.
func TestAReadMarkInAGroupNormalisesWhoWroteTheMessages(t *testing.T) {
	t.Parallel()

	session, _, _ := outboundSession(t)
	asked := make(chan waTypes.JID, 1)
	session.groupMode = func(_ context.Context, chat waTypes.JID) (waTypes.AddressingMode, error) {
		asked <- chat
		return waTypes.AddressingModeLID, nil
	}

	// The send itself has no socket to go out on, and that is not what is being read.
	_, _ = session.markRead(t.Context(), &protocol.Command{
		Type: protocol.CommandMessageMarkRead,
		Payload: json.RawMessage(`{"chat":{"kind":"group","id":"120363041234567890"},
			"sender":{"kind":"phone","id":"5511999990002"},
			"message_ids":["3EB0A1B2C3D4E5F60718"]}`),
	})

	select {
	case chat := <-asked:
		if chat.User != "120363041234567890" {
			t.Errorf("the read mark asked about %s", chat)
		}
	default:
		t.Fatal("the read mark sent the participant in whichever namespace it came in")
	}
}

// One receipt node is not one event. whatsmeow expands a grouped receipt into a dispatch
// per participant and carries on through the ones that fail, so a group read by six
// people is six calls into this handler. Six full waits is thirty times the budget and
// past the five minutes whatsmeow gives a node before it starts the next one alongside
// it -- and two node handlers at once is the ordering guarantee gone.
//
// What is measured is the wall clock across the burst, because that is the thing that
// breaks: the first call is allowed to wait, and the ones behind it are not.
func TestABurstOfReceiptsWaitsOnAStalledPublisherOnlyOnce(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.deliverWait = 200 * time.Millisecond

	const participants = 6
	started := time.Now()
	for range participants {
		if session.receipt(receiptEvent(waTypes.ReceiptTypeRead, "3EB0RECEIPT")) {
			t.Fatal("a receipt nobody published was acknowledged")
		}
	}
	spent := time.Since(started)

	// One budget for the burst, not one per participant. The bound is generous on
	// purpose: what would fail this is the linear growth, which is 6x away.
	if spent > 3*session.deliverWait {
		t.Errorf("six receipts against a stalled publisher spent %s, and the budget is %s",
			spent, session.deliverWait)
	}
}

// And the other half, which is what keeps the gate from being a one-way door: a
// publisher that comes back is asked again. Cleared only by a success, nothing would
// ever try, and the session would stop publishing receipts for good.
func TestAReceiptIsPublishedAgainOnceThePublisherAnswers(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.deliverWait = 100 * time.Millisecond

	if session.receipt(receiptEvent(waTypes.ReceiptTypeRead, "3EB0STALL")) {
		t.Fatal("a receipt nobody published was acknowledged")
	}
	if !session.publisherStalled() {
		t.Fatal("a publisher that answered nothing is not recorded as stalled")
	}

	// The publisher was slow rather than gone, so what it was handed is still queued.
	// Left there it would be the next thing read, and the check below would pass on the
	// wrong event.
	stale := next(t, session)
	stale.Settle(nil)

	// The window is the budget, so waiting it out is what a real burst does between
	// nodes. Measured against the recorded deadline rather than slept past blindly.
	for session.publisherStalled() {
		time.Sleep(10 * time.Millisecond)
	}
	receiptPublishedBy(t, session, receiptEvent(waTypes.ReceiptTypeRead, "3EB0AGAIN"))
	if session.publisherStalled() {
		t.Error("a publisher that answered is still recorded as stalled")
	}
}

// The boundary the message path already draws. A client that asked for direct chats only
// never got the group's messages, so a receipt about one is an event it could not apply
// even if it wanted to -- and withholding it would have WhatsApp redeliver every group
// receipt the account gets for as long as the session is up.
func TestAGroupReceiptIsDroppedWhenTheClientAskedForDirectChatsOnly(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.deliverWait = 50 * time.Millisecond
	event := receiptEvent(waTypes.ReceiptTypeRead, "3EB0RECEIPT")
	event.Chat = waTypes.NewJID("120363041234567890", waTypes.GroupServer)
	event.IsGroup = true

	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.receipt(event) }()
	select {
	case emission := <-session.Events():
		t.Fatalf("a group receipt reached a direct-only client: %s", emission.Payload)
	case got := <-acknowledged:
		if !got {
			t.Fatal("a group receipt nobody wants was left for WhatsApp to send again")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the handler never came back")
	}
}

// A message sent through a broadcast list is published under the direct chat with
// whoever sent it, because that is where the recipient's own phone shows it. A receipt
// published under the list would be about a message the client filed somewhere else.
//
// The field is not the one the message path reads: there the other party is the sender,
// and here the sender is this account's own device.
func TestABroadcastReceiptIsPublishedUnderTheChatItsMessageIsIn(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	event := receiptEvent(waTypes.ReceiptTypeReadSelf, "3EB0RECEIPT")
	event.Chat = waTypes.NewJID("5511999990001", waTypes.BroadcastServer)
	event.Sender = waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)
	event.IsFromMe = true
	event.BroadcastListOwner = waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)

	payload := decode(t, receiptPublishedBy(t, session, event).Payload)
	chat, _ := payload["chat"].(map[string]any)
	if chat["kind"] != "phone" || chat["id"] != "5511999990002" {
		t.Errorf("the receipt is filed under %v, and its message is in the direct chat with 5511999990002", chat)
	}
}

// The window is an interval, and an interval taken off the wall clock is not one: a host
// whose clock steps backwards -- an NTP correction, a suspend, a container's clock being
// set -- would hold the deadline ahead for as long as wall time took to catch up, and
// every receipt would be withheld until it did.
//
// What is read is the deadline itself, because that is where the difference is visible
// without moving the host's clock: an elapsed time since this process started is a
// handful of milliseconds, and a wall-clock stamp is nineteen digits.
func TestThePublisherWindowIsMeasuredOnAClockThatOnlyMovesForward(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.deliverWait = 60 * time.Millisecond

	session.publisherAnswered(false)
	if !session.publisherStalled() {
		t.Fatal("a publisher that answered nothing is not recorded as stalled")
	}
	// Any wall-clock reading in nanoseconds is past 1.5e18 and has been since 2017. An
	// elapsed time cannot reach it without this process having run for fifty years.
	if until := session.stalledUntil.Load(); until > int64(time.Hour) {
		t.Errorf("the deadline is %d, which is a wall-clock stamp rather than an elapsed time", until)
	}

	// And it still runs out, which is what keeps the gate from being a one-way door.
	deadline := time.Now().Add(2 * time.Second)
	for session.publisherStalled() {
		if time.Now().After(deadline) {
			t.Fatal("the window never ran out")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
