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

			err := markFailure(tc.err, "WhatsApp did not take the read mark")
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
// people is six calls into this handler. Six full waits is past the five minutes
// whatsmeow gives a node before it starts the next one alongside it -- and two node
// handlers at once is the ordering guarantee gone.
//
// What is asserted is how many of the six reached the publisher, rather than how long
// the burst took: elapsed wall time answers the same question and answers it differently
// on a loaded machine. The clock is held still so the window cannot expire mid-burst.
func TestABurstOfReceiptsWaitsOnAStalledPublisherOnlyOnce(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.deliverWait = 50 * time.Millisecond
	session.elapsed = func() time.Duration { return 0 }

	const participants = 6
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range participants {
			if session.receipt(receiptEvent(waTypes.ReceiptTypeRead, "3EB0RECEIPT")) {
				t.Error("a receipt nobody published was acknowledged")
				return
			}
		}
	}()

	// The one that was handed over, which is the gate letting the first through.
	handed := next(t, session)
	handed.Settle(errors.New("the publisher is down"))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the burst never finished")
	}
	// And nothing else was. Every call has returned by now, so anything the gate let
	// through is already queued behind this one.
	select {
	case extra := <-session.Events():
		t.Fatalf("a second receipt reached a publisher that had stopped answering: %s", extra.Payload)
	default:
	}
}

// And the other half, which is what keeps the gate from being a one-way door: the window
// runs out and the publisher is asked again. Cleared only by a success, nothing would
// ever try, and the session would stop publishing receipts for good.
//
// The clock is held rather than waited on, so what is asserted is the rule and not the
// scheduler.
func TestAReceiptIsPublishedAgainOnceTheWindowRunsOut(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.deliverWait = time.Minute
	now := time.Duration(0)
	session.elapsed = func() time.Duration { return now }

	session.publisherAnswered(false)
	if !session.publisherStalled() {
		t.Fatal("a publisher that answered nothing is not recorded as stalled")
	}
	now = session.deliverWait - time.Nanosecond
	if !session.publisherStalled() {
		t.Error("the window ran out early")
	}
	now = session.deliverWait
	if session.publisherStalled() {
		t.Error("the window never ran out, so nothing would ever ask the publisher again")
	}

	// And a publisher that answers clears it outright, rather than leaving the rest of
	// the window to expire over a publisher that is already back.
	session.publisherAnswered(false)
	session.publisherAnswered(true)
	if session.publisherStalled() {
		t.Error("a publisher that answered is still recorded as stalled")
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
	session.deliverWait = time.Minute

	session.publisherAnswered(false)
	// Any wall-clock reading in nanoseconds is past 1.5e18 and has been since 2017. An
	// elapsed time cannot reach it without this process having run for fifty years.
	if until := session.stalledUntil.Load(); until > int64(time.Hour) {
		t.Errorf("the deadline is %d, which is a wall-clock stamp rather than an elapsed time", until)
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

// A read receipt cannot be taken back, and whatsmeow decides whether to send one or its
// silent form by reading the account's own privacy setting -- swallowing the error and
// carrying on with an empty one, which is not `none` and so reads as receipts being on.
// An account that turned them off would have the read disclosed to the peer.
//
// This is the one place a failed query is answered with a refusal rather than carried on
// through, and the reason is the direction of the failure: the group's addressing fails
// closed, and nothing happens; this fails open, and there is no undoing it.
func TestAReadMarkIsRefusedWhenTheAccountsOwnSettingCannotBeRead(t *testing.T) {
	t.Parallel()

	const direct = `{"chat":{"kind":"phone","id":"5511999990002"},"message_ids":["3EB0A1B2C3D4E5F60718"]`

	t.Run("the setting could not be read", func(t *testing.T) {
		t.Parallel()

		session, _, _ := outboundSession(t)
		session.privacyKnown = func(context.Context) error {
			return errors.New("the privacy query came back with nothing in it")
		}
		_, err := session.markRead(t.Context(), &protocol.Command{
			Type: protocol.CommandMessageMarkRead, Payload: json.RawMessage(direct + `}`),
		})
		assertCode(t, err, protocol.ErrorWaError)
		if !strings.Contains(err.Error(), "read-receipt setting") {
			t.Errorf("the refusal reads %q, and what went wrong is the setting", err)
		}
	})

	// The setting is read, and what happens next is the socket's business. What is being
	// separated here is refusing before anything goes out from failing on the way out.
	t.Run("the setting was read", func(t *testing.T) {
		t.Parallel()

		session, _, _ := outboundSession(t)
		asked := false
		session.privacyKnown = func(context.Context) error { asked = true; return nil }
		_, err := session.markRead(t.Context(), &protocol.Command{
			Type: protocol.CommandMessageMarkRead, Payload: json.RawMessage(direct + `}`),
		})
		if !asked {
			t.Fatal("a read mark went out without the account's setting being read at all")
		}
		assertCode(t, err, protocol.ErrorNotConnected)
	})

	// Not asked where the answer could not change what goes out: whatsmeow downgrades a
	// read and not a played, and downgrades a newsletter whatever the setting says.
	// Asking there would refuse over a query nothing was going to use.
	for _, tc := range []struct{ name, payload string }{
		{"a played mark", direct + `,"type":"played"}`},
		{"a channel", `{"chat":{"kind":"newsletter","id":"120363041234567890@newsletter"},
			"message_ids":["3EB0A1B2C3D4E5F60718"]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _, _ := outboundSession(t)
			session.privacyKnown = func(context.Context) error {
				t.Error("the account's setting was read where it could not change the answer")
				return nil
			}
			if _, err := session.markRead(t.Context(), &protocol.Command{
				Type: protocol.CommandMessageMarkRead, Payload: json.RawMessage(tc.payload),
			}); err == nil {
				t.Fatal("a mark went out over a session with no socket")
			}
		})
	}
}

// A broadcast list and the status feed are chats whose messages have an author of their
// own, so a receipt in one is about somebody rather than about the chat. whatsmeow puts
// the participant on the node for every chat but a direct one, and without a sender it
// puts none: the write succeeds and the mark lands nowhere.
func TestAReadMarkWhereMessagesHaveAnAuthorHasToNameThem(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		chat   string
		author bool
	}{
		{"a group", `{"kind":"group","id":"120363041234567890"}`, true},
		{"a broadcast list", `{"kind":"broadcast","id":"5511999990001"}`, true},
		{"the status feed", `{"kind":"status","id":"status"}`, true},
		// A post's author is the channel, so the participant would repeat the chat and
		// requiring it would refuse a mark that works.
		{"a channel", `{"kind":"newsletter","id":"120363041234567890@newsletter"}`, false},
		{"a direct chat", `{"kind":"phone","id":"5511999990002"}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _, _ := outboundSession(t)
			session.privacyKnown = func(context.Context) error { return nil }
			_, err := session.markRead(t.Context(), &protocol.Command{
				Type: protocol.CommandMessageMarkRead,
				Payload: json.RawMessage(`{"chat":` + tc.chat +
					`,"message_ids":["3EB0A1B2C3D4E5F60718"]}`),
			})
			if tc.author {
				assertCode(t, err, protocol.ErrorInvalidPayload)
				return
			}
			// Not refused over the participant: it gets as far as the socket it does not
			// have, which is a different answer from being told its payload is wrong.
			assertCode(t, err, protocol.ErrorNotConnected)
		})
	}
}
