package whatsmeow

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

func TestAnAddressNamesTheKindTheContractHasForEachServer(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		jid  waTypes.JID
		want protocol.Address
	}{
		{"a phone number", waTypes.NewJID("5511999990001", waTypes.DefaultUserServer),
			protocol.Address{Kind: protocol.AddressPhone, ID: "5511999990001"}},
		{"the legacy phone server", waTypes.NewJID("5511999990001", waTypes.LegacyUserServer),
			protocol.Address{Kind: protocol.AddressPhone, ID: "5511999990001"}},
		{"a hidden identifier", waTypes.NewJID("112233445566", waTypes.HiddenUserServer),
			protocol.Address{Kind: protocol.AddressLID, ID: "112233445566"}},
		{"a group", waTypes.NewJID("120363000000000000", waTypes.GroupServer),
			protocol.Address{Kind: protocol.AddressGroup, ID: "120363000000000000"}},
		{"a newsletter", waTypes.NewJID("120363111111111111", waTypes.NewsletterServer),
			protocol.Address{Kind: protocol.AddressNewsletter, ID: "120363111111111111"}},
		{"a broadcast list", waTypes.NewJID("5511999990001-1600000000", waTypes.BroadcastServer),
			protocol.Address{Kind: protocol.AddressBroadcast, ID: "5511999990001-1600000000"}},
		{"the status feed", waTypes.StatusBroadcastJID,
			protocol.Address{Kind: protocol.AddressStatus, ID: "status"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, ok := addressOf(tc.jid)
			if !ok {
				t.Fatalf("%s is an address the contract has a kind for, and it was refused", tc.jid)
			}
			if got != tc.want {
				t.Fatalf("addressOf(%s) = %+v, want %+v", tc.jid, got, tc.want)
			}
		})
	}
}

func TestAnAddressTheContractCannotNameIsRefusedRatherThanGuessedAt(t *testing.T) {
	t.Parallel()

	// Every one of these is something WhatsApp really addresses and the contract's enum
	// deliberately does not carry. Rendering one under a kind it is not leaves a client
	// treating a bot or a bridged Messenger account as a phone number it can reply to.
	for _, tc := range []struct {
		name string
		jid  waTypes.JID
	}{
		{"a bot", waTypes.NewJID("13135550002", waTypes.BotServer)},
		// The same assistant on the ordinary phone server, which is how Meta AI reaches
		// most accounts. Its server says nothing; only the reserved range does.
		{"a bot on the phone server", waTypes.NewJID("13135550002", waTypes.DefaultUserServer)},
		{"an interop bridge", waTypes.NewJID("13135550002", waTypes.InteropServer)},
		{"a Messenger account", waTypes.NewJID("13135550002", waTypes.MessengerServer)},
		{"a server with no user", waTypes.NewJID("", waTypes.DefaultUserServer)},
		{"nothing at all", waTypes.EmptyJID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got, ok := addressOf(tc.jid); ok {
				t.Fatalf("addressOf(%s) named it %+v, and the contract has no kind for it", tc.jid, got)
			}
		})
	}
}

func TestASenderCarriesBothIdentifiersWhenWhatsAppSendsBoth(t *testing.T) {
	t.Parallel()

	// A chat addressed by LID: the sender is the hidden identifier and the phone number
	// arrives as the alternative. A client that has only ever stored one of the two has
	// to be able to tell it is the same person, which is what carrying both is for.
	info := &waTypes.MessageInfo{
		MessageSource: waTypes.MessageSource{
			Sender:    waTypes.NewJID("112233445566", waTypes.HiddenUserServer),
			SenderAlt: waTypes.NewJID("5511999990001", waTypes.DefaultUserServer),
		},
		PushName: "Alice",
	}

	party, named := partyOf(info)
	if !named || party == nil {
		t.Fatal("a sender WhatsApp named twice was left off the message")
	}
	if party.LID != "112233445566" || party.Phone != "5511999990001" {
		t.Fatalf("the party carries phone %q and lid %q, want both of WhatsApp's identifiers", party.Phone, party.LID)
	}
	if party.PushName != "Alice" {
		t.Fatalf("the party carries push name %q, want Alice", party.PushName)
	}
}

func TestASenderTheContractCannotNameStopsTheMessageRatherThanGoingOutEmpty(t *testing.T) {
	t.Parallel()

	// A bridged Messenger account: WhatsApp named a sender and the contract has no way
	// to say who it is. Publishing the message without the field is the tempting
	// answer and the wrong one, because a client reading an unattributed message in a
	// direct chat takes it for the person on the other side of it.
	info := &waTypes.MessageInfo{
		MessageSource: waTypes.MessageSource{Sender: waTypes.NewJID("13135550002", waTypes.MessengerServer)},
		PushName:      "Somebody",
	}

	party, named := partyOf(info)
	if named {
		t.Fatalf("a sender the contract cannot name was accepted as %+v", party)
	}
}

// The other half: a chat that has no per-message sender at all. A newsletter post is
// the ordinary case, and the contract makes `sender` nullable for it, so the message is
// fine and there is simply nobody to attribute it to.
func TestAChatWithNoSenderPublishesTheMessageWithoutOne(t *testing.T) {
	t.Parallel()

	info := &waTypes.MessageInfo{
		MessageSource: waTypes.MessageSource{
			Chat: waTypes.NewJID("120363111111111111", waTypes.NewsletterServer),
		},
	}

	party, named := partyOf(info)
	if !named {
		t.Fatal("a chat with no sender was refused, and the contract allows one")
	}
	if party != nil {
		t.Fatalf("a chat with no sender invented %+v", party)
	}
}

func TestAPlainTextMessageIsRenderedTheWayTheContractCarriesIt(t *testing.T) {
	t.Parallel()

	event := &waEvents.Message{
		Info: waTypes.MessageInfo{
			ID:        "3EB0ABCDEF",
			Timestamp: time.UnixMilli(1755000000000),
			MessageSource: waTypes.MessageSource{
				Chat:   waTypes.NewJID("5511999990001", waTypes.DefaultUserServer),
				Sender: waTypes.NewJID("5511999990001", waTypes.DefaultUserServer),
			},
			PushName: "Alice",
		},
		Message: &waE2E.Message{Conversation: proto.String("bom dia")},
	}

	message, ok := inboundOf(event)
	if !ok {
		t.Fatal("a plain text message is the one thing this build can carry, and it was refused")
	}
	if message.ID != "3EB0ABCDEF" || message.Timestamp != 1755000000000 {
		t.Fatalf("the message is %+v, want WhatsApp's own id and timestamp", message)
	}
	if body, isText := message.Content.(protocol.TextContent); !isText || body.Body != "bom dia" {
		t.Fatalf("the content is %+v, want the text WhatsApp sent", message.Content)
	}
	if message.QuotedID != "" || message.Mentions != nil || message.Ephemeral != 0 {
		t.Fatalf("a plain message invented a quote, a mention or a timer: %+v", message)
	}

	validateInboundAgainstContract(t, &message)
}

func TestAnExtendedTextMessageCarriesTheQuoteTheMentionsAndTheTimer(t *testing.T) {
	t.Parallel()

	event := &waEvents.Message{
		Info: waTypes.MessageInfo{
			ID:        "3EB0FEDCBA",
			Timestamp: time.UnixMilli(1755000001000),
			MessageSource: waTypes.MessageSource{
				Chat:   waTypes.NewJID("120363000000000000", waTypes.GroupServer),
				Sender: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
			},
		},
		Message: &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{
			Text: proto.String("@5511999990001 replying to that"),
			ContextInfo: &waE2E.ContextInfo{
				StanzaID:   proto.String("3EB0ABCDEF"),
				Expiration: proto.Uint32(604800),
				MentionedJID: []string{
					"5511999990001@" + waTypes.DefaultUserServer,
					// A bot the contract has no kind for, and a JID that does not parse.
					// Losing a mention is not worth withholding the body it annotates.
					"13135550002@" + waTypes.BotServer,
					"not a jid",
				},
			},
		}},
	}

	message, ok := inboundOf(event)
	if !ok {
		t.Fatal("an extended text message was refused")
	}
	if message.QuotedID != "3EB0ABCDEF" {
		t.Fatalf("the quote is %q, want the id of the message it answers", message.QuotedID)
	}
	if message.Ephemeral != 604800 {
		t.Fatalf("the disappearing timer is %d, want the chat's own", message.Ephemeral)
	}
	want := []protocol.Address{{Kind: protocol.AddressPhone, ID: "5511999990001"}}
	if len(message.Mentions) != len(want) || message.Mentions[0] != want[0] {
		t.Fatalf("the mentions are %+v, want only the one the contract can name", message.Mentions)
	}

	validateInboundAgainstContract(t, &message)
}

func TestAMessageThisBuildCannotRenderIsLeftUnacknowledged(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")

	// An image, which M2 reaches in a later slice. Refusing the acknowledgement is what
	// keeps it on WhatsApp's side until there is somewhere to put it; rendering it as
	// text or as unsupported would spend the one redelivery it gets.
	acknowledged := session.receive(&waEvents.Message{
		Info: waTypes.MessageInfo{
			ID: "3EB0IMAGE",
			MessageSource: waTypes.MessageSource{
				Chat: waTypes.NewJID("5511999990001", waTypes.DefaultUserServer),
			},
		},
		Message: &waE2E.Message{ImageMessage: &waE2E.ImageMessage{Mimetype: proto.String("image/jpeg")}},
	})
	if acknowledged {
		t.Fatal("a message this build cannot publish was acknowledged to WhatsApp anyway")
	}
	select {
	case emission := <-session.Events():
		t.Fatalf("a message this build cannot publish was published as %s", emission.Type)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestAnInboundMessageIsAcknowledgedOnlyAfterItIsPublished(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")

	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.handle(textMessage("3EB0ABCDEF", "bom dia")) }()

	emission := next(t, session)
	if emission.Type != protocol.EventMessageReceived {
		t.Fatalf("the session published %s, want %s", emission.Type, protocol.EventMessageReceived)
	}
	select {
	case got := <-acknowledged:
		t.Fatalf("WhatsApp was told the account has the message (%v) before it was published", got)
	case <-time.After(50 * time.Millisecond):
	}

	emission.Settle(nil)
	if got := <-acknowledged; !got {
		t.Fatal("a message that was published was left unacknowledged")
	}
	validateAgainstContract(t, "event_message_received", emission.Payload)
}

func TestAnInboundMessageThatWasNotPublishedIsLeftUnacknowledged(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")

	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.receive(textMessage("3EB0ABCDEF", "bom dia")) }()

	emission := next(t, session)
	emission.Settle(errors.New("redis is gone"))
	if got := <-acknowledged; got {
		t.Fatal("a message the client never got was acknowledged to WhatsApp, which is how one is lost")
	}
}

func TestAnInboundMessageNobodyPublishesIsLeftUnacknowledgedRatherThanWaitedOnForever(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.deliverWait = 50 * time.Millisecond

	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.receive(textMessage("3EB0ABCDEF", "bom dia")) }()

	// Read the emission and never settle it, which is a pump wedged behind a Redis that
	// answers neither way. whatsmeow gives a handler five minutes before it starts the
	// next node alongside it, so a wait with no bound is one that costs the session its
	// ordering rather than a redelivery.
	next(t, session)
	select {
	case got := <-acknowledged:
		if got {
			t.Fatal("a message nobody published was acknowledged to WhatsApp")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the session is still waiting on a publish nobody is going to settle")
	}
}

func TestAnInboundMessageIsLeftUnacknowledgedWhenTheSessionCloses(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")

	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.receive(textMessage("3EB0ABCDEF", "bom dia")) }()

	// Closing is what a lease handover does, and it stops the forwarder, so nothing is
	// ever going to settle what is in flight. The handler has to come back anyway:
	// whatsmeow holds its handler lock while this runs and Close waits for the same
	// lock behind RemoveEventHandler.
	next(t, session)
	if err := session.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case got := <-acknowledged:
		if got {
			t.Fatal("a session that closed before publishing acknowledged the message anyway")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the handler never came back from a session that has already closed")
	}
}

func textMessage(id, body string) *waEvents.Message {
	return &waEvents.Message{
		Info: waTypes.MessageInfo{
			ID:        id,
			Timestamp: time.UnixMilli(1755000000000),
			MessageSource: waTypes.MessageSource{
				Chat:   waTypes.NewJID("5511999990001", waTypes.DefaultUserServer),
				Sender: waTypes.NewJID("5511999990001", waTypes.DefaultUserServer),
			},
			PushName: "Alice",
		},
		Message: &waE2E.Message{Conversation: proto.String(body)},
	}
}

// validateInboundAgainstContract checks the message itself, not the event that wraps
// it. The wrapper declares one property, so validating only that leaves every field of
// the message the connector actually publishes unchecked against the contract.
func validateInboundAgainstContract(t *testing.T, message *protocol.InboundMessage) {
	t.Helper()

	payload, err := json.Marshal(message)
	if err != nil {
		t.Fatalf("marshal the message: %v", err)
	}
	validateAgainstContract(t, "inbound_message", payload)
}

func TestAGroupMessageIsDroppedAndAcknowledgedWhenTheClientAskedForDirectChatsOnly(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	// `groups` defaults to false on the client, so this is what an ordinary inbox looks
	// like. Acknowledging is the whole point: refusing would leave WhatsApp redelivering
	// every group message the account gets, for as long as the session is up.
	message := textMessage("3EB0GROUP", "bom dia a todos")
	message.Info.Chat = waTypes.NewJID("120363000000000000", waTypes.GroupServer)

	// Off the test goroutine and on a short bound, so a session that publishes this
	// after all fails on the assertion below rather than sitting out the real wait.
	session.deliverWait = 50 * time.Millisecond
	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.receive(message) }()

	select {
	case emission := <-session.Events():
		t.Fatalf("a group message the client did not ask for was published as %s", emission.Type)
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case got := <-acknowledged:
		if !got {
			t.Fatal("a group message nobody subscribed to was left for WhatsApp to redeliver forever")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the handler never came back from a message it published nothing for")
	}
}

func TestAGroupMessageIsPublishedOnceTheClientSubscribesToGroups(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	// A resume during a reconnect, which is the one connect that settles without a
	// socket. What is being checked is that Connect records the subscription at all.
	session.handle(&waEvents.Connected{})
	next(t, session)
	session.handle(&waEvents.Disconnected{})
	next(t, session)
	if err := session.Connect(t.Context(), engine.ConnectRequest{Pairing: "resume", Groups: true}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	message := textMessage("3EB0GROUP", "bom dia a todos")
	message.Info.Chat = waTypes.NewJID("120363000000000000", waTypes.GroupServer)

	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.receive(message) }()

	emission := next(t, session)
	if emission.Type != protocol.EventMessageReceived {
		t.Fatalf("the session published %s for a group message, want %s", emission.Type, protocol.EventMessageReceived)
	}
	emission.Settle(nil)
	if got := <-acknowledged; !got {
		t.Fatal("a group message that was published was left unacknowledged")
	}
}

// A code request is a connect the client did not send, so it has to carry the
// subscription the client already asked for. Losing it here turns group traffic off on
// a live session at the moment somebody asks for a pairing code.
func TestACodeRequestKeepsTheGroupSubscriptionTheClientAskedFor(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "")
	session.setGroups(true)

	// The connect underneath fails: there is no socket here. What matters is what it was
	// asked for on the way, which the session records before it dials.
	_ = session.requestCode(t.Context(), &protocol.Command{
		Type:    protocol.CommandPairingRequestCode,
		Payload: json.RawMessage(`{"phone":"5511999990001"}`),
	})

	if !session.wantsGroups() {
		t.Fatal("asking for a pairing code turned the group subscription off")
	}
}

// whatsmeow unwraps an edit into the shape a new message arrives in, so an edited text
// looks exactly like a fresh one by the time anything here sees it. Publishing it as
// message.received loses the correction and spends the acknowledgement that was the
// only way to get it back.
func TestAnEditedMessageIsNotPublishedAsANewOne(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.deliverWait = 50 * time.Millisecond

	edited := textMessage("3EB0ABCDEF", "bom dia, corrigido")
	edited.IsEdit = true
	edited.Info.Edit = waTypes.EditAttributeMessageEdit

	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.receive(edited) }()

	select {
	case emission := <-session.Events():
		t.Fatalf("an edit was published as %s, and the contract has message.edited for it", emission.Type)
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case got := <-acknowledged:
		if got {
			t.Fatal("an edit nothing published was acknowledged, so WhatsApp will not send it again")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the handler never came back from an edit")
	}
}

func TestAMessageFromOneOfMetasBotsIsDroppedAndAcknowledged(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		prepare func(*waEvents.Message)
	}{
		{"a chat with the assistant itself", func(m *waEvents.Message) {
			m.Info.Chat = waTypes.NewJID("13135550002", waTypes.DefaultUserServer)
			m.Info.Sender = m.Info.Chat
		}},
		{"the assistant replying inline in somebody else's chat", func(m *waEvents.Message) {
			m.Info.Sender = waTypes.NewJID("13135550002", waTypes.DefaultUserServer)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			session.deliverWait = 50 * time.Millisecond
			message := textMessage("3EB0BOT", "eu sou a Meta AI")
			tc.prepare(message)

			acknowledged := make(chan bool, 1)
			go func() { acknowledged <- session.receive(message) }()

			select {
			case emission := <-session.Events():
				t.Fatalf("a bot's message was published as %s", emission.Type)
			case <-time.After(100 * time.Millisecond):
			}
			select {
			case got := <-acknowledged:
				if !got {
					t.Fatal("a chat with an assistant was left for WhatsApp to redeliver for good")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("the handler never came back from a bot's message")
			}
		})
	}
}
