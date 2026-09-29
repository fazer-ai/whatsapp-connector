package whatsmeow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	wm "go.mau.fi/whatsmeow"
	waCommon "go.mau.fi/whatsmeow/proto/waCommon"
	waCompanionReg "go.mau.fi/whatsmeow/proto/waCompanionReg"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waHistorySync "go.mau.fi/whatsmeow/proto/waHistorySync"
	waWeb "go.mau.fi/whatsmeow/proto/waWeb"
	waStore "go.mau.fi/whatsmeow/store"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/media"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// historyNotification is the protocol message the phone sends this device when a dump is
// ready: it names the blob, and the dump itself is downloaded from it.
func historyNotification(id string, kind waE2E.HistorySyncType) *waEvents.Message {
	own := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)
	return &waEvents.Message{
		Info: waTypes.MessageInfo{
			ID:            id,
			Timestamp:     time.UnixMilli(1755000000000),
			MessageSource: waTypes.MessageSource{Chat: own, Sender: own, IsFromMe: true},
		},
		Message: &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{
			Type:                    waE2E.ProtocolMessage_HISTORY_SYNC_NOTIFICATION.Enum(),
			HistorySyncNotification: &waE2E.HistorySyncNotification{SyncType: kind.Enum()},
		}},
	}
}

// pastMessage is one message as a history dump carries it.
func pastMessage(chat, id string, at int64, fromMe bool, message *waE2E.Message) *waHistorySync.HistorySyncMsg {
	return &waHistorySync.HistorySyncMsg{Message: &waWeb.WebMessageInfo{
		Key:              &waCommon.MessageKey{RemoteJID: proto.String(chat), FromMe: proto.Bool(fromMe), ID: proto.String(id)},
		MessageTimestamp: proto.Uint64(uint64(at)),
		Message:          message,
	}}
}

func pastText(chat, id string, at int64, body string) *waHistorySync.HistorySyncMsg {
	return pastMessage(chat, id, at, false, &waE2E.Message{Conversation: proto.String(body)})
}

// dumpOf is a dump of the given kind holding the given chats.
func dumpOf(kind waHistorySync.HistorySync_HistorySyncType, chats ...*waHistorySync.Conversation) *waHistorySync.HistorySync {
	return &waHistorySync.HistorySync{SyncType: kind.Enum(), Conversations: chats}
}

// historyBench stands in for the socket: the dump the download returns, and the receipts
// and peer messages the session sends.
type historyBench struct {
	mu        sync.Mutex
	dump      *waHistorySync.HistorySync
	downloads int
	receipts  []string
	peers     []*waE2E.Message
}

func (b *historyBench) install(session *Session) {
	session.downloadHistory = func(context.Context, *wm.Client, *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.downloads++
		return b.dump, nil
	}
	session.receiptHistory = func(_ context.Context, _ *wm.Client, id waTypes.MessageID) error {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.receipts = append(b.receipts, id)
		return nil
	}
	session.sendPeer = func(_ context.Context, _ *wm.Client, message *waE2E.Message) error {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.peers = append(b.peers, message)
		return nil
	}
}

func (b *historyBench) receipted() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.receipts...)
}

// slices reads history.sync emissions, settling each, until the handler comes back.
func slicesUntil(t *testing.T, session *Session, acknowledged <-chan bool) ([]protocol.HistorySlice, bool) {
	t.Helper()

	var slices []protocol.HistorySlice
	for {
		select {
		case got := <-acknowledged:
			return slices, got
		case emission, ok := <-session.Events():
			if !ok {
				t.Fatal("the session closed while publishing a dump")
			}
			if emission.Type != protocol.EventHistorySync {
				t.Fatalf("the session published %s while handling a dump", emission.Type)
			}
			validateAgainstContract(t, "event_history_sync", emission.Payload)
			var slice protocol.HistorySlice
			if err := json.Unmarshal(emission.Payload, &slice); err != nil {
				t.Fatalf("unmarshal a slice: %v", err)
			}
			slices = append(slices, slice)
			emission.Settle(nil)
		case <-time.After(15 * time.Second):
			t.Fatal("the session neither published nor came back")
		}
	}
}

func TestAHistoryDumpIsPublishedOneSliceAChatAndReceiptedOnlyAfterward(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_INITIAL_BOOTSTRAP,
		&waHistorySync.Conversation{ID: proto.String("5511999990002@s.whatsapp.net"), Messages: []*waHistorySync.HistorySyncMsg{
			// Newest first, the way the phone lists them.
			pastText("5511999990002@s.whatsapp.net", "3EB0A2", 1754000060, "segunda"),
			pastText("5511999990002@s.whatsapp.net", "3EB0A1", 1754000000, "primeira"),
		}},
		&waHistorySync.Conversation{ID: proto.String("5511999990003@s.whatsapp.net"), Messages: []*waHistorySync.HistorySyncMsg{
			pastText("5511999990003@s.whatsapp.net", "3EB0B1", 1754000100, "outra conversa"),
		}},
	)}
	bench.install(session)

	acknowledged := make(chan bool, 1)
	go func() {
		acknowledged <- session.receive(historyNotification("NOTIF1", waE2E.HistorySyncType_INITIAL_BOOTSTRAP))
	}()

	// Nothing receipted while the slices are still being published: a dump receipted
	// before it reached the stream is a dump a lost Redis loses.
	emission := next(t, session)
	if got := bench.receipted(); len(got) != 0 {
		t.Fatalf("the dump was receipted (%v) before its first slice was published", got)
	}
	var first protocol.HistorySlice
	if err := json.Unmarshal(emission.Payload, &first); err != nil {
		t.Fatalf("unmarshal the first slice: %v", err)
	}
	emission.Settle(nil)
	rest, got := slicesUntil(t, session, acknowledged)
	if !got {
		t.Fatal("a dump that was published was left unacknowledged")
	}
	slices := append([]protocol.HistorySlice{first}, rest...)

	if len(slices) != 2 {
		t.Fatalf("published %d slices, want one for each of the two chats", len(slices))
	}
	byChat := map[string]protocol.HistorySlice{}
	for _, slice := range slices {
		if slice.Kind != protocol.HistoryKindMessages || slice.Sync != protocol.HistoryBootstrap {
			t.Errorf("a slice went out as %q/%q, want %q/%q", slice.Kind, slice.Sync, protocol.HistoryKindMessages, protocol.HistoryBootstrap)
		}
		byChat[slice.Chat.ID] = slice
	}
	two := byChat["5511999990002"]
	if len(two.Messages) != 2 || two.Messages[0].ID != "3EB0A1" || two.Messages[1].ID != "3EB0A2" {
		t.Fatalf("the first chat's slice is %+v, want its two messages oldest first", two.Messages)
	}
	if got := bench.receipted(); len(got) != 1 || got[0] != "NOTIF1" {
		t.Fatalf("receipts sent: %v, want the dump's notification receipted once", got)
	}
}

func TestAChatLongerThanASliceArrivesInSeveralOldestFirst(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	const chat = "5511999990002@s.whatsapp.net"
	total := historySliceLimit*2 + 3
	messages := make([]*waHistorySync.HistorySyncMsg, 0, total)
	for i := total - 1; i >= 0; i-- {
		messages = append(messages, pastText(chat, fmt.Sprintf("3EB0%04d", i), 1754000000+int64(i), "oi"))
	}
	bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_FULL,
		&waHistorySync.Conversation{ID: proto.String(chat), Messages: messages})}
	bench.install(session)

	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.receive(historyNotification("NOTIF2", waE2E.HistorySyncType_FULL)) }()
	slices, got := slicesUntil(t, session, acknowledged)
	if !got {
		t.Fatal("a dump that was published was left unacknowledged")
	}

	if len(slices) != 3 {
		t.Fatalf("published %d slices for %d messages, want 3 under a limit of %d", len(slices), total, historySliceLimit)
	}
	var last int64
	seen := 0
	for _, slice := range slices {
		if slice.Sync != protocol.HistoryFull {
			t.Errorf("a slice went out as %q, want %q", slice.Sync, protocol.HistoryFull)
		}
		if len(slice.Messages) > historySliceLimit {
			t.Fatalf("a slice carries %d messages, over the limit of %d", len(slice.Messages), historySliceLimit)
		}
		for _, message := range slice.Messages {
			if message.Timestamp < last {
				t.Fatalf("%s at %d came after a message at %d: the slices are not oldest first", message.ID, message.Timestamp, last)
			}
			last = message.Timestamp
			seen++
		}
	}
	if seen != total {
		t.Fatalf("the slices carry %d messages, want all %d", seen, total)
	}
}

func TestADumpNobodyAskedForIsDownloadedAndReceiptedButNotPublished(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_RECENT,
		&waHistorySync.Conversation{ID: proto.String("5511999990002@s.whatsapp.net"), Messages: []*waHistorySync.HistorySyncMsg{
			pastText("5511999990002@s.whatsapp.net", "3EB0C1", 1754000000, "oi"),
		}})}
	bench.install(session)

	// Downloaded all the same, because the download is also where whatsmeow stores what
	// the live traffic needs from a dump: the phone-number and LID pairs and the names.
	if !session.receive(historyNotification("NOTIF3", waE2E.HistorySyncType_RECENT)) {
		t.Fatal("a dump nobody asked for was left unacknowledged, so WhatsApp would send it again")
	}
	select {
	case emission := <-session.Events():
		t.Fatalf("published %s for a session that did not ask for history", emission.Type)
	case <-time.After(100 * time.Millisecond):
	}
	if bench.downloads != 1 {
		t.Fatalf("the dump was downloaded %d times, want once so what it carries is stored", bench.downloads)
	}
	if got := bench.receipted(); len(got) != 1 {
		t.Fatalf("receipts sent: %v, want the dump receipted once it was stored", got)
	}
}

func TestADumpThatWasNotPublishedIsNeitherReceiptedNorAcknowledged(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_INITIAL_BOOTSTRAP,
		&waHistorySync.Conversation{ID: proto.String("5511999990002@s.whatsapp.net"), Messages: []*waHistorySync.HistorySyncMsg{
			pastText("5511999990002@s.whatsapp.net", "3EB0D1", 1754000000, "oi"),
		}})}
	bench.install(session)

	acknowledged := make(chan bool, 1)
	go func() {
		acknowledged <- session.receive(historyNotification("NOTIF4", waE2E.HistorySyncType_INITIAL_BOOTSTRAP))
	}()
	next(t, session).Settle(errors.New("redis is gone"))
	if got := <-acknowledged; got {
		t.Fatal("a dump the client never got was acknowledged, which is how it is lost")
	}
	if got := bench.receipted(); len(got) != 0 {
		t.Fatalf("a dump the client never got was receipted (%v), so the phone would not send it again", got)
	}
}

func TestAnOnDemandAnswerWithNothingOlderMarksTheChatExhausted(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_ON_DEMAND,
		&waHistorySync.Conversation{ID: proto.String("5511999990002@s.whatsapp.net")})}
	bench.install(session)

	acknowledged := make(chan bool, 1)
	go func() {
		acknowledged <- session.receive(historyNotification("NOTIF5", waE2E.HistorySyncType_ON_DEMAND))
	}()
	slices, got := slicesUntil(t, session, acknowledged)
	if !got {
		t.Fatal("the answer was left unacknowledged")
	}
	if len(slices) != 1 || !slices[0].Exhausted || len(slices[0].Messages) != 0 || slices[0].Sync != protocol.HistoryOnDemand {
		t.Fatalf("published %+v, want one empty on_demand slice marking the chat exhausted", slices)
	}
}

func TestHistoryMediaIsPublishedWithoutBeingDownloaded(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	var fetched int
	session.download = func(context.Context, *wm.Client, wm.DownloadableMessage, media.File) error {
		fetched++
		return nil
	}
	image := &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
		Mimetype: proto.String("image/jpeg"), URL: proto.String("https://mmg.whatsapp.net/x"),
		DirectPath: proto.String("/v/x"), MediaKey: make([]byte, 32), FileSHA256: make([]byte, 32),
		FileEncSHA256: make([]byte, 32), FileLength: proto.Uint64(1024),
	}}
	bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_ON_DEMAND,
		&waHistorySync.Conversation{ID: proto.String("5511999990002@s.whatsapp.net"), Messages: []*waHistorySync.HistorySyncMsg{
			pastMessage("5511999990002@s.whatsapp.net", "3EB0E1", 1754000000, false, image),
		}})}
	bench.install(session)

	acknowledged := make(chan bool, 1)
	go func() {
		acknowledged <- session.receive(historyNotification("NOTIF6", waE2E.HistorySyncType_ON_DEMAND))
	}()
	slices, got := slicesUntil(t, session, acknowledged)
	if !got || len(slices) != 1 || len(slices[0].Messages) != 1 {
		t.Fatalf("published %+v (acknowledged %v), want one slice with the picture", slices, got)
	}
	raw, err := json.Marshal(slices[0].Messages[0].Content)
	if err != nil {
		t.Fatalf("marshal the content: %v", err)
	}
	content := decode(t, raw)
	if content["type"] != "media" || content["ref"] != nil {
		t.Fatalf("the picture went out as %v, want media with no file fetched for it", content)
	}
	if fetched != 0 {
		t.Fatalf("the picture was downloaded %d times while the dump was published", fetched)
	}
}

func historyRequest(t *testing.T, payload string) *protocol.Command {
	t.Helper()

	return &protocol.Command{
		V: protocol.Version, ID: "cmd-history", Type: protocol.CommandHistoryRequest, SID: "sid-1",
		Payload: json.RawMessage(payload),
	}
}

func TestAHistoryRequestAsksThePhoneForWhatCameBeforeTheAnchor(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	bench := &historyBench{}
	bench.install(session)

	result, err := session.Execute(t.Context(), historyRequest(t,
		`{"chat":{"kind":"phone","id":"5511999990002"},"count":30,"before":{"id":"3EB0F1","timestamp":1754000000123,"from_me":true}}`))
	if err != nil {
		t.Fatalf("history.request: %v", err)
	}
	if result != nil {
		t.Fatalf("history.request answered %s, want null: what the phone sends arrives later", result)
	}
	if len(bench.peers) != 1 {
		t.Fatalf("%d requests went to the phone, want one", len(bench.peers))
	}
	request := bench.peers[0].GetProtocolMessage().GetPeerDataOperationRequestMessage().GetHistorySyncOnDemandRequest()
	if request.GetChatJID() != "5511999990002@s.whatsapp.net" || request.GetOldestMsgID() != "3EB0F1" ||
		!request.GetOldestMsgFromMe() || request.GetOnDemandMsgCount() != 30 {
		t.Fatalf("the phone was asked %+v, want the chat, the anchor and the count the caller named", request)
	}
}

func TestAHistoryRequestWithNoAnchorIsUnsupported(t *testing.T) {
	t.Parallel()

	for _, payload := range []string{
		`{"chat":{"kind":"phone","id":"5511999990002"}}`,
		`{"chat":{"kind":"phone","id":"5511999990002"},"before":null}`,
	} {
		session, _ := newTestSession(t, "5511999990001")
		session.setConnected(true)
		bench := &historyBench{}
		bench.install(session)

		_, err := session.Execute(t.Context(), historyRequest(t, payload))
		var coded *protocol.Error
		if !errors.As(err, &coded) || coded.Code != protocol.ErrorUnsupported {
			t.Fatalf("%s: answered %v, want unsupported: the phone can only walk back from a message it is shown", payload, err)
		}
		if len(bench.peers) != 0 {
			t.Fatalf("%s: a request went to the phone anyway", payload)
		}
	}
}

// The connect's `history_sync` is what the session reads for every dump, and a connect
// that stops asking stops the publishing.
func TestTheConnectDecidesWhetherHistoryIsPublished(t *testing.T) {
	t.Parallel()

	request := engine.ConnectRequest{Pairing: "resume", HistorySync: true}
	if err := request.Validate(); err != nil {
		t.Fatalf("a connect asking for history was refused: %v", err)
	}
}

// Whether the phone sends everything it has is decided when the device pairs, by what the
// device says it wants, and that is each session's choice rather than the process's.
func TestAPairingAsksForTheFullHistoryOnlyWhenTheClientDid(t *testing.T) {
	t.Parallel()

	for _, wants := range []bool{true, false} {
		session, _ := newTestSession(t, "5511999990001")
		client := session.current()
		if !session.adopt(t.Context(), client) {
			t.Fatal("the session would not take its own client back")
		}
		session.setHistory(wants)
		// A device with no account yet, which is the one whose payload carries the
		// properties: pairing is where they are read.
		client.Store.ID = nil

		payload := client.GetClientPayload()
		var props waCompanionReg.DeviceProps
		if err := proto.Unmarshal(payload.GetDevicePairingData().GetDeviceProps(), &props); err != nil {
			t.Fatalf("unmarshal the device properties: %v", err)
		}
		if props.GetRequireFullSync() != wants {
			t.Errorf("a session whose client asked for history=%v paired asking for the full sync=%v", wants, props.GetRequireFullSync())
		}
		if props.GetPlatformType() != waStore.DeviceProps.GetPlatformType() || props.GetOs() != waStore.DeviceProps.GetOs() {
			t.Errorf("the rest of the properties changed on the way: %v", &props)
		}
	}
}

// A dump whose blob WhatsApp no longer has is one a redelivery names again, so withholding
// it would have the phone send it for good; a download that may work next time is withheld.
func TestADumpThatCannotBeDownloadedIsReceiptedOnlyWhenItNeverWill(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		err   error
		acked bool
	}{
		{"gone from the CDN", wm.ErrMediaDownloadFailedWith404, true},
		{"expired", wm.ErrMediaDownloadFailedWith403, true},
		{"the network", errors.New("dial tcp: i/o timeout"), false},
	} {
		session, _ := newTestSession(t, "5511999990001")
		session.setHistory(true)
		bench := &historyBench{}
		bench.install(session)
		session.downloadHistory = func(context.Context, *wm.Client, *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
			return nil, tc.err
		}

		if got := session.receive(historyNotification("NOTIF7", waE2E.HistorySyncType_RECENT)); got != tc.acked {
			t.Errorf("%s: acknowledged %v, want %v", tc.name, got, tc.acked)
		}
		if receipted := len(bench.receipted()) == 1; receipted != tc.acked {
			t.Errorf("%s: receipted %v, want %v", tc.name, receipted, tc.acked)
		}
	}
}

// The status feed, a broadcast list and a channel ride in a dump beside the conversations,
// and none of them is one.
func TestAFeedInADumpIsNotPublishedAsAConversation(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	status := pastText("status@broadcast", "3EB0S1", 1754000000, "meu status")
	status.Message.Key.Participant = proto.String("5511999990002@s.whatsapp.net")
	bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_RECENT,
		&waHistorySync.Conversation{ID: proto.String("status@broadcast"), Messages: []*waHistorySync.HistorySyncMsg{status}},
		&waHistorySync.Conversation{ID: proto.String("120363000000000001@newsletter"), Messages: []*waHistorySync.HistorySyncMsg{
			pastText("120363000000000001@newsletter", "3EB0N1", 1754000000, "post"),
		}},
		&waHistorySync.Conversation{ID: proto.String("5511999990002@s.whatsapp.net"), Messages: []*waHistorySync.HistorySyncMsg{
			pastText("5511999990002@s.whatsapp.net", "3EB0P1", 1754000000, "oi"),
		}},
	)}
	bench.install(session)

	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.receive(historyNotification("NOTIF8", waE2E.HistorySyncType_RECENT)) }()
	slices, _ := slicesUntil(t, session, acknowledged)
	if len(slices) != 1 || slices[0].Chat.Kind != protocol.AddressPhone {
		t.Fatalf("published %+v, want only the direct chat", slices)
	}
}

// Where the phone said what is left, and what the slices say about it. `exhausted` stops a
// client asking for more, so it is only on the last slice of a chat the phone has nothing
// older for, and never on a chat the phone kept some of.
func TestExhaustedIsWhatThePhoneSaidIsLeftOnTheLastSliceOnly(t *testing.T) {
	t.Parallel()

	const chat = "5511999990002@s.whatsapp.net"
	long := func() []*waHistorySync.HistorySyncMsg {
		messages := make([]*waHistorySync.HistorySyncMsg, 0, historySliceLimit+1)
		for i := historySliceLimit; i >= 0; i-- {
			messages = append(messages, pastText(chat, fmt.Sprintf("3EB1%04d", i), 1754000000+int64(i), "oi"))
		}
		return messages
	}
	for _, tc := range []struct {
		name  string
		ended *waHistorySync.Conversation_EndOfHistoryTransferType
		want  []bool
	}{
		{"nothing more on the phone", waHistorySync.Conversation_COMPLETE_AND_NO_MORE_MESSAGE_REMAIN_ON_PRIMARY.Enum(), []bool{false, true}},
		{"more the phone will not share", waHistorySync.Conversation_COMPLETE_ON_DEMAND_SYNC_WITH_MORE_MSG_ON_PRIMARY_BUT_NO_ACCESS.Enum(), []bool{false, true}},
		{"more on the phone", waHistorySync.Conversation_COMPLETE_BUT_MORE_MESSAGES_REMAIN_ON_PRIMARY.Enum(), []bool{false, false}},
		{"the phone did not say", nil, []bool{false, false}},
	} {
		session, _ := newTestSession(t, "5511999990001")
		session.setHistory(true)
		bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_FULL, &waHistorySync.Conversation{
			ID: proto.String(chat), Messages: long(),
			EndOfHistoryTransfer: proto.Bool(true), EndOfHistoryTransferType: tc.ended,
		})}
		bench.install(session)

		acknowledged := make(chan bool, 1)
		go func() { acknowledged <- session.receive(historyNotification("NOTIF9", waE2E.HistorySyncType_FULL)) }()
		slices, _ := slicesUntil(t, session, acknowledged)
		got := make([]bool, 0, len(slices))
		for _, slice := range slices {
			got = append(got, slice.Exhausted)
		}
		if fmt.Sprint(got) != fmt.Sprint(tc.want) {
			t.Errorf("%s: slices marked exhausted %v, want %v", tc.name, got, tc.want)
		}
	}
}

// An on-demand answer whose messages are all things that change another message publishes
// nothing, and is not the phone running out: it sent something, just nothing to show.
func TestAnOnDemandAnswerOfOnlyReactionsIsNotTheEnd(t *testing.T) {
	t.Parallel()

	const chat = "5511999990002@s.whatsapp.net"
	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	reaction := pastMessage(chat, "3EB0R1", 1754000000, false, &waE2E.Message{ReactionMessage: &waE2E.ReactionMessage{
		Key:  &waCommon.MessageKey{RemoteJID: proto.String(chat), ID: proto.String("3EB0R0")},
		Text: proto.String("👍"),
	}})
	bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_ON_DEMAND, &waHistorySync.Conversation{
		ID: proto.String(chat), Messages: []*waHistorySync.HistorySyncMsg{reaction},
	})}
	bench.install(session)

	acknowledged := make(chan bool, 1)
	go func() {
		acknowledged <- session.receive(historyNotification("NOTIF10", waE2E.HistorySyncType_ON_DEMAND))
	}()
	if slices, _ := slicesUntil(t, session, acknowledged); len(slices) != 0 {
		t.Fatalf("published %+v for an answer holding only a reaction", slices)
	}
}

// A chat the dump names with nothing in it is not published outside an on-demand answer:
// it says nothing, and an empty slice is a row a client imports for no reason.
func TestAnEmptyChatInADumpIsNotPublished(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_RECENT,
		&waHistorySync.Conversation{ID: proto.String("5511999990002@s.whatsapp.net")})}
	bench.install(session)

	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.receive(historyNotification("NOTIF11", waE2E.HistorySyncType_RECENT)) }()
	if slices, _ := slicesUntil(t, session, acknowledged); len(slices) != 0 {
		t.Fatalf("published %+v for a chat with nothing in it", slices)
	}
}

// What a slice says about where it sits: a group's subject, which a client names the
// conversation with, and how far the dump has got. A direct chat's name is the phone's
// label for a contact and is not sent.
func TestASliceCarriesTheGroupsSubjectAndTheDumpsProgress(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	session.setGroups(true)
	group := groupJID().String()
	past := pastText(group, "3EB0G1", 1754000000, "bom dia")
	past.Message.Key.Participant = proto.String("5511999990002@s.whatsapp.net")
	dump := dumpOf(waHistorySync.HistorySync_INITIAL_BOOTSTRAP,
		&waHistorySync.Conversation{ID: proto.String(group), Name: proto.String("Equipe fazer.ai"),
			Messages: []*waHistorySync.HistorySyncMsg{past}},
		&waHistorySync.Conversation{ID: proto.String("5511999990002@s.whatsapp.net"), Name: proto.String("Ana do trabalho"),
			Messages: []*waHistorySync.HistorySyncMsg{pastText("5511999990002@s.whatsapp.net", "3EB0D9", 1754000000, "oi")}},
	)
	dump.Progress = proto.Uint32(40)
	(&historyBench{dump: dump}).install(session)

	acknowledged := make(chan bool, 1)
	go func() {
		acknowledged <- session.receive(historyNotification("NOTIF12", waE2E.HistorySyncType_INITIAL_BOOTSTRAP))
	}()
	slices, _ := slicesUntil(t, session, acknowledged)
	if len(slices) != 2 {
		t.Fatalf("published %d slices, want one per chat", len(slices))
	}
	for _, slice := range slices {
		want := ""
		if slice.Chat.Kind == protocol.AddressGroup {
			want = "Equipe fazer.ai"
		}
		if slice.Name != want {
			t.Errorf("the %s slice is named %q, want %q", slice.Chat.Kind, slice.Name, want)
		}
		if slice.Progress == nil || *slice.Progress != 40 {
			t.Errorf("the %s slice carries progress %v, want 40", slice.Chat.Kind, slice.Progress)
		}
	}
}

// A slice names its chat the way its messages and the live events do. With the pairing
// known to this account, that is the LID, even for a chat the dump names by number.
func TestASliceNamesItsChatTheWayItsMessagesDo(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	lid := waTypes.NewJID("167392323834099", waTypes.HiddenUserServer)
	phone := waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)
	if err := session.current().Store.LIDs.PutLIDMapping(t.Context(), lid, phone); err != nil {
		t.Fatalf("PutLIDMapping: %v", err)
	}
	session.aliases.observe(session.aliases.stamp(t.Context()), phone, lid)
	(&historyBench{dump: dumpOf(waHistorySync.HistorySync_RECENT,
		&waHistorySync.Conversation{ID: proto.String(phone.String()), Messages: []*waHistorySync.HistorySyncMsg{
			pastText(phone.String(), "3EB0L1", 1754000000, "oi"),
		}})}).install(session)

	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.receive(historyNotification("NOTIF13", waE2E.HistorySyncType_RECENT)) }()
	slices, _ := slicesUntil(t, session, acknowledged)
	if len(slices) != 1 || len(slices[0].Messages) != 1 {
		t.Fatalf("published %+v, want one slice with the message", slices)
	}
	if slices[0].Chat != slices[0].Messages[0].Chat {
		t.Fatalf("the slice names its chat %+v and its message names it %+v", slices[0].Chat, slices[0].Messages[0].Chat)
	}
	if slices[0].Chat.Kind != protocol.AddressLID {
		t.Fatalf("the slice names its chat %+v, want the LID this account was shown", slices[0].Chat)
	}
}
