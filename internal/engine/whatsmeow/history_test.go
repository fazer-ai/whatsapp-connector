package whatsmeow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
	"github.com/fazer-ai/whatsapp-connector/internal/store"
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
	reuploads []string
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
	session.reuploadHistory = func(_ context.Context, _ *wm.Client, id waTypes.MessageID, _ []byte) error {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.reuploads = append(b.reuploads, id)
		return nil
	}
	session.sendPeer = func(_ context.Context, _ *wm.Client, message *waE2E.Message) error {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.peers = append(b.peers, message)
		return nil
	}
}

func (b *historyBench) reuploaded() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.reuploads...)
}

func (b *historyBench) receipted() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.receipts...)
}

// heldDumps is what the session has written down and not finished with.
func heldDumps(t *testing.T, session *Session) []string {
	t.Helper()
	held, err := session.store.PendingHistory(t.Context())
	if err != nil {
		t.Fatalf("PendingHistory: %v", err)
	}
	ids := make([]string, 0, len(held))
	for _, pending := range held {
		ids = append(ids, pending.MessageID)
	}
	return ids
}

// seenSlice is a published slice as the tests read it: the batch, with the envelope's
// kind and progress beside it.
type seenSlice struct {
	protocol.HistoryMessages
	Kind     string
	Progress *int
}

func seen(slice *protocol.HistorySlice) seenSlice {
	return seenSlice{HistoryMessages: slice.Data, Kind: slice.Kind, Progress: slice.Progress}
}

// slices reads history.sync emissions, settling each, until the handler comes back.
func slicesUntil(t *testing.T, session *Session, acknowledged <-chan bool) ([]seenSlice, bool) {
	t.Helper()

	var slices []seenSlice
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
			slices = append(slices, seen(&slice))
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
	slices := append([]seenSlice{seen(&first)}, rest...)

	if len(slices) != 2 {
		t.Fatalf("published %d slices, want one for each of the two chats", len(slices))
	}
	byChat := map[string]seenSlice{}
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

// The phone announces a dump once: a notification left unacknowledged was measured not to
// come again, after the outage or after a restart. So a dump the client never got is kept
// here and tried again, and it is receipted only once a later attempt published it.
func TestADumpThatWasNotPublishedIsKeptAndPublishedByTheNextAttempt(t *testing.T) {
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
	if got := <-acknowledged; !got {
		t.Fatal("a dump that is written down was withheld, and nothing ever sends it again")
	}
	if got := bench.receipted(); len(got) != 0 {
		t.Fatalf("a dump the client never got was receipted (%v)", got)
	}
	if got := heldDumps(t, session); len(got) != 1 || got[0] != "NOTIF4" {
		t.Fatalf("what is left for the next attempt is %v, want the dump", got)
	}

	replayed := make(chan bool, 1)
	go func() {
		session.replayHistory()
		replayed <- true
	}()
	slices, _ := slicesUntil(t, session, replayed)
	if len(slices) != 1 || len(slices[0].Messages) != 1 || slices[0].Messages[0].ID != "3EB0D1" {
		t.Fatalf("the next attempt published %+v", slices)
	}
	if got := bench.receipted(); len(got) != 1 || got[0] != "NOTIF4" {
		t.Fatalf("receipts after the next attempt: %v, want the dump's", got)
	}
	if got := heldDumps(t, session); len(got) != 0 {
		t.Fatalf("a dump that is finished with is still pending: %v", got)
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

	// With somewhere to put the file, so a download would have happened had it been asked.
	session, _ := mediaSession(t, media.Options{})
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

// A dump whose blob WhatsApp no longer has is one another attempt at the same notification
// names again, so keeping it would fail for good. The phone is asked to upload it again
// instead, and it is not receipted as done; a download that may work next time is kept.
func TestADumpThatCannotBeDownloadedIsAskedForAgainOrKept(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		err       error
		kept      bool
		reuploads int
	}{
		{"gone from the CDN", wm.ErrMediaDownloadFailedWith404, false, 1},
		{"expired", wm.ErrMediaDownloadFailedWith403, false, 1},
		{"the network", errors.New("dial tcp: i/o timeout"), true, 0},
	} {
		session, _ := newTestSession(t, "5511999990001")
		session.setHistory(true)
		bench := &historyBench{}
		bench.install(session)
		session.downloadHistory = func(context.Context, *wm.Client, *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
			return nil, tc.err
		}

		if !session.receive(historyNotification("NOTIF7", waE2E.HistorySyncType_RECENT)) {
			t.Errorf("%s: a dump that is written down was withheld", tc.name)
		}
		if got := len(heldDumps(t, session)) == 1; got != tc.kept {
			t.Errorf("%s: kept for another attempt %v, want %v", tc.name, got, tc.kept)
		}
		if got := len(bench.reuploaded()); got != tc.reuploads {
			t.Errorf("%s: asked the phone to upload again %d times, want %d", tc.name, got, tc.reuploads)
		}
		if got := bench.receipted(); len(got) != 0 {
			t.Errorf("%s: receipted %v a dump that was never published", tc.name, got)
		}
	}
}

// Asking the phone to upload again is a write, and one that did not go out leaves the
// dump with nothing coming: kept, the next attempt asks again.
func TestADumpThePhoneCouldNotBeAskedToUploadAgainIsKept(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	(&historyBench{}).install(session)
	session.downloadHistory = func(context.Context, *wm.Client, *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
		return nil, wm.ErrMediaDownloadFailedWith410
	}
	session.reuploadHistory = func(context.Context, *wm.Client, waTypes.MessageID, []byte) error {
		return wm.ErrNotConnected
	}
	session.receive(historyNotification("NOTIF25", waE2E.HistorySyncType_RECENT))
	if got := heldDumps(t, session); len(got) != 1 {
		t.Fatalf("a dump nobody could ask for again is not kept (%v), so nothing would bring it back", got)
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
	// Held until the handler has come back: the request goes out off the node handler, and
	// a send stuck on whatsmeow's lock must not hold the handler with it.
	release := make(chan struct{})
	asked := make(chan *waE2E.Message, 1)
	session.sendPeer = func(_ context.Context, _ *wm.Client, message *waE2E.Message) error {
		<-release
		asked <- message
		return nil
	}

	acknowledged := make(chan bool, 1)
	go func() {
		acknowledged <- session.receive(historyNotification("NOTIF10", waE2E.HistorySyncType_ON_DEMAND))
	}()
	if slices, _ := slicesUntil(t, session, acknowledged); len(slices) != 0 {
		t.Fatalf("published %+v for an answer holding only a reaction", slices)
	}
	close(release)
	// And the next page is asked for from the reaction, the oldest thing the phone sent:
	// the client has nothing new to anchor on, and asking from its old anchor gets this
	// same page again.
	var sent *waE2E.Message
	select {
	case sent = <-asked:
	case <-time.After(10 * time.Second):
		t.Fatal("the next page was never asked for")
	}
	request := sent.GetProtocolMessage().GetPeerDataOperationRequestMessage().GetHistorySyncOnDemandRequest()
	if request.GetChatJID() != chat || request.GetOldestMsgID() != "3EB0R1" {
		t.Fatalf("the phone was asked %+v, want the page before the reaction", request)
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

// whatsmeow puts the blob's URL in a failed download, built from the direct path and the
// hash, and neither belongs in a log. Both ways out of a failed download are asked.
func TestAFailedHistoryDownloadIsLoggedWithoutTheBlobsAddress(t *testing.T) {
	t.Parallel()

	const where = "https://mmg.whatsapp.net/v/t62.7117-24/AbCdEfGhIjKlMnOpQrStUvWxYz0123456789?ccb=11-4&oh=SECRETHASHSECRETHASHSECRET"
	for _, err := range []error{
		fmt.Errorf("failed to download: Get %q: dial tcp: i/o timeout", where),
		fmt.Errorf("failed to download %s: %w", where, wm.ErrMediaDownloadFailedWith404),
	} {
		session, written := newLoggedTestSession(t, "5511999990001")
		session.setHistory(true)
		(&historyBench{}).install(session)
		session.downloadHistory = func(context.Context, *wm.Client, *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
			return nil, err
		}

		session.receive(historyNotification("NOTIF14", waE2E.HistorySyncType_RECENT))
		if logged := written.String(); strings.Contains(logged, "AbCdEfGhIjKlMnOpQrStUvWxYz0123456789") ||
			strings.Contains(logged, "SECRETHASHSECRETHASHSECRET") {
			t.Errorf("the blob's address reached the log: %s", logged)
		}
	}
}

// A dump carries the account's own pairs of number and LID, and a new pairing has nothing
// else to go on: named by number in the dump, the chat still goes out under the LID the
// live traffic will use, so a client does not open two conversations for one person.
func TestTheDumpsOwnPairsNameItsChats(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	phone := "5511999990002@s.whatsapp.net"
	dump := dumpOf(waHistorySync.HistorySync_INITIAL_BOOTSTRAP,
		&waHistorySync.Conversation{ID: proto.String(phone), Messages: []*waHistorySync.HistorySyncMsg{
			pastText(phone, "3EB0M1", 1754000000, "oi"),
		}})
	dump.PhoneNumberToLidMappings = []*waHistorySync.PhoneNumberToLIDMapping{
		{PnJID: proto.String(phone), LidJID: proto.String("167392323834077@lid")},
	}
	(&historyBench{dump: dump}).install(session)

	acknowledged := make(chan bool, 1)
	go func() {
		acknowledged <- session.receive(historyNotification("NOTIF15", waE2E.HistorySyncType_INITIAL_BOOTSTRAP))
	}()
	slices, _ := slicesUntil(t, session, acknowledged)
	if len(slices) != 1 || slices[0].Chat != (protocol.Address{Kind: protocol.AddressLID, ID: "167392323834077"}) {
		t.Fatalf("published %+v, want the chat under the LID the dump paired the number with", slices)
	}
}

// A correction in a dump is parsed into the message it corrects, same id, new body. The
// original is in the dump too, already corrected, so publishing the correction would be
// the same id twice with the correction's clock on it.
func TestACorrectionInADumpIsNotPublishedAsAMessage(t *testing.T) {
	t.Parallel()

	const chat = "5511999990002@s.whatsapp.net"
	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	edit := pastMessage(chat, "3EB0E2", 1754000100, false, &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{
		Type:          waE2E.ProtocolMessage_MESSAGE_EDIT.Enum(),
		Key:           &waCommon.MessageKey{RemoteJID: proto.String(chat), ID: proto.String("3EB0E1")},
		EditedMessage: &waE2E.Message{Conversation: proto.String("corrigido")},
	}})
	(&historyBench{dump: dumpOf(waHistorySync.HistorySync_RECENT,
		&waHistorySync.Conversation{ID: proto.String(chat), Messages: []*waHistorySync.HistorySyncMsg{
			edit, pastText(chat, "3EB0E1", 1754000000, "corrigido"),
		}})}).install(session)

	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.receive(historyNotification("NOTIF16", waE2E.HistorySyncType_RECENT)) }()
	slices, _ := slicesUntil(t, session, acknowledged)
	if len(slices) != 1 || len(slices[0].Messages) != 1 || slices[0].Messages[0].Timestamp != 1754000000000 {
		t.Fatalf("published %+v, want the corrected message once, at its own time", slices)
	}
}

// A dump's clock has seconds only, so messages sent within one second tie, and the order
// the phone listed them in is all that says which came first.
func TestMessagesThatTieOnTheSecondKeepThePhonesOrder(t *testing.T) {
	t.Parallel()

	const chat = "5511999990002@s.whatsapp.net"
	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	(&historyBench{dump: dumpOf(waHistorySync.HistorySync_RECENT,
		&waHistorySync.Conversation{ID: proto.String(chat), Messages: []*waHistorySync.HistorySyncMsg{
			// Newest first, the way the phone lists them.
			pastText(chat, "3EB0T3", 1754000000, "terceira"),
			pastText(chat, "3EB0T2", 1754000000, "segunda"),
			pastText(chat, "3EB0T1", 1754000000, "primeira"),
		}})}).install(session)

	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.receive(historyNotification("NOTIF17", waE2E.HistorySyncType_RECENT)) }()
	slices, _ := slicesUntil(t, session, acknowledged)
	if len(slices) != 1 || len(slices[0].Messages) != 3 {
		t.Fatalf("published %+v, want one slice of three", slices)
	}
	for i, want := range []string{"3EB0T1", "3EB0T2", "3EB0T3"} {
		if got := slices[0].Messages[i].ID; got != want {
			t.Fatalf("message %d is %s, want %s: the tie reversed the conversation", i, got, want)
		}
	}
}

// A history request is a send, and whatsmeow's retry after a reconnect has no deadline of
// its own: without the ceiling, a command that brought none holds the session's queue for
// as long as the socket stays down.
func TestAHistoryRequestIsHeldToTheSendCeiling(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.wireLimit = 50 * time.Millisecond
	(&historyBench{}).install(session)
	session.sendPeer = func(ctx context.Context, _ *wm.Client, _ *waE2E.Message) error {
		<-ctx.Done()
		return ctx.Err()
	}

	answered := make(chan error, 1)
	go func() {
		_, err := session.Execute(context.WithoutCancel(t.Context()), historyRequest(t,
			`{"chat":{"kind":"phone","id":"5511999990002"},"before":{"id":"3EB0F1","timestamp":1754000000123,"from_me":false}}`))
		answered <- err
	}()
	select {
	case err := <-answered:
		var coded *protocol.Error
		if !errors.As(err, &coded) || coded.Code != protocol.ErrorTimeout {
			t.Fatalf("a request that never went out answered %v, want timeout", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a request that never went out held the session past its ceiling")
	}
}

// A message can reach a dump after it arrived live, and what was kept then names the file
// already on this instance's disk. The dump's copy must not replace it; a message only the
// dump has is kept, so its file can still be fetched.
func TestHistoryMediaKeepsWhatWasAlreadyKeptAndRecordsTheRest(t *testing.T) {
	t.Parallel()

	const chat = "5511999990002@s.whatsapp.net"
	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	known := store.MediaPart{MessageID: "3EB0K1", ChatKind: "phone", ChatID: "5511999990002", Kind: "image",
		DirectPath: "/v/live", BlobID: "blob-live"}
	if err := session.store.PutMediaPart(t.Context(), &known, time.Now()); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}
	image := func() *waE2E.Message {
		return &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
			Mimetype: proto.String("image/jpeg"), DirectPath: proto.String("/v/dump"), MediaKey: make([]byte, 32),
			FileSHA256: make([]byte, 32), FileEncSHA256: make([]byte, 32), FileLength: proto.Uint64(1024),
		}}
	}
	(&historyBench{dump: dumpOf(waHistorySync.HistorySync_RECENT,
		&waHistorySync.Conversation{ID: proto.String(chat), Messages: []*waHistorySync.HistorySyncMsg{
			pastMessage(chat, "3EB0K1", 1754000000, false, image()),
			pastMessage(chat, "3EB0K2", 1754000001, false, image()),
		}})}).install(session)

	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.receive(historyNotification("NOTIF18", waE2E.HistorySyncType_RECENT)) }()
	slicesUntil(t, session, acknowledged)

	kept, found, err := session.store.MediaPart(t.Context(), "3EB0K1")
	if err != nil || !found || kept.BlobID != "blob-live" || kept.DirectPath != "/v/live" {
		t.Fatalf("the row kept live is now %+v (found %v, %v), want it untouched", kept, found, err)
	}
	fresh, found, err := session.store.MediaPart(t.Context(), "3EB0K2")
	if err != nil || !found || fresh.DirectPath != "/v/dump" {
		t.Fatalf("the row for a message only the dump has is %+v (found %v, %v), want the dump's coordinates", fresh, found, err)
	}
}

// A dump that cannot be published within its budget is left for another attempt, rather
// than holding the node handler past the watchdog that would start the next node beside it.
func TestADumpOverItsBudgetIsLeftForAnotherAttempt(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	session.historyBudget = -time.Second
	bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_RECENT,
		&waHistorySync.Conversation{ID: proto.String("5511999990002@s.whatsapp.net"), Messages: []*waHistorySync.HistorySyncMsg{
			pastText("5511999990002@s.whatsapp.net", "3EB0B9", 1754000000, "oi"),
		}})}
	bench.install(session)

	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.receive(historyNotification("NOTIF19", waE2E.HistorySyncType_RECENT)) }()
	slices, got := slicesUntil(t, session, acknowledged)
	if !got || len(slices) != 0 || len(bench.receipted()) != 0 || len(heldDumps(t, session)) != 1 {
		t.Fatalf("a dump over its budget published %d slices, acknowledged %v, receipted %v, kept %v",
			len(slices), got, bench.receipted(), heldDumps(t, session))
	}
}

// A dump carries the account's own protocol messages among a chat's messages: a
// disappearing-timer change, a deletion. None is a message somebody sent, and each would
// otherwise go out as an unreadable bubble.
func TestAProtocolMessageInADumpIsNotPublished(t *testing.T) {
	t.Parallel()

	const chat = "5511999990002@s.whatsapp.net"
	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	timer := pastMessage(chat, "3EB0PR", 1754000001, false, &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{
		Type: waE2E.ProtocolMessage_EPHEMERAL_SETTING.Enum(), EphemeralExpiration: proto.Uint32(86400),
	}})
	(&historyBench{dump: dumpOf(waHistorySync.HistorySync_RECENT,
		&waHistorySync.Conversation{ID: proto.String(chat), Messages: []*waHistorySync.HistorySyncMsg{
			timer, pastText(chat, "3EB0PT", 1754000000, "oi"),
		}})}).install(session)

	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.receive(historyNotification("NOTIF20", waE2E.HistorySyncType_RECENT)) }()
	slices, _ := slicesUntil(t, session, acknowledged)
	if len(slices) != 1 || len(slices[0].Messages) != 1 || slices[0].Messages[0].ID != "3EB0PT" {
		t.Fatalf("published %+v, want only the text", slices)
	}
}

// Newest first is how the phone has been seen to list a chat, not something its protocol
// says, so the slices are put in order by the clock rather than trusted to be.
func TestADumpListedOutOfOrderIsPublishedOldestFirst(t *testing.T) {
	t.Parallel()

	const chat = "5511999990002@s.whatsapp.net"
	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	(&historyBench{dump: dumpOf(waHistorySync.HistorySync_RECENT,
		&waHistorySync.Conversation{ID: proto.String(chat), Messages: []*waHistorySync.HistorySyncMsg{
			pastText(chat, "3EB0O2", 1754000020, "b"),
			pastText(chat, "3EB0O3", 1754000030, "c"),
			pastText(chat, "3EB0O1", 1754000010, "a"),
		}})}).install(session)

	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.receive(historyNotification("NOTIF21", waE2E.HistorySyncType_RECENT)) }()
	slices, _ := slicesUntil(t, session, acknowledged)
	if len(slices) != 1 || len(slices[0].Messages) != 3 {
		t.Fatalf("published %+v, want one slice of three", slices)
	}
	for i, want := range []string{"3EB0O1", "3EB0O2", "3EB0O3"} {
		if got := slices[0].Messages[i].ID; got != want {
			t.Fatalf("message %d is %s, want %s", i, got, want)
		}
	}
}

// A code request is a connect the client did not send, so it carries the history choice
// the client already made. Lost here, the pairing it asks for would not ask the phone for
// the full history, and every dump after it would be receipted and published nowhere.
func TestACodeRequestKeepsTheHistoryTheClientAskedFor(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "")
	session.setHistory(true)
	// The connect underneath fails: there is no socket here. What matters is what it was
	// asked for on the way, which the session records before it dials.
	_ = session.requestCode(t.Context(), &protocol.Command{
		Type:    protocol.CommandPairingRequestCode,
		Payload: json.RawMessage(`{"phone":"5511999990001"}`),
	})
	if !session.wantsHistory() {
		t.Fatal("asking for a pairing code turned history off")
	}
}

// The budget starts before the download: whatsmeow's media client has no overall timeout,
// and a download that stalls is the node handler stalling with it.
func TestTheDumpsBudgetBoundsItsDownload(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	session.historyBudget = 50 * time.Millisecond
	bench := &historyBench{}
	bench.install(session)
	session.downloadHistory = func(ctx context.Context, _ *wm.Client, _ *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.receive(historyNotification("NOTIF22", waE2E.HistorySyncType_RECENT)) }()
	select {
	case got := <-acknowledged:
		if !got || len(bench.receipted()) != 0 || len(heldDumps(t, session)) != 1 {
			t.Fatalf("a download that ran out of time was acknowledged %v, receipted %v, kept %v",
				got, bench.receipted(), heldDumps(t, session))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a download that never finished held the node handler past its budget")
	}
}

// A dump's media carries no reference, so the row that says how to fetch the file is the
// only way it is ever fetched. A row that could not be written keeps the dump for another
// attempt, which writes it; published and receipted, the file would be gone for good.
func TestADumpWhoseFileCouldNotBeKeptIsKept(t *testing.T) {
	t.Parallel()

	const chat = "5511999990002@s.whatsapp.net"
	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	image := &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
		Mimetype: proto.String("image/jpeg"), DirectPath: proto.String("/v/dump"), MediaKey: make([]byte, 32),
		FileSHA256: make([]byte, 32), FileEncSHA256: make([]byte, 32), FileLength: proto.Uint64(1024),
	}}
	bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_RECENT,
		&waHistorySync.Conversation{ID: proto.String(chat), Messages: []*waHistorySync.HistorySyncMsg{
			pastMessage(chat, "3EB0U1", 1754000000, false, image),
		}})}
	bench.install(session)
	// The store refuses every write from the download on, the way it does for a session
	// another instance took: the dump itself is written down first, and its file is not.
	session.downloadHistory = func(context.Context, *wm.Client, *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
		session.store.Drop()
		return bench.dump, nil
	}

	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.receive(historyNotification("NOTIF23", waE2E.HistorySyncType_RECENT)) }()
	slices, got := slicesUntil(t, session, acknowledged)
	if !got || len(slices) != 0 || len(bench.receipted()) != 0 || len(heldDumps(t, session)) != 1 {
		t.Fatalf("a dump whose file could not be kept published %d slices, acknowledged %v, receipted %v, kept %v",
			len(slices), got, bench.receipted(), heldDumps(t, session))
	}
}

// A receipt is a write on the socket inside the node handler, and one that stalls holds
// the handler past the watchdog after the whole dump was published in time.
func TestTheDumpsReceiptIsHeldToItsOwnWait(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.historyReceiptWait = 50 * time.Millisecond
	(&historyBench{dump: dumpOf(waHistorySync.HistorySync_PUSH_NAME)}).install(session)
	session.receiptHistory = func(ctx context.Context, _ *wm.Client, _ waTypes.MessageID) error {
		<-ctx.Done()
		return ctx.Err()
	}

	acknowledged := make(chan bool, 1)
	go func() {
		acknowledged <- session.receive(historyNotification("NOTIF24", waE2E.HistorySyncType_PUSH_NAME))
	}()
	select {
	case <-acknowledged:
	case <-time.After(10 * time.Second):
		t.Fatal("a receipt that never went out held the node handler past its wait")
	}
}

// A file kept under the number before this account knew the person's LID is still the
// same conversation's file once a dump carries the pairing and the history names the chat
// by LID. Refusing it would leave a bubble the client can never fill in.
func TestAFileKeptUnderANumberIsServedUnderItsLID(t *testing.T) {
	t.Parallel()

	session, downloads := mediaSession(t, media.Options{})
	downloads.answer([]byte("os mesmos bytes"), nil)
	connect(session)
	event := imageEvent("3EB0ALIAS")
	if _, acknowledged := deliver(t, session, event, 1); !acknowledged {
		t.Fatal("a media message with a file was left unacknowledged")
	}
	phone := event.Info.Chat
	lid := waTypes.NewJID("167392323834055", waTypes.HiddenUserServer)
	session.aliases.observe(session.aliases.stamp(t.Context()), phone, lid)

	if ref := refetch(t, session, "3EB0ALIAS", &protocol.Address{Kind: protocol.AddressLID, ID: lid.User}); ref.ID == "" {
		t.Fatal("a download naming the chat's LID was not served")
	}
}

// Everything a dump can spend in the node handler, added up, is under the five minutes
// whatsmeow gives it before starting the next node beside it: the budget, the last
// slice's wait on the publisher that the budget is checked before, and the receipt.
func TestADumpsWorstCaseFitsUnderTheNodeWatchdog(t *testing.T) {
	t.Parallel()

	const watchdog = 5 * time.Minute
	// The row written before the dump and dropped after it, each on the store's bound.
	if worst := bindTimeout + historyBudget + deliverTimeout + historyReceiptTimeout + bindTimeout; worst >= watchdog {
		t.Fatalf("a dump can hold the node handler for %s, which is not under the %s watchdog", worst, watchdog)
	}
}

// A conversation names its other party's second address itself, and not always in the
// dump's own list of pairs: the chat still goes out under the LID the live traffic uses.
func TestAConversationsOwnPairNamesItsChat(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	phone := "5511999990002@s.whatsapp.net"
	(&historyBench{dump: dumpOf(waHistorySync.HistorySync_INITIAL_BOOTSTRAP,
		&waHistorySync.Conversation{ID: proto.String(phone), LidJID: proto.String("167392323834066@lid"),
			Messages: []*waHistorySync.HistorySyncMsg{pastText(phone, "3EB0C9", 1754000000, "oi")}},
	)}).install(session)

	acknowledged := make(chan bool, 1)
	go func() {
		acknowledged <- session.receive(historyNotification("NOTIF26", waE2E.HistorySyncType_INITIAL_BOOTSTRAP))
	}()
	slices, _ := slicesUntil(t, session, acknowledged)
	if len(slices) != 1 || slices[0].Chat != (protocol.Address{Kind: protocol.AddressLID, ID: "167392323834066"}) {
		t.Fatalf("published %+v, want the chat under the LID its conversation names", slices)
	}
}

// Each connect says again whether history is wanted, and the session reads it on every
// dump: turned on by one connect, off by the next.
func TestEachConnectSaysWhetherHistoryIsPublished(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	// Connected first, so the connects below do not dial.
	session.setConnected(true)
	for _, wants := range []bool{true, false} {
		if err := session.Connect(t.Context(), engine.ConnectRequest{Pairing: "resume", HistorySync: wants}); err != nil {
			t.Fatalf("Connect: %v", err)
		}
		if session.wantsHistory() != wants {
			t.Fatalf("a connect asking for history=%v left the session at %v", wants, session.wantsHistory())
		}
	}
}

// The account's own device announces a dump. The same protocol message from anybody else
// is not one, and nothing is downloaded for it.
func TestOnlyTheAccountsOwnDeviceAnnouncesADump(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_RECENT)}
	bench.install(session)
	notice := historyNotification("NOTIF27", waE2E.HistorySyncType_RECENT)
	notice.Info.IsFromMe = false
	notice.Info.Sender = waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)
	notice.Info.Chat = notice.Info.Sender

	session.receive(notice)
	if bench.downloads != 0 || len(bench.receipted()) != 0 {
		t.Fatalf("a notification from somebody else was downloaded %d times and receipted %v", bench.downloads, bench.receipted())
	}
}

// A history request puts a message on the wire, so it is refused on a session that is not
// connected, the way a send is, rather than handed to a socket that is not there.
func TestAHistoryRequestOnASessionThatIsNotConnectedIsRefused(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	bench := &historyBench{}
	bench.install(session)

	_, err := session.Execute(t.Context(), historyRequest(t,
		`{"chat":{"kind":"phone","id":"5511999990002"},"before":{"id":"3EB0F1","timestamp":1754000000123,"from_me":false}}`))
	var coded *protocol.Error
	if !errors.As(err, &coded) || coded.Code != protocol.ErrorNotConnected {
		t.Fatalf("answered %v, want not_connected", err)
	}
	if len(bench.peers) != 0 {
		t.Fatal("a request went to the phone from a session that is not connected")
	}
}

// A dump the store would not take when it arrived is kept in memory and written by the
// next attempt: withholding the acknowledgement would bring nothing back.
func TestADumpThatCouldNotBeWrittenDownIsWrittenByTheNextAttempt(t *testing.T) {
	t.Parallel()

	// The store refuses the write, the way it does while the lease is in doubt; a handle
	// with a fence of its own below is the store answering again.
	session, container := newTestSession(t, "5511999990001")
	session.historyRetry = time.Hour
	session.store.Drop()
	bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_RECENT)}
	bench.install(session)

	if !session.receive(historyNotification("NOTIF30", waE2E.HistorySyncType_RECENT)) {
		t.Fatal("a dump kept in memory was withheld, and withholding brings nothing back")
	}
	if got := heldDumps(t, session); len(got) != 0 || bench.downloads != 0 {
		t.Fatalf("a dump the store refused is pending %v and was downloaded %d times", got, bench.downloads)
	}
	session.dumps.mu.Lock()
	armed := session.dumps.armed
	session.dumps.mu.Unlock()
	if !armed {
		t.Fatal("a dump kept in memory armed no retry, so it waits for a connection that may not come")
	}

	// A retry the store still refuses has not finished, and keeps backing off.
	session.dumps.mu.Lock()
	session.dumps.wait = 4 * time.Minute
	session.dumps.mu.Unlock()
	session.replayHistory()
	session.dumps.mu.Lock()
	wait := session.dumps.wait
	session.dumps.mu.Unlock()
	if wait == 0 {
		t.Fatal("a retry that could not write the dump down counted as finished")
	}

	session.store = container.For(session.sid)
	session.replayHistory()
	if got := bench.receipted(); len(got) != 1 || got[0] != "NOTIF30" {
		t.Fatalf("after the store came back the dump was receipted %v", got)
	}
	if got := heldDumps(t, session); len(got) != 0 {
		t.Fatalf("a dump that is finished with is still pending: %v", got)
	}
	if got := session.dumps.unwrittenDumps(); len(got) != 0 {
		t.Fatalf("a dump written down is still kept in memory: %v", got)
	}
}

// And one kept in memory for an account the session no longer holds is let go.
func TestAnUnwrittenDumpOfAReplacedAccountIsLetGo(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.dumps.keepUnwritten(unwrittenDump{
		id: "NOTIF43", device: deviceOf(session.current()), notice: recentNotice(t),
		learned: 1755000000000, generation: session.aliases.learning(),
	})
	session.aliases.forget()

	session.replayHistory()
	if got := session.dumps.unwrittenDumps(); len(got) != 0 {
		t.Fatalf("the replaced account's dump is still kept: %v", got)
	}
	if got := heldDumps(t, session); len(got) != 0 {
		t.Fatalf("the replaced account's dump was written down for the new one: %v", got)
	}
}

// A retry is not the node handler and is not held to its budget: a dump too big for the
// first attempt's has to finish somewhere.
func TestARetryHasABudgetOfItsOwn(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	session.historyBudget = -time.Second
	bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_RECENT,
		&waHistorySync.Conversation{ID: proto.String("5511999990002@s.whatsapp.net"), Messages: []*waHistorySync.HistorySyncMsg{
			pastText("5511999990002@s.whatsapp.net", "3EB0BIG", 1754000000, "oi"),
		}})}
	bench.install(session)
	holdDump(t, session, "NOTIF44", recentNotice(t))

	replayed := make(chan bool, 1)
	go func() {
		session.replayHistory()
		replayed <- true
	}()
	if slices, _ := slicesUntil(t, session, replayed); len(slices) != 1 {
		t.Fatalf("a retry held to the first attempt's budget published %d slices", len(slices))
	}
	if got := bench.receipted(); len(got) != 1 {
		t.Fatalf("a retry held to the first attempt's budget receipted %v", got)
	}
	if historyReplayBudget <= historyBudget {
		t.Fatalf("a retry's budget %s is no longer than the first attempt's %s", historyReplayBudget, historyBudget)
	}
}

// holdDump writes a dump down the way a previous owner of the session would have left it.
func holdDump(t *testing.T, session *Session, id string, notice []byte) {
	t.Helper()
	if err := session.store.PutPendingHistory(t.Context(), &store.PendingHistory{
		MessageID: id, Device: deviceOf(session.current()), Notice: notice, LearnedAt: 1755000000000,
	}); err != nil {
		t.Fatalf("PutPendingHistory: %v", err)
	}
}

func recentNotice(t *testing.T) []byte {
	t.Helper()
	raw, err := proto.Marshal(&waE2E.HistorySyncNotification{SyncType: waE2E.HistorySyncType_RECENT.Enum()})
	if err != nil {
		t.Fatalf("marshal a notice: %v", err)
	}
	return raw
}

// A dump a previous owner left is finished by the next socket this session opens: that is
// where a lost lease, a restart and a Redis outage long enough to lose the lease all end.
func TestAPendingDumpIsFinishedWhenTheSessionConnects(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_RECENT,
		&waHistorySync.Conversation{ID: proto.String("5511999990002@s.whatsapp.net"), Messages: []*waHistorySync.HistorySyncMsg{
			pastText("5511999990002@s.whatsapp.net", "3EB0R1", 1754000000, "oi"),
		}})}
	bench.install(session)
	holdDump(t, session, "NOTIF31", recentNotice(t))

	session.handle(&waEvents.Connected{})
	for {
		emission := next(t, session)
		if emission.Settle != nil {
			emission.Settle(nil)
		}
		if emission.Type == protocol.EventHistorySync {
			break
		}
	}
	deadline := time.After(10 * time.Second)
	for len(bench.receipted()) == 0 || len(heldDumps(t, session)) != 0 {
		select {
		case <-deadline:
			t.Fatalf("after the connect the dump was receipted %v and is pending %v", bench.receipted(), heldDumps(t, session))
		case <-time.After(5 * time.Millisecond):
		}
	}
	if got := bench.receipted(); got[0] != "NOTIF31" {
		t.Fatalf("receipted %v, want the pending dump", got)
	}
}

// A dump that did not finish is tried again by this session after a wait, without anybody
// reconnecting: an outage shorter than the lease ends with the same socket up.
func TestADumpThatDidNotFinishIsTriedAgainAfterAWait(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	session.historyRetry = 10 * time.Millisecond
	bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_RECENT,
		&waHistorySync.Conversation{ID: proto.String("5511999990002@s.whatsapp.net"), Messages: []*waHistorySync.HistorySyncMsg{
			pastText("5511999990002@s.whatsapp.net", "3EB0R2", 1754000000, "oi"),
		}})}
	bench.install(session)

	go session.receive(historyNotification("NOTIF32", waE2E.HistorySyncType_RECENT))
	next(t, session).Settle(errors.New("redis is gone"))
	// A retry that fails is followed by another: the first one firing is what lets the
	// next be armed.
	next(t, session).Settle(errors.New("redis is still gone"))
	retried := next(t, session)
	if retried.Type != protocol.EventHistorySync {
		t.Fatalf("the retry published %s", retried.Type)
	}
	retried.Settle(nil)
	deadline := time.After(10 * time.Second)
	for len(bench.receipted()) == 0 {
		select {
		case <-deadline:
			t.Fatal("the retry published the dump and never receipted it")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// Each attempt downloads the dump again, so the wait doubles, up to a ceiling, and starts
// over once everything finished.
func TestTheWaitBetweenAttemptsDoublesUpToTheCeiling(t *testing.T) {
	t.Parallel()

	var dumps pendingDumps
	var waits []time.Duration
	for range 5 {
		wait, armed := dumps.arm(time.Second, 5*time.Second)
		if !armed {
			t.Fatal("an attempt was not armed with none armed")
		}
		if _, again := dumps.arm(time.Second, 5*time.Second); again {
			t.Fatal("a second attempt was armed beside the first")
		}
		dumps.disarm()
		waits = append(waits, wait)
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second, 5 * time.Second}
	if fmt.Sprint(waits) != fmt.Sprint(want) {
		t.Fatalf("waits %v, want %v", waits, want)
	}

	dumps.settle()
	if wait, _ := dumps.arm(time.Second, 5*time.Second); wait != time.Second {
		t.Fatalf("after a replay that finished everything the wait is %v, want the first", wait)
	}
}

// A row nothing can read is one no attempt will ever finish, and every later owner would
// fail on it the same way.
func TestAPendingDumpNothingCanReadIsDropped(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	bench := &historyBench{}
	bench.install(session)
	holdDump(t, session, "NOTIF33", []byte{0xff, 0xff, 0xff})

	session.replayHistory()
	if got := heldDumps(t, session); len(got) != 0 {
		t.Fatalf("an unreadable dump is still pending: %v", got)
	}
	if bench.downloads != 0 {
		t.Fatalf("an unreadable dump was downloaded %d times", bench.downloads)
	}
}

// A notification that arrives again while a retry of it runs is the same dump, and one
// attempt at it is all there is: two would publish it twice.
func TestADumpBeingRetriedIsNotStartedASecondTime(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_RECENT)}
	bench.install(session)
	started, carryOn := make(chan struct{}), make(chan struct{})
	var downloads sync.Mutex
	count := 0
	session.downloadHistory = func(context.Context, *wm.Client, *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
		downloads.Lock()
		count++
		downloads.Unlock()
		close(started)
		<-carryOn
		return bench.dump, nil
	}
	holdDump(t, session, "NOTIF34", recentNotice(t))

	replayed := make(chan struct{})
	go func() {
		session.replayHistory()
		close(replayed)
	}()
	<-started
	if !session.receive(historyNotification("NOTIF34", waE2E.HistorySyncType_RECENT)) {
		t.Error("the notification of a dump being retried was withheld")
	}
	close(carryOn)
	<-replayed
	downloads.Lock()
	defer downloads.Unlock()
	if count != 1 {
		t.Fatalf("one dump was downloaded %d times", count)
	}
}

// A replay whose attempt did not finish arms another, the way a live dump does.
func TestAReplayThatDidNotFinishIsTriedAgain(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	session.historyRetry = 10 * time.Millisecond
	bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_RECENT)}
	bench.install(session)
	var mu sync.Mutex
	failures := 1
	session.downloadHistory = func(context.Context, *wm.Client, *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
		mu.Lock()
		defer mu.Unlock()
		if failures > 0 {
			failures--
			return nil, errors.New("dial tcp: i/o timeout")
		}
		return bench.dump, nil
	}
	holdDump(t, session, "NOTIF35", recentNotice(t))

	session.replayHistory()
	deadline := time.After(10 * time.Second)
	for len(bench.receipted()) == 0 {
		select {
		case <-deadline:
			t.Fatal("a replay that did not finish was never tried again")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// A replay that could not even read what is pending is tried again as well.
func TestAReplayThatCouldNotReadWhatIsPendingIsTriedAgain(t *testing.T) {
	t.Parallel()

	session, container := newTestSession(t, "5511999990001")
	session.historyRetry = time.Hour
	if err := container.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	session.replayHistory()
	session.dumps.mu.Lock()
	defer session.dumps.mu.Unlock()
	if !session.dumps.armed {
		t.Fatal("a replay that read nothing armed no retry, so the dumps wait for the next connection")
	}
}

// A logout during an attempt rebuilds the session, possibly on another account. What the
// old account was announced is not published under the new one, nor receipted.
func TestADumpOfAnAccountThatWasReplacedIsNotPublished(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_RECENT,
		&waHistorySync.Conversation{ID: proto.String("5511999990002@s.whatsapp.net"), Messages: []*waHistorySync.HistorySyncMsg{
			pastText("5511999990002@s.whatsapp.net", "3EB0OLD", 1754000000, "oi"),
		}})}
	bench.install(session)
	session.downloadHistory = func(context.Context, *wm.Client, *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
		// The rebuild a logout ends in, landing while the dump downloads.
		session.aliases.forget()
		return bench.dump, nil
	}
	holdDump(t, session, "NOTIF36", recentNotice(t))

	session.replayHistory()
	select {
	case emission := <-session.Events():
		t.Fatalf("the replaced account's dump published %s", emission.Type)
	default:
	}
	if got := bench.receipted(); len(got) != 0 {
		t.Fatalf("the replaced account's dump was receipted: %v", got)
	}
	// Finished with rather than tried again: there is nobody left to finish it for.
	session.dumps.mu.Lock()
	defer session.dumps.mu.Unlock()
	if session.dumps.armed {
		t.Fatal("the replaced account's dump armed a retry")
	}
}

// And a row read before the logout is not attempted on the account after it.
func TestAPendingDumpReadBeforeALogoutIsNotAttemptedAfterIt(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_RECENT)}
	bench.install(session)
	holdDump(t, session, "NOTIF37", recentNotice(t))
	holdDump(t, session, "NOTIF38", recentNotice(t))
	downloads := 0
	session.downloadHistory = func(context.Context, *wm.Client, *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
		downloads++
		session.aliases.forget()
		return bench.dump, nil
	}

	session.replayHistory()
	if downloads != 1 {
		t.Fatalf("after the account was replaced %d more dump(s) of the old one were downloaded", downloads-1)
	}
}

// Nor is a file of the replaced account recorded under the session: the fence is about the
// lease, and the row would hand the old account's file to whatever pairs next.
func TestAFileOfAnAccountThatWasReplacedIsNotRecorded(t *testing.T) {
	t.Parallel()

	const chat = "5511999990002@s.whatsapp.net"
	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	image := &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
		Mimetype: proto.String("image/jpeg"), DirectPath: proto.String("/v/old"), MediaKey: make([]byte, 32),
		FileSHA256: make([]byte, 32), FileEncSHA256: make([]byte, 32), FileLength: proto.Uint64(1024),
	}}
	bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_RECENT,
		&waHistorySync.Conversation{ID: proto.String(chat), Messages: []*waHistorySync.HistorySyncMsg{
			pastMessage(chat, "3EB0OLDFILE", 1754000000, false, image),
		}})}
	bench.install(session)
	session.downloadHistory = func(context.Context, *wm.Client, *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
		session.aliases.forget()
		return bench.dump, nil
	}
	holdDump(t, session, "NOTIF39", recentNotice(t))

	session.replayHistory()
	if _, found, err := session.store.MediaPart(t.Context(), "3EB0OLDFILE"); err != nil || found {
		t.Fatalf("the replaced account's file was recorded under the session (found %v, err %v)", found, err)
	}
}

// A replay that finished everything starts the next wait from the first one; one that left
// a dump behind keeps backing off, or a Redis that stays down is hit every half minute.
func TestOnlyAReplayThatFinishedEverythingResetsTheWait(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		fails bool
		reset bool
	}{
		{"finished", false, true},
		{"left one behind", true, false},
	} {
		session, _ := newTestSession(t, "5511999990001")
		session.historyRetry = time.Hour
		bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_RECENT)}
		bench.install(session)
		if tc.fails {
			session.downloadHistory = func(context.Context, *wm.Client, *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
				return nil, errors.New("dial tcp: i/o timeout")
			}
		}
		holdDump(t, session, "NOTIF40", recentNotice(t))
		session.dumps.wait = 4 * time.Minute

		session.replayHistory()
		session.dumps.mu.Lock()
		reset := session.dumps.wait == 0
		session.dumps.mu.Unlock()
		if reset != tc.reset {
			t.Errorf("%s: the wait was reset %v, want %v", tc.name, reset, tc.reset)
		}
	}
}

// A dump with nothing to publish reaches the receipt without a slice to stop at, and the
// receipt is the old account's to send, not the session's.
func TestAnEmptyDumpOfAnAccountThatWasReplacedIsNotReceipted(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_RECENT)}
	bench.install(session)
	session.downloadHistory = func(context.Context, *wm.Client, *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
		session.aliases.forget()
		return bench.dump, nil
	}
	holdDump(t, session, "NOTIF41", recentNotice(t))

	session.replayHistory()
	if got := bench.receipted(); len(got) != 0 {
		t.Fatalf("the replaced account's dump was receipted: %v", got)
	}
}

// The pairs in a dump are the account's that was announced it, and a logout during the
// download does not teach them to the account after it.
func TestTheDumpsPairsAreNotLearnedByTheAccountAfterIt(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	dump := dumpOf(waHistorySync.HistorySync_RECENT)
	dump.PhoneNumberToLidMappings = []*waHistorySync.PhoneNumberToLIDMapping{
		{PnJID: proto.String("5511999990002@s.whatsapp.net"), LidJID: proto.String("123456789012345@lid")},
	}
	bench := &historyBench{dump: dump}
	bench.install(session)
	session.downloadHistory = func(context.Context, *wm.Client, *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
		session.aliases.forget()
		return bench.dump, nil
	}
	holdDump(t, session, "NOTIF42", recentNotice(t))

	session.replayHistory()
	session.aliases.mu.RLock()
	defer session.aliases.mu.RUnlock()
	if len(session.aliases.seen) != 0 {
		t.Fatalf("the account after the logout learned the old one's pairs: %v", session.aliases.seen)
	}
}

// A replay that finds the account replaced leaves the rows alone: a row under the dump's
// id after a logout is the next account's, and the phone does not announce it again.
func TestAReplayForAReplacedAccountDeletesNothing(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_RECENT)}
	bench.install(session)
	session.historyRetry = time.Hour
	session.downloadHistory = func(context.Context, *wm.Client, *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
		session.aliases.forget()
		return bench.dump, nil
	}
	holdDump(t, session, "NOTIF45", recentNotice(t))

	session.replayHistory()
	if got := heldDumps(t, session); len(got) != 1 {
		t.Fatalf("the row under the replaced account's dump was deleted: %v", got)
	}
}

// A logout landing while the rows are read leaves them unattempted: they may be the next
// account's, read under the old one's generation.
func TestRowsReadAcrossALogoutAreNotAttempted(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.historyRetry = time.Hour
	bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_RECENT)}
	bench.install(session)
	holdDump(t, session, "NOTIF46", recentNotice(t))
	read := session.readPendingHistory
	session.readPendingHistory = func(ctx context.Context) ([]store.PendingHistory, error) {
		session.aliases.forget()
		return read(ctx)
	}

	session.replayHistory()
	if bench.downloads != 0 || len(bench.receipted()) != 0 {
		t.Fatalf("rows read across a logout were attempted: %d downloads, receipts %v", bench.downloads, bench.receipted())
	}
	if got := heldDumps(t, session); len(got) != 1 {
		t.Fatalf("rows read across a logout were deleted: %v", got)
	}
}

// A logout and a new pairing can land while a dump kept in memory is written, with the
// generation still the old one. The write is held to the device the dump reached, so it
// is not filed under the account that paired after it.
func TestAnUnwrittenDumpOfAnotherDeviceIsNotFiledUnderThisOne(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.historyRetry = time.Hour
	session.dumps.keepUnwritten(unwrittenDump{
		id: "NOTIF47", device: "5511999990009:3@s.whatsapp.net", notice: recentNotice(t),
		learned: 1755000000000, generation: session.aliases.learning(),
	})

	session.replayHistory()
	if got := heldDumps(t, session); len(got) != 0 {
		t.Fatalf("the other device's dump was filed under this one: %v", got)
	}
}

// The files a dump names are held to the device it reached, the same way its row is.
func TestAFileOfADumpForAnotherDeviceIsNotRecorded(t *testing.T) {
	t.Parallel()

	const chat = "5511999990002@s.whatsapp.net"
	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	session.historyRetry = time.Hour
	image := &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
		Mimetype: proto.String("image/jpeg"), DirectPath: proto.String("/v/other"), MediaKey: make([]byte, 32),
		FileSHA256: make([]byte, 32), FileEncSHA256: make([]byte, 32), FileLength: proto.Uint64(1024),
	}}
	bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_RECENT,
		&waHistorySync.Conversation{ID: proto.String(chat), Messages: []*waHistorySync.HistorySyncMsg{
			pastMessage(chat, "3EB0OTHERDEV", 1754000000, false, image),
		}})}
	bench.install(session)

	finished := make(chan bool, 1)
	go func() {
		finished <- session.finishDump(dumpAttempt{
			id: "NOTIF48", device: "5511999990009:3@s.whatsapp.net",
			notice:  &waE2E.HistorySyncNotification{SyncType: waE2E.HistorySyncType_RECENT.Enum()},
			learned: 1755000000000, generation: session.aliases.learning(), budget: time.Minute,
		})
	}()
	slicesUntil(t, session, finished)
	if _, found, err := session.store.MediaPart(t.Context(), "3EB0OTHERDEV"); err != nil || found {
		t.Fatalf("another device's file was recorded under the session (found %v, err %v)", found, err)
	}
}

// A request a dump sent by itself holds whatsmeow's send lock for as long as the send
// takes, and that lock reads no context. A `history.request` queued behind it waits for its
// turn under its own deadline instead, so it does not hold the session's command queue.
func TestAHistoryRequestWaitsForItsTurnUnderItsOwnDeadline(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.wireLimit = 50 * time.Millisecond
	(&historyBench{}).install(session)
	sent := 0
	session.sendPeer = func(context.Context, *wm.Client, *waE2E.Message) error {
		sent++
		return nil
	}
	// The turn the dump's own request is holding.
	session.sending <- struct{}{}

	answered := make(chan error, 1)
	go func() {
		_, err := session.Execute(context.WithoutCancel(t.Context()), historyRequest(t,
			`{"chat":{"kind":"phone","id":"5511999990002"},"before":{"id":"3EB0F2","timestamp":1754000000123,"from_me":false}}`))
		answered <- err
	}()
	select {
	case err := <-answered:
		var coded *protocol.Error
		if !errors.As(err, &coded) || coded.Code != protocol.ErrorTimeout {
			t.Fatalf("a request that never got its turn answered %v, want timeout", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a request waiting for its turn held the session past its deadline")
	}
	if sent != 0 {
		t.Fatalf("a request that never got its turn was sent %d times", sent)
	}
}

// Every message of a slice carries the slice's chat, whatever address it resolved itself.
func TestEveryMessageOfASliceIsAddressedToTheSlicesChat(t *testing.T) {
	t.Parallel()

	chat := protocol.Address{Kind: protocol.AddressLID, ID: "123456789012345"}
	messages := []protocol.InboundMessage{
		{ID: "3EB0A1", Chat: protocol.Address{Kind: protocol.AddressPhone, ID: "5511999990002"}},
		{ID: "3EB0A2", Chat: chat},
	}
	addressedTo(messages, chat)
	for _, message := range messages {
		if message.Chat != chat {
			t.Fatalf("message %s is addressed to %+v in a slice of %+v", message.ID, message.Chat, chat)
		}
	}
}

// A message send lands behind the request for history a dump sends by itself the same way
// a `history.request` does, and waits for its turn under its own deadline too.
func TestAMessageSendWaitsForItsTurnUnderItsOwnDeadline(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.wireLimit = 50 * time.Millisecond
	handed := 0
	session.handOver = func(context.Context, waTypes.JID, string, *waE2E.Message) (wm.SendResponse, error) {
		handed++
		return wm.SendResponse{}, nil
	}
	// The turn the dump's own request is holding.
	session.sending <- struct{}{}

	answered := make(chan error, 1)
	go func() {
		_, err := session.putOnTheWire(t.Context(), waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
			"3EB0TURN", &waE2E.Message{Conversation: proto.String("oi")})
		answered <- err
	}()
	select {
	case err := <-answered:
		var coded *protocol.Error
		if !errors.As(err, &coded) || coded.Code != protocol.ErrorTimeout {
			t.Fatalf("a send that never got its turn answered %v, want timeout", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a send waiting for its turn held the session past its deadline")
	}
	if handed != 0 {
		t.Fatalf("a send that never got its turn was handed to the socket %d times", handed)
	}
}

// A group's history is left out for a client that did not ask for groups, except the
// answer to its own `history.request`: dropped, the client waits for an answer that never
// comes, not even the empty one that tells it to stop asking.
func TestAGroupsOnDemandAnswerIsPublishedWithoutTheGroupsSubscription(t *testing.T) {
	t.Parallel()

	group := groupJID().String()
	for _, tc := range []struct {
		name    string
		kind    waHistorySync.HistorySync_HistorySyncType
		notice  waE2E.HistorySyncType
		publish bool
	}{
		{"an answer to a request", waHistorySync.HistorySync_ON_DEMAND, waE2E.HistorySyncType_ON_DEMAND, true},
		{"a dump nobody asked for", waHistorySync.HistorySync_RECENT, waE2E.HistorySyncType_RECENT, false},
	} {
		session, _ := newTestSession(t, "5511999990001")
		session.setHistory(true)
		bench := &historyBench{dump: dumpOf(tc.kind, &waHistorySync.Conversation{ID: proto.String(group)})}
		bench.install(session)

		acknowledged := make(chan bool, 1)
		go func() { acknowledged <- session.receive(historyNotification("NOTIF49", tc.notice)) }()
		slices, _ := slicesUntil(t, session, acknowledged)
		if published := len(slices) == 1 && slices[0].Exhausted; published != tc.publish {
			t.Errorf("%s: published %+v, want the empty exhausted answer %v", tc.name, slices, tc.publish)
		}
	}
}

// The queue and the pump between a slice being queued and being written are room enough for
// a logout and a new pairing, so the slice is asked again at the write.
func TestASliceIsAskedAtTheWriteWhetherTheAccountIsStillTheSame(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setHistory(true)
	bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_RECENT,
		&waHistorySync.Conversation{ID: proto.String("5511999990002@s.whatsapp.net"), Messages: []*waHistorySync.HistorySyncMsg{
			pastText("5511999990002@s.whatsapp.net", "3EB0CLAIM", 1754000000, "oi"),
		}})}
	bench.install(session)

	go session.receive(historyNotification("NOTIF50", waE2E.HistorySyncType_RECENT))
	emission := next(t, session)
	if emission.Claim == nil || !emission.Claim() {
		t.Fatal("a slice of the session's own account is not claimed at the write")
	}
	session.aliases.forget()
	if emission.Claim() {
		t.Fatal("a slice of an account the session no longer holds is still claimed at the write")
	}
	emission.Settle(nil)
}
