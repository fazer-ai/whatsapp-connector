package whatsmeow

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"time"

	wm "go.mau.fi/whatsmeow"
	waCompanionReg "go.mau.fi/whatsmeow/proto/waCompanionReg"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waHistorySync "go.mau.fi/whatsmeow/proto/waHistorySync"
	waWa6 "go.mau.fi/whatsmeow/proto/waWa6"
	waWeb "go.mau.fi/whatsmeow/proto/waWeb"
	waStore "go.mau.fi/whatsmeow/store"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// historySliceLimit is how many messages one history.sync event carries at most. A chat
// longer than this arrives in several events, so no stream entry holds a whole dump.
const historySliceLimit = 100

func (s *Session) setHistory(history bool) {
	s.mu.Lock()
	s.history = history
	s.mu.Unlock()
}

func (s *Session) wantsHistory() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.history
}

// downloadHistoryOverClient is the default for the seam of the same name. The storage is
// synchronous so that what whatsmeow keeps from a dump is written before it is receipted.
func downloadHistoryOverClient(ctx context.Context, client *wm.Client, notification *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
	return client.DownloadHistorySync(ctx, notification, true) //nolint:wrapcheck // wrapped by its caller
}

func receiptHistoryOverClient(ctx context.Context, client *wm.Client, id waTypes.MessageID) error {
	return client.SendProtocolMessageReceipt(ctx, id, waTypes.ReceiptTypeHistorySync) //nolint:wrapcheck // wrapped by its caller
}

func sendPeerOverClient(ctx context.Context, client *wm.Client, message *waE2E.Message) error {
	_, err := client.SendPeerMessage(ctx, message)
	return err //nolint:wrapcheck // wrapped by its caller
}

// historyNotice reports whether a message is the phone announcing a history dump, which is
// the one protocol message this session does something with rather than dropping.
func historyNotice(event *waEvents.Message) *waE2E.HistorySyncNotification {
	if !event.Info.IsFromMe {
		return nil
	}
	notice := event.Message.GetProtocolMessage()
	if notice.GetType() != waE2E.ProtocolMessage_HISTORY_SYNC_NOTIFICATION {
		return nil
	}
	return notice.GetHistorySyncNotification()
}

// historySyncs is what each kind of dump is published as. The kinds missing here carry no
// conversation: the status feed, the push names, the settings, the notice that there is no
// history at all.
var historySyncs = map[waHistorySync.HistorySync_HistorySyncType]protocol.HistorySync{
	waHistorySync.HistorySync_INITIAL_BOOTSTRAP: protocol.HistoryBootstrap,
	waHistorySync.HistorySync_RECENT:            protocol.HistoryRecent,
	waHistorySync.HistorySync_FULL:              protocol.HistoryFull,
	waHistorySync.HistorySync_ON_DEMAND:         protocol.HistoryOnDemand,
}

// receiveHistory handles a history dump and reports whether its notification may be
// acknowledged.
//
// The dump is downloaded whether or not the client asked for history, because the download
// is also where whatsmeow stores what the live traffic depends on: the pairs of phone
// number and LID, and the names people gave themselves. What the client's choice decides
// is whether the conversations in it are published.
//
// The receipt is what tells the phone the dump arrived, and it is sent last, after every
// slice was published. A dump receipted before that is a dump a lost Redis loses, which is
// invariant 4 for a whole account's history at once; withheld, WhatsApp sends the
// notification again and the dump is downloaded again.
func (s *Session) receiveHistory(event *waEvents.Message, notice *waE2E.HistorySyncNotification, learned int64) bool {
	client := s.current()
	dump, err := s.downloadHistory(s.ctx, client, notice)
	var gone refused
	switch {
	case err != nil && errors.As(downloadFailure(err), &gone):
		// The blob is gone or is not the one the notification describes, and a
		// redelivery names the same blob: withheld, the phone would send it again for
		// good. Receipted, the dump is lost, which it already was.
		// Redacted, as every download error is: whatsmeow puts the blob's URL in it,
		// built from the direct path and the hash.
		s.log.Warn().Str("error", redact(err.Error())).Str("message_id", event.Info.ID).
			Msg("receipting a history dump whose blob can no longer be downloaded")
		s.receiptDump(client, event.Info.ID)
		return true
	case err != nil:
		s.log.Warn().Str("error", redact(err.Error())).Str("message_id", event.Info.ID).
			Msg("withholding the acknowledgement for a history dump that did not download")
		return false
	}

	sync, conversational := historySyncs[dump.GetSyncType()]
	if conversational && s.wantsHistory() {
		for _, conversation := range dump.GetConversations() {
			if !s.publishConversation(client, sync, dump, conversation, learned) {
				return false
			}
		}
	}

	s.receiptDump(client, event.Info.ID)
	return true
}

// receiptDump tells the phone a dump arrived. A failure is logged and nothing else:
// whatever the dump held was already published or stored, and the phone sending the
// notification again costs a download and slices the client deduplicates.
func (s *Session) receiptDump(client *wm.Client, id waTypes.MessageID) {
	if err := s.receiptHistory(s.ctx, client, id); err != nil {
		s.log.Warn().Err(err).Str("message_id", id).Msg("could not receipt a history dump")
	}
}

// publishConversation publishes one chat of a dump, oldest first, in slices of at most
// historySliceLimit messages, and reports whether every slice was published.
func (s *Session) publishConversation(client *wm.Client, sync protocol.HistorySync, dump *waHistorySync.HistorySync, conversation *waHistorySync.Conversation, learned int64) bool {
	jid, err := waTypes.ParseJID(conversation.GetID())
	if err != nil {
		s.log.Debug().Err(err).Msg("skipping a chat in a history dump that names no chat")
		return true
	}
	kind, named := addressOf(jid)
	if !named || jid.IsBot() || !conversational(kind.Kind) ||
		(kind.Kind == protocol.AddressGroup && !s.wantsGroups()) {
		return true
	}
	// Through the session's resolver, which is what each message in the slice is addressed
	// by: a slice naming a chat by phone while its messages name it by LID is two chats to
	// a client, and a `message.download_media` naming the slice's chat misses the file.
	looking, done := s.looking()
	chat, _ := s.address(looking, jid)
	done()

	messages := make([]protocol.InboundMessage, 0, len(conversation.GetMessages()))
	for _, past := range conversation.GetMessages() {
		if message, ok := s.pastMessageOf(client, jid, past.GetMessage()); ok {
			messages = append(messages, message)
		}
	}
	// The phone lists a chat newest first, and a client imports it in the order it arrives.
	sort.SliceStable(messages, func(i, j int) bool { return messages[i].Timestamp < messages[j].Timestamp })

	exhausted := exhaustedBy(sync, conversation)
	if len(messages) == 0 && !exhausted {
		return true
	}

	var name string
	if chat.Kind == protocol.AddressGroup {
		name = conversation.GetName()
	}
	var progress *int
	if dump.Progress != nil {
		value := int(dump.GetProgress())
		progress = &value
	}

	for start := 0; start == 0 || start < len(messages); start += historySliceLimit {
		end := min(start+historySliceLimit, len(messages))
		slice := protocol.HistorySlice{
			Kind: protocol.HistoryKindMessages, Sync: sync, Chat: chat, Name: name,
			Messages: messages[start:end], Progress: progress,
			// On the last slice of the chat only: until then there is more on its way.
			Exhausted: exhausted && end == len(messages),
		}
		if !s.deliver(protocol.EventHistorySync, slice, learned) {
			return false
		}
	}
	return true
}

// exhaustedBy reports whether the phone said it has nothing older for this chat.
//
// Read off what the phone sent, not off what is left after filtering: an on-demand answer
// holding only reactions publishes nothing and still has older messages behind it. The end
// of a transfer is not the answer either, because WhatsApp marks the end of every transfer
// and says separately whether more remains on the phone. A transfer that ended with more
// the phone will not share counts as exhausted, since asking again gets no further.
func exhaustedBy(sync protocol.HistorySync, conversation *waHistorySync.Conversation) bool {
	if sync == protocol.HistoryOnDemand && len(conversation.GetMessages()) == 0 {
		return true
	}
	if conversation.EndOfHistoryTransferType == nil {
		return false
	}
	switch conversation.GetEndOfHistoryTransferType() {
	case waHistorySync.Conversation_COMPLETE_AND_NO_MORE_MESSAGE_REMAIN_ON_PRIMARY,
		waHistorySync.Conversation_COMPLETE_ON_DEMAND_SYNC_WITH_MORE_MSG_ON_PRIMARY_BUT_NO_ACCESS:
		return true
	default:
		return false
	}
}

// conversational reports whether a chat of this kind is a conversation a dump is
// published for. The status feed, a broadcast list and a channel are in a dump too, and
// none of them is a conversation with somebody: a list's messages are already in the
// direct chat of each recipient, and the other two are feeds.
func conversational(kind protocol.AddressKind) bool {
	return kind == protocol.AddressPhone || kind == protocol.AddressLID || kind == protocol.AddressGroup
}

// pastMessageOf renders one message of a dump the way a live one is rendered, and reports
// whether it is one a conversation shows.
//
// What is left out is what the live path turns into something other than a message: a
// reaction, an edit, a deletion, a vote. In a dump those arrive already applied to the
// message they change, so publishing them as well would only put a second bubble next to it.
func (s *Session) pastMessageOf(client *wm.Client, chat waTypes.JID, past *waWeb.WebMessageInfo) (protocol.InboundMessage, bool) {
	if past == nil {
		return protocol.InboundMessage{}, false
	}
	event, err := client.ParseWebMessage(chat, past)
	if err != nil {
		s.log.Debug().Err(err).Str("message_id", past.GetKey().GetID()).
			Msg("skipping a message in a history dump that could not be read")
		return protocol.InboundMessage{}, false
	}
	message := event.Message
	switch {
	case event.Info.Sender.IsBot(),
		bodyless(message),
		message.GetProtocolMessage() != nil,
		message.GetReactionMessage() != nil,
		message.GetEncReactionMessage() != nil,
		marksAMessage(message):
		return protocol.InboundMessage{}, false
	}
	inbound, _, ok := s.inboundOf(event, s.pastBody)
	return inbound, ok
}

// pastBody renders the body of a message from a dump.
//
// The same as a live one except for the file, which is never fetched here: a dump can
// carry thousands of pictures, and downloading each before publishing its slice would hold
// the whole dump behind the slowest of them. The message goes out with no reference, and
// what it takes to fetch the file later is recorded, so `message.download_media` can.
func (s *Session) pastBody(event *waEvents.Message) (body, bool) {
	if plain, ok := plainBody(event); ok {
		return plain, true
	}
	if shared, ok := sharedBody(event); ok {
		return shared, true
	}
	if part, isAFile := attachmentOf(event.Message); isAFile {
		if viewOnce(event) {
			// Neither kept nor previewed, for the reason the live path gives.
			part.content.Thumbnail = ""
			return body{content: part.content, context: part.context}, true
		}
		s.remember(event, &part)
		return body{content: part.content, context: part.context}, true
	}
	return unreadableBody(event)
}

// historyAsk is the payload of `history.request`.
type historyAsk struct {
	Chat   *protocol.Address `json:"chat"`
	Count  *int              `json:"count"`
	Before *struct {
		ID        string `json:"id"`
		Timestamp int64  `json:"timestamp"`
		FromMe    bool   `json:"from_me"`
	} `json:"before"`
}

// historyAskCount is how many messages a `history.request` asks for when it does not say.
const historyAskCount = 50

// requestHistory is `history.request`: it asks the phone for the messages of a chat that
// came before one the client already has.
//
// Nothing comes back in the answer. The phone sends what it has as a dump of its own,
// which arrives as `history.sync` with `on_demand`, or never, if the phone is offline.
func (s *Session) requestHistory(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	var ask historyAsk
	if err := json.Unmarshal(command.Payload, &ask); err != nil || ask.Chat == nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload, "a history request has to name the chat")
	}
	chat, err := jidOf(*ask.Chat)
	if err != nil {
		return nil, err
	}
	if ask.Before == nil || ask.Before.ID == "" {
		// The phone walks back from a message it is shown, and has no way to be asked for
		// a chat from the start.
		return nil, protocol.NewError(protocol.ErrorUnsupported,
			"a history request has to name the message to walk back from")
	}
	count := historyAskCount
	if ask.Count != nil {
		if *ask.Count < 1 || *ask.Count > math.MaxInt32 {
			return nil, protocol.NewError(protocol.ErrorInvalidPayload, "a history request asks for at least one message")
		}
		count = *ask.Count
	}
	if err := s.readyToSend(); err != nil {
		return nil, err
	}

	client := s.current()
	request := client.BuildHistorySyncRequest(&waTypes.MessageInfo{
		MessageSource: waTypes.MessageSource{Chat: chat, IsFromMe: ask.Before.FromMe},
		ID:            ask.Before.ID,
		Timestamp:     time.UnixMilli(ask.Before.Timestamp),
	}, count)
	if err := s.sendPeer(ctx, client, request); err != nil {
		if named, coded := commandFailure(err, "history request"); named {
			return nil, coded
		}
		return nil, protocol.NewError(protocol.ErrorWaError, "WhatsApp refused the history request")
	}
	return nil, nil
}

// payloadWithHistory is the handshake payload this session dials with: whatsmeow's own,
// with the device properties saying whether the phone should send its whole history.
//
// Per session and not in the process-wide properties, because it is each client's choice:
// a device that asks for the full sync makes the phone upload everything it has, and a
// session whose client did not ask for history should not have its account do that on
// its behalf. The properties only travel on a pairing, so a paired device's login payload
// goes out untouched.
func (s *Session) payloadWithHistory(client *wm.Client) func() *waWa6.ClientPayload {
	return func() *waWa6.ClientPayload {
		payload := client.Store.GetClientPayload()
		if pairing := payload.GetDevicePairingData(); pairing != nil {
			props, cloned := proto.Clone(waStore.DeviceProps).(*waCompanionReg.DeviceProps)
			if !cloned {
				return payload
			}
			props.RequireFullSync = proto.Bool(s.wantsHistory())
			if marshalled, err := proto.Marshal(props); err == nil {
				pairing.DeviceProps = marshalled
			}
		}
		return payload
	}
}
