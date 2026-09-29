package whatsmeow

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"slices"
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
	"github.com/fazer-ai/whatsapp-connector/internal/store"
)

// historySliceLimit is how many messages one history.sync event carries at most. A chat
// longer than this arrives in several events, so no stream entry holds a whole dump.
const historySliceLimit = 100

// historyBudget is how long one dump may spend being published before it is left for a
// later attempt. Three minutes, so that the last slice, which may wait out the whole
// deliverTimeout, still ends under the five minutes whatsmeow gives a node handler.
const historyBudget = 5*time.Minute - deliverTimeout - time.Minute

// historyReceiptTimeout bounds the one write a dump makes after its budget: the receipt, or
// the request to upload it again. Half of the minute the budget leaves over.
const historyReceiptTimeout = 30 * time.Second

// historyRetry is the wait before a dump that did not finish is tried again, doubled after
// every attempt that does not finish either, up to historyRetryCeiling. What usually stops
// a dump is the client's Redis, and each attempt downloads the dump again, so an outage of
// an hour costs a dozen downloads rather than one every half minute.
const (
	historyRetry        = 30 * time.Second
	historyRetryCeiling = 5 * time.Minute
)

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
// Written, not known to be written: a failure to store the dump's message secrets is
// logged inside whatsmeow and never returned (#350).
func downloadHistoryOverClient(ctx context.Context, client *wm.Client, notification *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
	return client.DownloadHistorySync(ctx, notification, true) //nolint:wrapcheck // wrapped by its caller
}

func receiptHistoryOverClient(ctx context.Context, client *wm.Client, id waTypes.MessageID) error {
	return client.SendProtocolMessageReceipt(ctx, id, waTypes.ReceiptTypeHistorySync) //nolint:wrapcheck // wrapped by its caller
}

// reuploadHistoryOverClient asks the phone to upload a dump again, which is the answer to
// a blob the CDN no longer has.
func reuploadHistoryOverClient(ctx context.Context, client *wm.Client, id waTypes.MessageID, mediaKey []byte) error {
	return client.SendHistorySyncServerErrorReceipt(ctx, id, mediaKey) //nolint:wrapcheck // wrapped by its caller
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

// receiveHistory writes a history dump down before doing anything with it, and reports
// whether its notification may be acknowledged: only when the row is written.
//
// The row is what brings back a dump that does not finish, and nothing else does. The
// phone announces a dump once, whatsmeow answers the announcement with a peer receipt
// before this runs, and a notification whose node was left unacknowledged was measured not
// to come again, on the same socket or on a new one. So a dump that stops here -- the
// client's Redis gone, the budget spent, a lease lost -- is tried again by this session
// after a wait, and by whichever session connects the account next.
func (s *Session) receiveHistory(event *waEvents.Message, notice *waE2E.HistorySyncNotification, learned int64) bool {
	id := event.Info.ID
	generation := s.aliases.learning()
	if !s.dumps.claim(id) {
		// The same notification is already being worked on by a retry, which answers for
		// it: two attempts at one dump would publish it twice.
		return true
	}
	defer s.dumps.release(id)

	raw, err := proto.Marshal(notice)
	if err == nil {
		// Bounded like every store write in the handler, which is part of what keeps it
		// under whatsmeow's watchdog.
		writing, cancel := context.WithTimeout(s.ctx, s.storeLimit)
		err = s.store.PutPendingHistory(writing, &store.PendingHistory{MessageID: id, Notice: raw, LearnedAt: learned})
		cancel()
	}
	if err != nil {
		s.log.Warn().Err(err).Str("message_id", id).Msg("withholding a history dump that could not be written down")
		return false
	}
	s.finishDump(id, notice, learned, generation)
	return true
}

// finishDump makes one attempt at a dump that is written down, and forgets it when the
// attempt finished with it.
func (s *Session) finishDump(id string, notice *waE2E.HistorySyncNotification, learned int64, generation uint64) bool {
	if !s.handleDump(id, notice, learned, generation) {
		s.retryHistoryLater()
		return false
	}
	dropping, cancel := context.WithTimeout(s.ctx, s.storeLimit)
	defer cancel()
	if err := s.store.DropPendingHistory(dropping, id); err != nil {
		// Left for the next owner, which publishes it again: the client deduplicates.
		s.log.Warn().Err(err).Str("message_id", id).Msg("could not forget a history dump that is finished with")
	}
	return true
}

// retryHistoryLater arms one attempt at the dumps still written down, unless one is armed.
func (s *Session) retryHistoryLater() {
	wait, arm := s.dumps.arm(s.historyRetry, s.historyRetryCeiling)
	if !arm {
		return
	}
	go func() {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-s.ctx.Done():
			return
		case <-timer.C:
		}
		s.dumps.disarm()
		s.replayHistory()
	}()
}

// replayHistory works through the dumps this session has written down and not finished
// with, its own or a previous owner's, oldest first. Two replays at once are harmless: each
// dump is claimed by one attempt, so the second only picks up what the first did not list.
func (s *Session) replayHistory() {
	if s.replayOnce() {
		s.dumps.settle()
		return
	}
	s.retryHistoryLater()
}

// replayOnce makes one attempt at every dump written down, and reports whether all of them
// finished.
func (s *Session) replayOnce() bool {
	// Before the read: a row read after a logout is the next account's, and one read
	// before it is published only while the account is still the one read under.
	generation := s.aliases.learning()
	held, err := s.store.PendingHistory(s.ctx)
	if err != nil {
		s.log.Warn().Err(err).Msg("could not read the history dumps this session left pending")
		return false
	}
	finished := true
	for _, pending := range held {
		if s.ctx.Err() != nil {
			return false
		}
		if !s.dumps.claim(pending.MessageID) {
			continue
		}
		var notice waE2E.HistorySyncNotification
		if err := proto.Unmarshal(pending.Notice, &notice); err != nil {
			// A row nothing can read is a row no attempt will ever finish, and every owner
			// after this one would fail on it the same way.
			s.log.Error().Err(err).Str("message_id", pending.MessageID).
				Msg("dropping a pending history dump this build cannot read")
			if err := s.store.DropPendingHistory(s.ctx, pending.MessageID); err != nil {
				s.log.Warn().Err(err).Str("message_id", pending.MessageID).Msg("could not drop an unreadable history dump")
			}
			s.dumps.release(pending.MessageID)
			continue
		}
		s.log.Info().Str("message_id", pending.MessageID).Msg("picking up a history dump that did not finish")
		if !s.finishDump(pending.MessageID, &notice, pending.LearnedAt, generation) {
			finished = false
		}
		s.dumps.release(pending.MessageID)
	}
	return finished
}

// handleDump makes one attempt at a history dump and reports whether it is finished with.
//
// The dump is downloaded whether or not the client asked for history, because the download
// is also where whatsmeow stores what the live traffic depends on: the pairs of phone
// number and LID, and the names people gave themselves. What the client's choice decides
// is whether the conversations in it are published.
//
// The receipt is what tells the phone the dump arrived, and it is sent last, after every
// slice was published. A dump that did not get there is not receipted and stays written
// down, and the next attempt downloads it again.
//
// A dump belongs to the account that was announced it. A logout during an attempt rebuilds
// the session, possibly on another account, and deletes the row with the old device, so
// an attempt that finds the account replaced stops publishing and reports the dump
// finished with: there is nobody left to finish it for.
//
// generation is the account the caller found the dump under, read before the dump was:
// the client and the generation are swapped under one lock, so a rebuild after that read
// leaves an old generation beside a new client, which the checks below catch.
func (s *Session) handleDump(id string, notice *waE2E.HistorySyncNotification, learned int64, generation uint64) bool {
	client := s.current()
	if s.aliases.learning() != generation {
		s.log.Info().Str("message_id", id).Msg("dropping a history dump of an account this session no longer holds")
		return true
	}
	// The dump is handled inside the node handler, and whatsmeow starts the next node
	// alongside a handler that has run for five minutes, which is the order of everything
	// after it gone. So the whole of it has a budget, from the download on: whatsmeow's
	// media client has no overall timeout of its own, and a dump's rows are one store write
	// per file. Over it, the dump is left unreceipted for the next attempt, which publishes it
	// whole, and the client deduplicates what it already has.
	ctx, cancel := context.WithTimeout(s.ctx, s.historyBudget)
	defer cancel()
	// Stamped with the dump's own account, the way a command is: a logout during it
	// rebuilds the session on another account, and the pairs in this dump are the old one's.
	learning := context.WithValue(s.ctx, generationKey{}, generation)
	dump, err := s.downloadHistory(ctx, client, notice)
	var gone refused
	switch {
	case err != nil && errors.As(downloadFailure(err), &gone):
		// The blob is gone or is not the one the notification describes, and another
		// attempt at this notification names the same blob, so keeping it would fail
		// for good. What the phone can do is upload the dump
		// again, which is what a server-error receipt asks for; the new upload arrives
		// as a notification of its own. The dump is not receipted as done, because it
		// is not. Only a request that did not go out keeps this one, so it is asked
		// again on the next attempt.
		// Redacted, as every download error is: whatsmeow puts the blob's URL in it,
		// built from the direct path and the hash.
		s.log.Warn().Str("error", redact(err.Error())).Str("message_id", id).
			Msg("asking the phone to upload again a history dump whose blob can no longer be downloaded")
		asking, cancel := context.WithTimeout(s.ctx, s.historyReceiptWait)
		defer cancel()
		if err := s.reuploadHistory(asking, client, id, notice.GetMediaKey()); err != nil {
			s.log.Warn().Err(err).Str("message_id", id).
				Msg("keeping for another attempt a history dump the phone could not be asked to upload again")
			return false
		}
		return true
	case err != nil:
		s.log.Warn().Str("error", redact(err.Error())).Str("message_id", id).
			Msg("keeping for another attempt a history dump that did not download")
		return false
	}

	// The dump is this account's own, so the pairs of number and LID in it are pairs it
	// was shown. whatsmeow writes them to the store every session shares, which the
	// resolver deliberately does not read; learned here, the history below and the live
	// traffic after it name each person the same way.
	for _, pair := range dump.GetPhoneNumberToLidMappings() {
		s.aliases.observe(learning, parsedJIDs(pair.GetPnJID(), pair.GetLidJID())...)
	}
	// And the pair a conversation names its other party by, which is not always repeated
	// in the list above.
	for _, conversation := range dump.GetConversations() {
		s.aliases.observe(learning,
			parsedJIDs(conversation.GetID(), conversation.GetPnJID(), conversation.GetLidJID())...)
	}

	sync, conversational := historySyncs[dump.GetSyncType()]
	run := &dumpRun{s: s, ctx: ctx, client: client, sync: sync, dump: dump, learned: learned, generation: generation}
	publishing := conversational && s.wantsHistory()
	if publishing {
		for _, conversation := range dump.GetConversations() {
			if !run.publishConversation(conversation) {
				return run.replaced()
			}
		}
	}
	// One line per dump, with counts and nothing a message said: without it, a dump the
	// phone sent empty and one whose messages were all filtered here look the same from
	// outside, and that is the first question anybody asks about history that did not show.
	sent := 0
	for _, conversation := range dump.GetConversations() {
		sent += len(conversation.GetMessages())
	}
	s.log.Info().Str("message_id", id).Str("sync_type", dump.GetSyncType().String()).
		Uint32("chunk", dump.GetChunkOrder()).Uint32("progress", dump.GetProgress()).
		Int("conversations", len(dump.GetConversations())).Int("messages_sent", sent).
		Bool("publishing", publishing).Int("slices", run.slices).Int("messages_published", run.published).
		Msg("history dump handled")
	if run.replaced() {
		return true
	}

	s.receiptDump(client, id)
	return true
}

// receiptDump tells the phone a dump arrived. A failure is logged and nothing else:
// whatever the dump held was already published or stored.
//
// Bounded, because it is a write on the socket inside the node handler, and by a wait of
// its own rather than the send ceiling, which is minutes: what is left of the handler's
// five minutes after the dump's budget and the last slice's wait is what it gets.
func (s *Session) receiptDump(client *wm.Client, id waTypes.MessageID) {
	ctx, cancel := context.WithTimeout(s.ctx, s.historyReceiptWait)
	defer cancel()
	if err := s.receiptHistory(ctx, client, id); err != nil {
		s.log.Warn().Err(err).Str("message_id", id).Msg("could not receipt a history dump")
	}
}

// parsedJIDs is the JIDs among these strings that parse, which is what observe takes a
// pair out of: it keeps the first number and the first LID it is handed.
func parsedJIDs(raw ...string) []waTypes.JID {
	jids := make([]waTypes.JID, 0, len(raw))
	for _, one := range raw {
		if one == "" {
			continue
		}
		if jid, err := waTypes.ParseJID(one); err == nil {
			jids = append(jids, jid)
		}
	}
	return jids
}

// dumpRun is one dump being published: what every chat in it shares, and whether a file
// in it could not be recorded.
type dumpRun struct {
	s       *Session
	ctx     context.Context // the dump's budget
	client  *wm.Client
	sync    protocol.HistorySync
	dump    *waHistorySync.HistorySync
	learned int64
	// generation is the account the dump was announced to, and gone is it having been
	// replaced, said once.
	generation uint64
	gone       bool
	// slices and published count what went out, for the line the dump is logged with.
	slices, published int
	// unkept is a file this run published and could not record how to fetch. A dump's
	// media carries no reference, so that row is the only way the file is ever fetched,
	// and the dump is withheld for it the way a message is for an event that did not
	// publish.
	unkept bool
}

// overBudget reports whether the dump has run out of time, and says so once.
func (r *dumpRun) overBudget() bool {
	if r.ctx.Err() == nil {
		return false
	}
	r.s.log.Warn().Dur("budget", r.s.historyBudget).
		Msg("keeping for another attempt a history dump that did not publish within its budget")
	return true
}

// replaced reports whether the account the dump belongs to is no longer this session's.
func (r *dumpRun) replaced() bool {
	if r.gone {
		return true
	}
	if r.s.aliases.learning() == r.generation {
		return false
	}
	r.gone = true
	r.s.log.Info().Msg("dropping a history dump of an account this session no longer holds")
	return true
}

// publishConversation publishes one chat of a dump, oldest first, in slices of at most
// historySliceLimit messages, and reports whether every slice was published.
func (r *dumpRun) publishConversation(conversation *waHistorySync.Conversation) bool {
	s, sync, dump := r.s, r.sync, r.dump
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
		if r.overBudget() {
			return false
		}
		if message, ok := r.pastMessageOf(jid, past.GetMessage()); ok {
			messages = append(messages, message)
		}
	}
	if r.unkept {
		s.log.Warn().Msg("keeping for another attempt a history dump with a file whose coordinates could not be kept")
		return false
	}
	// The phone lists a chat newest first, and a client imports it in the order it arrives.
	// Reversed before the sort, because a dump's clock has seconds only: messages sent
	// within one second tie, and the stable sort keeps them in the order it was given.
	slices.Reverse(messages)
	sort.SliceStable(messages, func(i, j int) bool { return messages[i].Timestamp < messages[j].Timestamp })

	exhausted := exhaustedBy(sync, conversation)
	if len(messages) == 0 && !exhausted {
		if sync == protocol.HistoryOnDemand {
			// A page of nothing but reactions, corrections and the like. Published as
			// nothing, it leaves the client asking again from the anchor it already used,
			// which gets it this same page for good; so the next page is asked for here,
			// from the oldest message the phone sent.
			return r.askPastThePage(jid, conversation)
		}
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
			Kind: protocol.HistoryKindMessages, Progress: progress,
			Data: protocol.HistoryMessages{
				Sync: sync, Chat: chat, Name: name, Messages: messages[start:end],
				// On the last slice of the chat only: until then there is more on its way.
				Exhausted: exhausted && end == len(messages),
			},
		}
		if r.overBudget() || r.replaced() {
			return false
		}
		if !s.deliver(protocol.EventHistorySync, slice, r.learned) {
			return false
		}
		r.slices++
		r.published += end - start
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

// askPastThePage asks the phone for what came before the oldest message of an on-demand
// page that published nothing.
//
// Off the node handler, the way a call is refused: whatsmeow sends a peer message under
// the lock every send takes, which does not watch a context, so a send already on the wire
// would hold the handler for as long as that one takes. Nothing waits on the answer
// either, since it arrives as a dump of its own; a request that did not go out is logged,
// and the client asking again gets this page again, which is where it was before.
func (r *dumpRun) askPastThePage(chat waTypes.JID, conversation *waHistorySync.Conversation) bool {
	var oldest *waWeb.WebMessageInfo
	for _, past := range conversation.GetMessages() {
		message := past.GetMessage()
		if message.GetKey().GetID() == "" {
			continue
		}
		if oldest == nil || message.GetMessageTimestamp() < oldest.GetMessageTimestamp() {
			oldest = message
		}
	}
	if oldest == nil {
		return true
	}
	s, client := r.s, r.client
	request := client.BuildHistorySyncRequest(&waTypes.MessageInfo{
		MessageSource: waTypes.MessageSource{Chat: chat, IsFromMe: oldest.GetKey().GetFromMe()},
		ID:            oldest.GetKey().GetID(),
		Timestamp:     time.Unix(int64(oldest.GetMessageTimestamp()), 0), //nolint:gosec // a WhatsApp timestamp in seconds fits
	}, historyAskCount)
	go func() {
		ctx, cancel := context.WithTimeoutCause(s.ctx, s.wireLimit, errSendCeiling)
		defer cancel()
		if err := s.sendPeer(ctx, client, request); err != nil {
			s.log.Warn().Err(err).Msg("could not ask for the page before an on-demand page that published nothing")
		}
	}()
	return true
}

// pastMessageOf renders one message of a dump the way a live one is rendered, and reports
// whether it is one a conversation shows.
//
// What is left out is what the live path turns into something other than a message: a
// reaction, an edit, a deletion, a vote. In a dump those arrive already applied to the
// message they change, so publishing them as well would only put a second bubble next to it.
func (r *dumpRun) pastMessageOf(chat waTypes.JID, past *waWeb.WebMessageInfo) (protocol.InboundMessage, bool) {
	s, client := r.s, r.client
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
	case event.IsEdit, resentEdit(event):
		// A correction, which the parser has already turned into the message it corrects:
		// the id is the original's and the body the new one. The original is in the dump
		// on its own, already corrected.
		return protocol.InboundMessage{}, false
	case event.Info.Sender.IsBot(),
		bodyless(message),
		message.GetProtocolMessage() != nil,
		message.GetReactionMessage() != nil,
		message.GetEncReactionMessage() != nil,
		marksAMessage(message):
		return protocol.InboundMessage{}, false
	}
	inbound, _, ok := s.inboundOf(event, r.pastBody)
	return inbound, ok
}

// pastBody renders the body of a message from a dump.
//
// The same as a live one except for the file, which is never fetched here: a dump can
// carry thousands of pictures, and downloading each before publishing its slice would hold
// the whole dump behind the slowest of them. The message goes out with no reference, and
// what it takes to fetch the file later is recorded, so `message.download_media` can.
func (r *dumpRun) pastBody(event *waEvents.Message) (body, bool) {
	s := r.s
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
		// Checked right before the write: the fence is about the lease and not the
		// account, and a row written after a logout would hand the old account's file to
		// whatever pairs next under this session.
		if r.replaced() || !s.rememberPast(r.ctx, event, &part) {
			r.unkept = true
		}
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

	// Under the ceiling every send has, because this is one: whatsmeow's retry after a
	// reconnect has no deadline of its own, and a command that brought none would hold
	// the session's queue for as long as the socket stayed down.
	wire, giveUp := context.WithTimeoutCause(ctx, s.wireLimit, errSendCeiling)
	defer giveUp()
	client := s.current()
	request := client.BuildHistorySyncRequest(&waTypes.MessageInfo{
		MessageSource: waTypes.MessageSource{Chat: chat, IsFromMe: ask.Before.FromMe},
		ID:            ask.Before.ID,
		Timestamp:     time.UnixMilli(ask.Before.Timestamp),
	}, count)
	if err := s.sendPeer(wire, client, request); err != nil {
		return nil, sendFailure(whichClockRanOut(wire, err))
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
