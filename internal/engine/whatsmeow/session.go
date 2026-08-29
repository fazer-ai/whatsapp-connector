package whatsmeow

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	qrcode "github.com/skip2/go-qrcode"
	wm "go.mau.fi/whatsmeow"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/media"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/store"
)

// inboxDepth is how many emissions may wait for the pump. WhatsApp's own event
// handlers run on the socket's goroutine, so an emission that blocked there would stop
// the socket reading; the buffer is what keeps a slow publisher off the wire.
const inboxDepth = 256

// qrSize is the pixel width of the pairing image. Large enough to scan off a screen
// at the size a dashboard renders it, small enough to travel inside an event.
const qrSize = 512

// clientDisplayName is what code pairing tells the server this client is. WhatsApp
// validates the `Browser (OS)` shape against a list of the ones it knows and answers
// 400 for anything else, so it cannot carry the operator's device name: what shows in
// the account's linked-devices list comes from the device properties, not from here.
const clientDisplayName = "Chrome (Linux)"

// Session is one WhatsApp account on a whatsmeow client.
type Session struct {
	sid   string
	store *store.Container
	log   zerolog.Logger
	waLog waLog.Logger

	// Producers send here and never close it; the forwarder owns `events` and is the
	// only thing that closes it. Two channels rather than one because whatsmeow's
	// handlers can still be running when Close is called, and a send on a closed
	// channel is a panic in a library goroutine we do not own.
	inbox  chan pending
	events chan engine.Emission
	done   chan struct{}

	// ctx is the session's own lifetime, cancelled by Close. Every socket the client
	// dials is dialled under it, so a Close during a handshake tears the dial down
	// instead of waiting behind it: whatsmeow's own Connect runs on a background
	// context, and a stale owner blocked in a dial is one that outlives its lease.
	ctx    context.Context
	cancel context.CancelFunc

	mu        sync.Mutex
	client    *wm.Client
	handlerID uint32

	// detach removes the event handler from a client. It is a field only so the
	// teardown order can be held to: whatsmeow runs a handler under a lock that
	// RemoveEventHandler also takes, and nothing outside the library can make it
	// dispatch, so a test cannot otherwise tell a Close that releases the handler first
	// from one that waits on it forever.
	detach func(*wm.Client, uint32)

	// disconnect closes the socket. A field for the same reason as detach: whatsmeow
	// holds its socket lock for the length of a dial and Disconnect waits for it, and
	// nothing outside the library can put it in that state, so a test cannot otherwise
	// reach the path where a disconnect outlives its deadline.
	disconnect func(*wm.Client)

	// logout ends the session on WhatsApp's side. A field for the same reason as the two
	// above: whatsmeow refuses to log out a client that never connected, so nothing
	// outside the library can reach the path where WhatsApp has revoked the device and
	// the local cleanup is what fails.
	logout func(context.Context, *wm.Client) error

	// storeLimit bounds the store work an event handler does before it can publish what
	// the event was. A field only so a test can make a store that stalls take less than
	// the real bound; nothing else changes it.
	storeLimit time.Duration

	// deliverWait bounds how long an inbound message waits to hear that its event was
	// published. A field for the same reason as storeLimit, and for no other.
	deliverWait time.Duration

	// stalledUntil is when a receipt is worth handing to the publisher again, in
	// monotonic nanoseconds, and zero while it is.
	//
	// It exists because one receipt node is not one event: whatsmeow expands a grouped
	// receipt into a dispatch per participant and carries on through the ones that fail,
	// so a group of six read by six people is six calls into the handler. Each waiting
	// out deliverWait puts the node past the five minutes whatsmeow gives a handler
	// before it starts the next one alongside it, and two node handlers running at once
	// is the ordering guarantee gone. Waiting once per window and refusing the rest
	// costs a redelivery, which is the trade the whole path is built on.
	stalledUntil atomic.Int64

	// downloadWait bounds how long an inbound media message spends fetching its file.
	// A field for the same reason as the two above it, and for no other.
	downloadWait time.Duration

	// download fetches the bytes of a media message. A field for the same reason as
	// detach and the two below it: nothing outside whatsmeow can make a real client
	// answer a download, so a test cannot otherwise reach either side of the split
	// between a failure worth retrying and one that is permanent.
	download func(context.Context, *wm.Client, wm.DownloadableMessage) ([]byte, error)

	// uploadWait bounds how long an outbound media message spends fetching its file and
	// handing it to WhatsApp. A field for the same reason as the three above it.
	uploadWait time.Duration

	// retrieve fetches the bytes of an outbound file from the address the caller named,
	// and uploadFile hands them to WhatsApp. Fields for the same reason as download:
	// nothing outside an HTTP server and a real socket can make either of them fail the
	// way this has to answer for, so a test cannot otherwise reach the split between a
	// caller's bad address and a server having a bad minute.
	retrieve   func(ctx context.Context, address string, headers map[string]string) (source, error)
	uploadFile func(context.Context, *wm.Client, wm.MediaType, io.Reader) (wm.UploadResponse, error)

	// deliver hands a built message to WhatsApp. A field for the same reason again, and
	// for one more: what a command decides to put on the wire -- the body, the key that
	// names another message, the stanza id a retry has to arrive under -- is only
	// observable here. A test without this seam can check the pieces and not that the
	// command uses them, which is the difference between covering a rule and covering
	// the function that was supposed to apply it.
	handOver func(context.Context, waTypes.JID, string, *waE2E.Message) (wm.SendResponse, error)

	// unseal opens a change WhatsApp sealed under the secret of the message it is
	// about. A field for the same reason as the ones above: the key lives in the
	// device store the socket writes, so nothing that does not have one can reach
	// either side of what this has to answer for -- a secret that was never stored and
	// a socket that is not up yet are not the same answer, and only one of them is
	// worth another delivery.
	unseal func(context.Context, *waEvents.Message) (*waE2E.Message, error)

	// handoffWait bounds how long a moment waits on a reader that is busy. A field for
	// the same reason as deliverWait, and for no other.
	handoffWait time.Duration

	// board holds the newest presence per chat that has not been published yet. The
	// inbox holds a marker for each one, saying when its turn is.
	//
	// Value and order want opposite shapes, so they are kept apart. What matters about
	// presence is the last thing somebody did: a FIFO preserves the first, so behind a
	// backlog a queued `composing` outlives the `paused` dropped for want of room, and
	// there is no event after a stop -- the client is left showing somebody typing with
	// nothing coming. Keyed by chat, the stop replaces the typing it stops. But the
	// order still has to hold against everything else the session publishes, because a
	// `composing` that overtakes the message ending it leaves the same indicator stuck,
	// and that is what the marker is for: it sits in the one queue with the messages,
	// and the value it stands for is read when the forwarder reaches it. Nothing older
	// is published after something newer, and no value waits in a queue long enough to
	// go stale in it.
	board    map[string]posted
	boardMu  sync.Mutex
	boardSeq int64
	// transitions counts the writes to `connected`, which is what a presence that failed
	// to publish is checked against before it is given another go. Everything presence
	// describes belongs to the socket that reported it -- WhatsApp forgets subscriptions
	// and availability when a connection goes, and whatsmeow replays neither -- so a
	// state re-asserted across one of these is a fact nothing warrants any more, put back
	// on top of a client that cleared presence when it saw the session go.
	//
	// The connection flag and not the events, because they are not the same set: an
	// ordinary drop publishes `session.state`, while a stream replaced, a ban, an
	// outdated client and a connect failure each publish an event of their own and no
	// state. Counting event types would have to name all six and would miss the seventh
	// somebody adds. `setConnected` and `offline` are the two functions that own the
	// flag, and every one of those paths goes through one of them.
	transitions atomic.Int64

	// awaited holds the messages that arrived unreadable and have not been given up on
	// yet, so the one that arrives afterwards under the same id can call the placeholder
	// off. Keyed by message id, and emptied by whichever of the two happens first.
	//
	// It exists because a client deduplicates on that id: a placeholder published before
	// the real message is a placeholder for good, and both of the ways an unreadable
	// message is recovered -- the sender re-encrypting, the phone forwarding -- deliver
	// the real one under the id the placeholder already took.
	awaited   map[string]*awaiting
	awaitedMu sync.Mutex

	// fence stands in front of every write the device makes, and is dropped when this
	// session stops. Cancelling s.ctx already refuses the writes that go through a node
	// handler; this covers the ones whatsmeow deliberately detaches from that context.
	fence *store.Fence

	// rerequestWait is how long a message that could not be read is left to arrive before
	// its placeholder goes out. A field so a test does not have to wait it out.
	rerequestWait time.Duration

	// rerequestRetry is how long that placeholder waits before offering itself to the
	// publisher again. A field for the same reason.
	rerequestRetry time.Duration

	// picked, when it is set, receives once for every emission the forwarder takes off
	// the inbox, before it tries to hand it on. A seam so a test can know the pump is
	// parked rather than sleeping until it probably is.
	picked chan struct{}

	// elapsed is the monotonic reading the publisher window is measured with, and nil is
	// the real one. A seam so a test can hold the clock still instead of racing it.
	elapsed func() time.Duration

	// wallClock is where a published event's moment comes from, and nil is the real one.
	// A seam for the same reason as the one above: a test that has to see two events of
	// one node carry one moment cannot get there by racing a clock whose two readings are
	// usually the same millisecond anyway.
	wallClock func() time.Time

	// privacyKnown reports whether this account's own privacy settings could be read.
	// A seam for the same reason as the ones below it: nil is the real one.
	privacyKnown func(context.Context) error

	// groupMode is how a group addresses its members, which decides the namespace a
	// message key in it names a sender by. A field because reading it is a round trip to
	// WhatsApp, and a test cannot otherwise reach either branch of what depends on it.
	groupMode func(context.Context, waTypes.JID) (waTypes.AddressingMode, error)

	// sendLimit is the largest file this session will send. Not the blob cap: an
	// instance with nowhere to keep an inbound file still sends one.
	sendLimit int64

	// blobs is where the file of an inbound message is kept, and blobBase the address a
	// client fetches it from. Nil when the deployment gave this instance nowhere to
	// write: media messages are then published with no file to fetch rather than
	// filling a directory nobody asked for.
	blobs    Blobs
	blobBase string

	// groups is the last connect's `groups`: whether the client wants group chats
	// alongside direct ones. Guarded by mu, written by Connect and read by every
	// inbound message.
	groups bool

	// transition serialises a change to the socket's state with the event announcing
	// it. It is not mu: emit can block on a full inbox, and holding the session's own
	// lock across that would stop everything that reads state, Close included.
	transition sync.Mutex
	closed     bool
	// dialing is true while a connect is in flight. whatsmeow holds its socket lock for
	// the length of one, and every question asked of the client takes that lock for
	// read, so a dial that outlived its command would block the next command on a
	// socket nobody is waiting for. This is the answer to "is it connecting" that costs
	// no lock.
	dialing bool
	// hungUp is an explicit disconnect this session performed and has not been asked to
	// undo. whatsmeow's own reconnect can already be past its wait when that lands, and
	// it then opens a socket nobody asked for, after the command has answered `close`.
	hungUp bool
	// reconnecting is whatsmeow retrying a paired socket on its own, which runs outside
	// this session's dial. Without it a status would report `close` while the event
	// stream says reconnecting, and a resume would start a second dial alongside it.
	reconnecting bool
	// connected is what the socket is actually doing, kept from the events that report
	// it. whatsmeow's own IsLoggedIn is set on authentication and cleared only by a
	// stream error, so it stays true through a Disconnect and cannot answer this.
	connected bool
	// stale is a client whose device whatsmeow deleted and whose replacement could not
	// be built. Nothing works on it, so the next connect tries the rebuild again rather
	// than talking to it.
	stale bool
	// phone and lid are this session's copy of what it paired. whatsmeow assigns the
	// same fields on its pairing goroutine, so reading them off the client from a
	// command is a race; this is written from the event handler and read under the
	// lock.
	phone string
	lid   string
	// pairing is the conversation currently open, if any. It is a pointer rather than a
	// bare cancel func so a finished run can clear itself without clearing the one that
	// replaced it: functions are not comparable, and "is this still mine" is the whole
	// question.
	pairing *pairingRun
	// hangingUp is a disconnect this session started and has not seen finish. whatsmeow
	// holds its socket lock for the length of a dial and Disconnect waits for the same
	// lock, so a disconnect can outlive the command that asked for it.
	hangingUp chan struct{}
	// runs counts the conversations this session has started. Paired with nonce it is
	// what gives each one a name the client can answer to.
	runs uint64
	// pairingMu serialises retiring a conversation and putting its socket back with
	// starting the next one. Retirement clears s.pairing and only then publishes and
	// disconnects, and a replacement installed in between is one the attempt that just
	// ended disconnects the socket of: the operator's corrected attempt fails for reasons
	// belonging to the one before it.
	pairingMu sync.Mutex
	// nonce is this session's own, drawn once when it is built. A count alone starts
	// over whenever a session is rebuilt — a restart, a lease moving — so the first
	// conversation of the new one would answer to the name the last one used, and an
	// answer still in flight from before would be sent to WhatsApp against a challenge
	// it was never meant for.
	nonce string
	// closing is what the engine wants told when this session ends, so a session that
	// is over stops being something the engine hands out or holds on to.
	closing func()
}

// pairingRun is one pairing conversation.
type pairingRun struct {
	// id names this conversation on the wire. The passkey exchange is the one part of
	// pairing where the client answers back, and an answer meant for an attempt the
	// operator has already replaced would be sent to WhatsApp as if it were this one's.
	id     string
	cancel context.CancelFunc
	// done is closed when this conversation is cancelled. whatsmeow only watches the
	// pairing context from the goroutine that emits codes, and that goroutine is started
	// by the first QR event: a conversation whose dial failed before one arrived leaves
	// its output channel open for good, so the reader needs something else to wake on.
	done <-chan struct{}
}

//nolint:gocritic // zerolog.Logger is designed to be copied; every With() returns one by value
func newSession(
	sid string, client *wm.Client, container *store.Container, fence *store.Fence,
	blobs MediaOptions, log zerolog.Logger, wa waLog.Logger,
) *Session {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Session{
		sid:        sid,
		store:      container,
		fence:      fence,
		log:        log.With().Str("sid", sid).Logger(),
		waLog:      wa,
		inbox:      make(chan pending, inboxDepth),
		events:     make(chan engine.Emission),
		done:       make(chan struct{}),
		ctx:        ctx,
		cancel:     cancel,
		detach:     func(client *wm.Client, id uint32) { client.RemoveEventHandler(id) },
		disconnect: func(client *wm.Client) { client.Disconnect() },
		nonce:      sessionNonce(),
		logout:     func(ctx context.Context, client *wm.Client) error { return client.Logout(ctx) },
		download: func(ctx context.Context, client *wm.Client, part wm.DownloadableMessage) ([]byte, error) {
			return client.Download(ctx, part) //nolint:wrapcheck // classified by downloadFailure, which needs the sentinels
		},
		retrieve:   retrieveOverHTTP,
		uploadFile: uploadOverClient,
		sendLimit:  cmp.Or(blobs.SendMax, media.DefaultSendMax),
		blobs:      blobs.Blobs,
		blobBase:   blobs.BaseURL,

		storeLimit:  bindTimeout,
		deliverWait: deliverTimeout,
		handoffWait: perishableHandoff,
		awaited:     make(map[string]*awaiting),

		rerequestWait:  rerequestTimeout,
		rerequestRetry: rerequestRetry,
		board:          make(map[string]posted),
		downloadWait:   downloadTimeout,
		uploadWait:     uploadTimeout,
	}
	s.adopt(client)
	go s.forward()
	return s
}

// onClose registers what to run once this session has ended. The engine uses it to drop
// the session from its cache; nothing else does, and it is set before the session is
// handed out, so it never changes under a Close.
func (s *Session) onClose(fn func()) {
	s.mu.Lock()
	s.closing = fn
	s.mu.Unlock()
}

// sessionNonce is what makes one session's pairing names its own.
//
// The clock is the fallback rather than the answer: two sessions built in the same
// nanosecond would share a name, and a rebuild after a restart is exactly when that is
// least unlikely.
func sessionNonce() string {
	raw := make([]byte, 8)
	if _, err := rand.Read(raw); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(raw)
}

// identityOf reads what a client was built knowing. Safe before the client is running,
// which is the only time this is called: once it is, whatsmeow assigns the same fields
// from its pairing goroutine.
func identityOf(client *wm.Client) (phone, lid string) {
	if id := client.Store.ID; id != nil {
		phone = id.User
	}
	if stored := client.Store.LID; !stored.IsEmpty() {
		lid = stored.User
	}
	return phone, lid
}

// adopt takes a client over: it wires the callbacks, subscribes to its events, and
// copies out the identity it was built with. Nothing else assigns s.client, so the
// logout path can hand over a replacement without a second code path.
// It reports false when the session closed while this was happening, in which case the
// client it was handed is disconnected and nothing is kept: a handler on a closed
// session is one nothing will ever remove.
func (s *Session) adopt(client *wm.Client) bool {
	// WhatsApp itself demands a reconnect in the middle of pairing: the server closes
	// the stream with a 515 and expects the client back. Turning whatsmeow's reconnect
	// off would leave every pairing hanging one step from done, so the socket's own
	// recovery stays with the library. Ownership does not: the layer above closes this
	// session the moment the lease is gone, which is what keeps two instances off one
	// account.
	client.EnableAutoReconnect = true
	client.PrePairCallback = s.bind
	client.BackgroundEventCtx = s.ctx
	// The ack for an inbound message waits for the handlers, and a handler that reports
	// failure stops it being sent at all. That pairing is what lets this build refuse a
	// message it cannot publish instead of telling WhatsApp it was delivered: the
	// invariant is that losing an event costs a redelivery, never a message.
	client.SynchronousAck = true
	// The history dump has an acknowledgement of its own that the handler gate does not
	// cover: whatsmeow downloads it and receipts it on its own. Both are turned off
	// together, because receipting a dump nobody published is the same loss as
	// acknowledging a message nobody published, and M6 is where the dump gets somewhere
	// to go.
	client.ManualHistorySyncDownload = true
	client.DisableManualHistorySyncReceipt = true
	// Refusing the ack only keeps a message if the redelivery can still be read.
	// Decrypting advances the Signal ratchet and that advance is persisted, so the
	// second copy of the same ciphertext fails with an old-counter error, is skipped
	// without a dispatch, and is acknowledged anyway: the loss moves to the redelivery
	// instead of being prevented. The buffer keeps the plaintext, keyed by the
	// ciphertext, until a handler accepts it.
	client.EnableDecryptedEventBuffer = true

	phone, lid := identityOf(client)

	// Subscribed before the swap, so the client is never live with nobody listening,
	// and both halves are one lifecycle step: a Close that lands between them would
	// otherwise leave a handler on a client the session no longer knows about.
	handlerID := client.AddEventHandlerWithSuccessStatus(s.handle)

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		client.RemoveEventHandler(handlerID)
		client.Disconnect()
		return false
	}
	s.client = client
	s.handlerID = handlerID
	s.phone = phone
	s.lid = lid
	s.stale = false
	s.connected = false
	s.mu.Unlock()
	return true
}

// current is the client this session is on right now.
func (s *Session) current() *wm.Client {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.client
}

func (s *Session) identity() (phone, lid string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.phone, s.lid
}

func (s *Session) setDialing(dialing bool) {
	s.mu.Lock()
	s.dialing = dialing
	s.mu.Unlock()
}

func (s *Session) setConnected(connected bool) {
	s.mu.Lock()
	s.transitions.Add(1)
	s.connected = connected
	// Either way the dial is over: whatsmeow has answered for it, with an
	// authenticated session or with the socket going down again.
	s.dialing = false
	if connected {
		s.reconnecting = false
	}
	s.mu.Unlock()
}

func (s *Session) setReconnecting(reconnecting bool) {
	s.mu.Lock()
	s.reconnecting = reconnecting
	s.mu.Unlock()
}

// offline is a connection that is down and not coming back on its own. Every terminal
// outcome goes through it, because leaving the retry flag up after whatsmeow has given
// up reports a session as reconnecting forever.
// undoHangUp reports whether a connection arrived while an explicit disconnect still
// stands, clearing the mark either way: one uninvited socket is answered once.
func (s *Session) undoHangUp() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	standing := s.hungUp
	s.hungUp = false
	return standing
}

func (s *Session) offline() {
	s.mu.Lock()
	s.transitions.Add(1)
	s.connected = false
	s.reconnecting = false
	s.dialing = false
	s.mu.Unlock()
}

// connection is the count of connection writes and whether the session is on one right
// now, read together so the two cannot disagree about the same moment. Presence is the
// only caller and needs both: a node from a socket that is already down describes nothing
// current, and a node from the live one has to remember which connection that was.
func (s *Session) connection() (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.transitions.Load(), s.connected
}

// learned is the moment an event says the session found out about the thing it reports,
// which is what the frame's `ts` carries.
func (s *Session) learned() int64 {
	if s.wallClock != nil {
		return s.wallClock().UnixMilli()
	}
	return time.Now().UnixMilli()
}

func (s *Session) setGroups(groups bool) {
	s.mu.Lock()
	s.groups = groups
	s.mu.Unlock()
}

func (s *Session) wantsGroups() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.groups
}

func (s *Session) setIdentity(phone, lid string) {
	s.mu.Lock()
	s.phone = phone
	s.lid = lid
	s.mu.Unlock()
}

// Events is the emission channel, closed once the session is done.
func (s *Session) Events() <-chan engine.Emission { return s.events }

// Connect starts pairing or resumes a stored session.
func (s *Session) Connect(ctx context.Context, req engine.ConnectRequest) error {
	if s.isClosed() {
		return errors.New("whatsmeow: the session is closed")
	}

	if req.Proxy != nil && req.Proxy.URL != "" {
		// Decoding it is not honouring it. Connecting directly for a deployment that
		// asked for egress routing puts its own address on the wire, and does it
		// silently; per-session proxies are M5.
		return protocol.NewError(protocol.ErrorUnsupported,
			"this connector does not route a session through a proxy yet")
	}
	// Same rule as the proxy, and for the same reason: these ask the connector to do
	// something, and a build that does not do it answers `open` to a client that will
	// then wait for a call to be refused, or for a backlog to arrive, and never find out
	// it was never going to happen. `groups` is not on this list because it is honoured
	// now that there is conversation traffic to leave out.
	if req.Calls != nil && req.Calls.AutoReject {
		return protocol.NewError(protocol.ErrorUnsupported,
			"this connector does not answer incoming calls yet")
	}
	if req.HistorySync {
		return protocol.NewError(protocol.ErrorUnsupported,
			"this connector does not import the phone's history yet")
	}
	if s.isStale() {
		// The device behind this client was deleted and its replacement could not be
		// built at the time. Nothing on it works, so the connect that would have failed
		// is the connect that repairs it.
		if err := s.recover(ctx); err != nil {
			return fmt.Errorf("whatsmeow: %s is still without a usable device: %w", s.sid, err)
		}
	}

	if req.Pairing != "resume" && req.Pairing != "qr" && req.Pairing != "code" {
		return protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("%q is not a pairing mode this connector knows", req.Pairing))
	}
	if req.Pairing == "code" && digitsOf(req.Phone) == "" {
		// Checked here rather than only where it is used, because everything below this
		// point changes the session, and a refusal that has already changed it is a
		// command that failed and took effect.
		return protocol.NewError(protocol.ErrorInvalidPayload, "code pairing needs the phone number to pair")
	}

	// Recorded once the request is one the session is going to act on, because the
	// client sends it on every connect and it is a property of the subscription rather
	// than of the device. A refused connect leaves the subscription alone: the caller
	// sees a failed command, and a session that had quietly turned group traffic off
	// underneath it would go on acknowledging and dropping group messages until the
	// next connect that happened to succeed.
	s.setGroups(req.Groups)

	// Waited for before the guard comes down, and before anything is dialled. A
	// disconnect that outlived its command is still going to close the socket, and a
	// connect answered `open` in between is one the older command then closes underneath:
	// the two take effect in the opposite order to the one they were sent in, which is
	// the whole thing a session's own queue exists to prevent.
	if err := s.awaitHangUp(ctx); err != nil {
		return err
	}

	// Dropped here and not on the way in. hungUp is what rejects a Connected event the
	// library had already queued when a manual disconnect completed, and a manual
	// disconnect is followed by no Disconnected event to undo it. A request refused
	// above would have dropped the guard and left that stale event free to report a
	// session that is down as open, with nothing arriving later to correct it.
	//
	// The state comes back with it because the two are one decision. Asked separately,
	// the queued Connected lands in between: the guard is down, so it is announced rather
	// than refused, and the resume below is then told the session is already open — over
	// a socket that is down for good, and with nothing arriving later to say so.
	standing := s.dropHangUp()

	var err error
	switch req.Pairing {
	case "resume":
		err = s.resume(ctx, standing)
	case "qr":
		err = s.pairWithQR(ctx, standing)
	default:
		err = s.pairWithCode(ctx, req.Phone, standing)
	}
	if err != nil && s.state() == "close" {
		// Refused before anything was opened: a resume on a session that never paired, a
		// code pairing with no number, a pairing channel that could not be opened. The
		// guard that was standing before this request has to go back up, or a Connected
		// the library had queued before the manual disconnect reports a socket that is
		// down as open, with nothing arriving later to correct it.
		//
		// Only when nothing started. A dial still in flight reads as connecting, and its
		// own failure raises the guard where it belongs.
		s.transition.Lock()
		s.refuseLateConnect()
		s.transition.Unlock()
	}
	return err
}

// awaitHangUp waits for a disconnect this session started and has not seen finish.
//
// The command that asked for it was answered with a failure when it ran out of time, and
// the socket goes down a moment later regardless. Whoever comes next has to wait for that
// to land rather than read the socket that is still up as one it may keep.
func (s *Session) awaitHangUp(ctx context.Context) error {
	s.mu.Lock()
	pending := s.hangingUp
	s.mu.Unlock()
	if pending == nil {
		return nil
	}
	select {
	case <-pending:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("whatsmeow: %s is still disconnecting: %w", s.sid, ctx.Err())
	case <-s.done:
		return errors.New("whatsmeow: the session closed while a disconnect was finishing")
	}
}

// resume reconnects a session that has already paired. Asking to resume one that has
// not is a client bug rather than a reason to silently start a pairing the operator
// is not watching for.
func (s *Session) resume(ctx context.Context, state string) error {
	if phone, _ := s.identity(); phone == "" {
		return protocol.NewError(protocol.ErrorNotPaired,
			"this session has not paired, so there is nothing to resume")
	}
	if state == "open" || state == "connecting" || state == "reconnecting" {
		// whatsmeow is already on it. Dialling alongside its retry loses the race about
		// half the time and answers the caller with `ErrAlreadyConnected` for a socket
		// that was recovering perfectly well.
		return nil
	}

	// Published before the dial, not after: whatsmeow can report the connection from
	// its own goroutine while ConnectContext is still returning, and a `connecting`
	// queued behind that `open` leaves the client believing the session never finished
	// connecting.
	client := s.current()
	s.emit(protocol.EventSessionState, map[string]any{"state": "connecting"})
	reportFailure := func(error) {
		s.emit(protocol.EventSessionState, map[string]any{"state": "close", "reason": "connect_failed"})
	}
	if err := s.dial(ctx, client, reportFailure); err != nil {
		// Only when the dial itself failed. A caller that stopped waiting leaves the
		// connect running, and a `close` published over it is a terminal state the very
		// next event contradicts.
		if !errors.Is(err, ctx.Err()) || ctx.Err() == nil {
			s.emit(protocol.EventSessionState, map[string]any{"state": "close", "reason": "connect_failed"})
		}
		return fmt.Errorf("whatsmeow: resume %s: %w", s.sid, err)
	}
	return nil
}

// dial connects, and stops waiting when the command that asked for it does.
//
// The socket belongs to the session and not to the command, so a deadline that passes
// mid-handshake answers the caller and leaves the dial running: the alternative, handing
// whatsmeow a context that dies with the RPC, would also kill the reconnect loop it
// starts from the same one. What the deadline must not do is hold the session's command
// queue, which is single-file, behind a network round trip nobody is waiting for.
func (s *Session) dial(ctx context.Context, client *wm.Client, onDetached func(error)) error {
	s.setDialing(true)
	dialed := make(chan error, 1)
	go func() {
		err := client.ConnectContext(s.ctx)
		if err != nil {
			s.setDialing(false)
		}
		// On success the flag stands until whatsmeow says the session is authenticated.
		// ConnectContext returns once the socket is up, and the handshake that follows is
		// asynchronous: clearing it here leaves a window where nothing is dialing, nothing
		// is connected, and `session.status` answers `close` — which is the reply
		// `session.connect` carries back for a resume that is going perfectly well.
		dialed <- err
	}()

	select {
	case err := <-dialed:
		return err
	case <-ctx.Done():
		// The caller is answered and the dial carries on, so somebody still has to say
		// how it ended. Without this the client sits on the `connecting` published just
		// before it, for as long as the session lasts: nothing else reports a connect
		// that failed after its command gave up.
		go s.awaitDetachedDial(dialed, onDetached)
		return fmt.Errorf("whatsmeow: %s was still connecting: %w", s.sid, ctx.Err())
	case <-s.done:
		return errors.New("whatsmeow: the session closed while it was connecting")
	}
}

// awaitDetachedDial reports a dial that failed after the command that asked for it had
// already been answered. A successful one needs nobody: whatsmeow announces it as a
// Connected event, which the handler turns into the state the client is waiting for.
func (s *Session) awaitDetachedDial(dialed <-chan error, onDetached func(error)) {
	var err error
	select {
	case err = <-dialed:
	case <-s.done:
		return
	}
	if err == nil || onDetached == nil {
		return
	}
	if s.ctx.Err() != nil {
		// The dial was interrupted by the session ending, which is not a failure a
		// client acts on and not a state anything will contradict.
		return
	}
	onDetached(err)
}

// pairWithQR connects and publishes the codes WhatsApp issues until one is scanned.
//
// The channel has to be taken before connecting: it is how whatsmeow reports the
// outcome of the pairing, and asking for it afterwards misses the first code.
//
// The conversation itself outlives the command that started it, running on the
// session's lifetime until a code is scanned, the codes run out, or Close ends it. Only
// the wait for the socket honours the command's deadline.
// `standing` is the state as it stood when the guard came off, which is what a resume
// from here has to decide on: see dropHangUp.
func (s *Session) pairWithQR(ctx context.Context, standing string) error {
	if phone, _ := s.identity(); phone != "" {
		// Already paired. A client asking for a QR code here means the operator hit
		// connect on an inbox that is simply disconnected, and resuming is what they
		// meant.
		return s.resume(ctx, standing)
	}

	client := s.current()
	pairCtx, cancel := context.WithCancel(s.ctx)
	codes, err := client.GetQRChannel(pairCtx)
	if err != nil {
		cancel()
		return fmt.Errorf("whatsmeow: open the pairing channel of %s: %w", s.sid, err)
	}
	run := s.startPairing(pairCtx, cancel)

	go s.readPairing(run, codes)

	s.emit(protocol.EventSessionState, map[string]any{"state": "connecting"})
	abandon := func(err error) { s.abandonPairing(run, client, "connect_failed", err) }
	if err := s.dial(ctx, client, abandon); err != nil {
		s.giveUpOn(ctx, run, client, err)
		return fmt.Errorf("whatsmeow: connect %s: %w", s.sid, err)
	}
	return nil
}

// giveUpOn tears a pairing attempt down for a dial that failed, and leaves it alone for
// one the caller merely stopped waiting for.
//
// The distinction matters twice over: the conversation is documented to outlive the
// command, and tearing it down calls Disconnect, which would wait on the socket lock
// the still-running dial is holding — putting the executor right back behind the
// deadline it just escaped.
func (s *Session) giveUpOn(ctx context.Context, run *pairingRun, client *wm.Client, err error) {
	if errors.Is(err, ctx.Err()) && ctx.Err() != nil {
		// The dial is still running and reports its own outcome when it detaches.
		return
	}
	// Reported, not just torn down. The `connecting` this attempt published is still
	// standing, and nothing else will take it down: a dial that never reached WhatsApp
	// produces no whatsmeow event, so a failure that only answered its caller leaves
	// every client watching the stream connecting for good.
	go s.abandonPairing(run, client, "connect_failed", err)
}

// hangUp closes the socket without holding the session's command queue behind a dial.
//
// whatsmeow keeps its socket lock for the length of a connect, and Disconnect waits for
// that same lock, so a disconnect arriving mid-handshake would otherwise sit there long
// past its deadline and then report success. Cancelling the session context is what
// actually interrupts the dial; this is the part that stops waiting.
func (s *Session) hangUp(ctx context.Context, client *wm.Client) error {
	done := make(chan struct{})
	s.mu.Lock()
	s.hangingUp = done
	s.mu.Unlock()
	go func() {
		defer close(done)
		s.disconnect(client)
		// Settled here rather than by the caller, because a disconnect that outlives its
		// deadline still happens. The caller is told it failed and stops; the socket goes
		// down a moment later regardless, and a session that never recorded it would go
		// on reporting itself open over a connection that no longer exists, with no close
		// event to correct it.
		s.settleHangUp()
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		// The socket is still up, held by a dial this had to queue behind. Answering the
		// caller with success would have it record a session as closed while events from
		// that connection are still on their way.
		return fmt.Errorf("whatsmeow: %s was still connecting: %w", s.sid, ctx.Err())
	case <-s.done:
		return nil
	}
}

// settleHangUp records a socket this session took down on purpose.
//
// Held under transition like every other socket transition, and for the same reason.
// whatsmeow can have queued a Connected before the disconnect landed: without this, that
// handler reads hungUp before this sets it, publishes `close` here, and then sets the
// state to connected and publishes `open` on top of it. A socket that is down would then
// be reported open for good, with nothing arriving later to correct it.
func (s *Session) settleHangUp() {
	s.transition.Lock()
	defer s.transition.Unlock()

	s.mu.Lock()
	if s.closed {
		// The session is being torn down; its own Close publishes nothing and neither
		// should this.
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	s.refuseLateConnect()
	s.offline()
	s.emit(protocol.EventSessionState, map[string]any{"state": "close", "reason": "disconnect_requested"})
}

// abandonPairing gives up a pairing conversation and puts the socket back where a
// corrected connect can start a new one. Without the disconnect the next GetQRChannel
// refuses, because whatsmeow will not open a second pairing channel on a live socket,
// and the operator is stuck until the codes run out.
func (s *Session) abandonPairing(run *pairingRun, client *wm.Client, reason string, err error) {
	s.pairingMu.Lock()
	defer s.pairingMu.Unlock()

	// Only if this run is still the current one. These calls can be detached from the
	// command that started them, and the operator's corrected attempt may already own
	// the socket: tearing that one down would be this attempt failing the next one.
	if !s.endPairing(run) {
		return
	}
	// Published from inside that gate, because the reader of the pairing channel takes
	// the same one for the outcomes WhatsApp reports. Outside it, an attempt that ends
	// here while its reader is reporting a timeout publishes both.
	if reason != "" {
		s.publishPairingFailure(reason, err)
	}
	s.tearDownPairing(run, client)
}

// tearDownPairing puts the socket back where a corrected connect can start a new
// conversation on it. The caller has already retired the run.
func (s *Session) tearDownPairing(run *pairingRun, client *wm.Client) {
	run.cancel()
	client.Disconnect()

	// Cancelling the context is not enough to close whatsmeow's QR channel: it only
	// looks at that context from the loop that emits codes, which a pairing failing
	// before its first code never reaches. The subscription and its reader would then
	// outlive the attempt and join the next one, so the client is replaced instead.
	s.markStale()
}

// digitsOf keeps the digits of a phone number and drops the rest, which is what
// whatsmeow does to it before pairing.
func digitsOf(phone string) string {
	var digits strings.Builder
	for _, r := range phone {
		if r >= '0' && r <= '9' {
			digits.WriteRune(r)
		}
	}
	return digits.String()
}

// pairWithCode connects and asks WhatsApp for a code the operator types on the phone.
//
// whatsmeow needs the socket up before it can ask, and it reports readiness by putting
// the first QR code on the pairing channel. Waiting for that is what makes the request
// land on an established connection rather than a sleep that is usually long enough.
func (s *Session) pairWithCode(ctx context.Context, rawPhone, standing string) error {
	// whatsmeow strips everything that is not a digit before it asks, so a number typed
	// with a plus and spaces pairs perfectly well. The number this session then reports
	// has to be the one it actually paired: `pairing.code` promises digits, and a client
	// that validates the contract drops the event over the plus it sent us itself.
	phone := digitsOf(rawPhone)
	if phone == "" {
		return protocol.NewError(protocol.ErrorInvalidPayload, "code pairing needs the phone number to pair")
	}
	if paired, _ := s.identity(); paired != "" {
		return s.resume(ctx, standing)
	}

	client := s.current()
	pairCtx, cancel := context.WithCancel(s.ctx)
	codes, err := client.GetQRChannel(pairCtx)
	if err != nil {
		cancel()
		return fmt.Errorf("whatsmeow: open the pairing channel of %s: %w", s.sid, err)
	}
	run := s.startPairing(pairCtx, cancel)

	// The QR codes themselves are dropped: an operator who asked for a code is not
	// looking at an image, and publishing both would have the dashboard show two ways
	// to pair the same session.
	// Buffered and carrying the answer: readiness is a code having arrived, and a
	// channel that ended without one is a pairing that already failed. Calling PairPhone
	// on that socket adds a second, meaningless failure on top of the real one.
	ready := make(chan bool, 1)
	go func() {
		s.readPairingWith(run, codes, func(arrived bool) { ready <- arrived }, false)
	}()

	s.emit(protocol.EventSessionState, map[string]any{"state": "connecting"})
	// Nothing to report if this one detaches: the attempt is torn down below whatever
	// the dial goes on to do, and a code pairing cannot continue without its command.
	if err := s.dial(ctx, client, nil); err != nil {
		// Off the executor when the dial is still running: Disconnect waits on the lock
		// that dial is holding, and this attempt is over either way.
		go s.abandonPairing(run, client, "connect_failed", err)
		return fmt.Errorf("whatsmeow: connect %s: %w", s.sid, err)
	}

	select {
	case arrived := <-ready:
		if !arrived {
			return protocol.NewError(protocol.ErrorWaError,
				"WhatsApp ended the pairing before it offered a code")
		}
	case <-ctx.Done():
		// Unlike a QR conversation, this one cannot carry on without its command:
		// nothing else will ever call PairPhone. Left up, the socket refuses the
		// operator's next attempt at GetQRChannel until WhatsApp's own codes run out.
		go s.abandonPairing(run, client, "connect_failed", ctx.Err())
		return fmt.Errorf("whatsmeow: %s did not reach the server in time: %w", s.sid, ctx.Err())
	case <-s.done:
		return errors.New("whatsmeow: the session closed before it could ask for a code")
	}

	code, err := client.PairPhone(ctx, phone, true, wm.PairClientChrome, clientDisplayName)
	if err != nil {
		// A number WhatsApp refuses is the ordinary case here, and the operator is about
		// to type a corrected one. Leaving the pairing socket up would refuse that next
		// attempt too, for reasons that have nothing to do with the number. The reply
		// carries the reason; this is what takes the connection down with the attempt.
		s.abandonPairing(run, client, "code_refused", err)
		if coded := codeForPairPhone(err); coded != nil {
			return coded
		}
		return fmt.Errorf("whatsmeow: request a pairing code for %s: %w", s.sid, err)
	}
	s.emit(protocol.EventPairingCode, map[string]any{"code": code, "phone": phone})
	return nil
}

// Disconnect drops the socket and keeps the credentials.
func (s *Session) Disconnect(ctx context.Context) error {
	s.cancelPairing()
	return s.hangUp(ctx, s.current())
}

// Logout ends the session on WhatsApp's side and forgets the credentials here, so the
// next connect has to pair again.
func (s *Session) Logout(ctx context.Context) error {
	s.cancelPairing()
	if err := s.logout(ctx, s.current()); err != nil {
		if sentNothing(err) {
			// Nothing was sent, and the device is untouched on both sides. Clearing
			// credentials that still resume would cost the operator a fresh pairing for
			// a logout that visibly failed, while WhatsApp goes on listing the device
			// they asked to remove.
			//
			// The state is left exactly as it was, and that is the same point twice: the
			// commonest way to get here is a session whatsmeow is already reconnecting.
			// A guard raised now would have this session close the socket that comes
			// back, and calling it offline would have `session.status` answer `close`
			// for a reconnect that is going perfectly well — and a resume start a second
			// dial alongside it.
			return fmt.Errorf("whatsmeow: log %s out: %w", s.sid, err)
		}
		s.settleLogout()
		// The request went out. whatsmeow unlinks the device before it deletes the local
		// one and disconnects in between, so this failure can be the deletion alone:
		// WhatsApp has revoked the device and this session is holding credentials that
		// are gone. Marking it stale is what has the next connect clear them, and saying
		// so is what keeps a client from going on treating the account as paired. The
		// command still answers with the error it got.
		s.markStale()
		s.emit(protocol.EventSessionLoggedOut, map[string]any{"reason": "logout_requested"})
		return fmt.Errorf("whatsmeow: log %s out: %w", s.sid, err)
	}
	s.settleLogout()
	// Published as soon as WhatsApp has accepted it, ahead of every local step. From
	// here the device is revoked whatever this process manages to do next, and that is
	// the fact the client acts on: putting a database round trip in front of it means a
	// store that stopped answering leaves the operator looking at a session that was
	// logged out minutes ago, on credentials WhatsApp threw away.
	s.emit(protocol.EventSessionLoggedOut, map[string]any{"reason": "logout_requested"})

	if err := s.store.Forget(ctx, s.sid); err != nil {
		// The client here is on a deleted device whatever happens next, and the mapping
		// still points at it: rebuilding on top of that would hand the fresh client the
		// very credentials WhatsApp threw away.
		s.markStale()
		return err
	}
	if err := s.rebuild(ctx); err != nil {
		s.markStale()
		return err
	}
	return nil
}

// settleLogout records a socket the account no longer has, and refuses the connection
// whatsmeow may still have queued behind the unlink.
//
// Held under transition, like every other socket transition, and it takes the hang-up
// guard with it. Without both, a Connected that authentication produced before the
// unlink lands after this, sets the state back to connected and publishes `open` on top
// of `session.logged_out` — over credentials the account has revoked, and the rebuild
// that follows clears the flag without publishing anything that would correct the
// stream. The next connect clears the guard, which is where it is cleared for a manual
// disconnect too.
func (s *Session) settleLogout() {
	s.transition.Lock()
	defer s.transition.Unlock()

	s.refuseLateConnect()
	s.offline()
}

// dropHangUp takes the guard down and answers with the state as it stood at that moment.
//
// One step, under transition, because a caller that drops the guard and then asks
// separately can be answered by the very event the guard was there to refuse. Held
// across both, a Connected the library had queued for the old socket is either handled
// entirely before — refused, and the socket it came from closed — or entirely after,
// by which point this request has already decided to dial and the worst it costs is a
// second `open` behind the first.
func (s *Session) dropHangUp() string {
	s.transition.Lock()
	defer s.transition.Unlock()

	s.mu.Lock()
	s.hungUp = false
	s.mu.Unlock()
	return s.state()
}

// hangUpStanding reports whether the guard is up, without taking it down. The Connected
// handler is what consumes it, one uninvited socket answered once; everything else only
// wants to know that the socket it is hearing from is one this session has finished with.
func (s *Session) hangUpStanding() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hungUp
}

// refuseLateConnect raises the guard that has a Connected from a socket this session is
// done with closed instead of announced.
//
// whatsmeow dispatches from whichever goroutine produced the event, so authentication
// finishing can land after the thing that ended the connection. Without this the handler
// puts the state back to connected and publishes `open` on top of whatever terminal event
// just went out, over a socket nobody is on. The next connect clears it, which is where
// it is cleared for a manual disconnect too.
//
// The caller holds transition: this is one half of a socket transition, not one of its
// own.
func (s *Session) refuseLateConnect() {
	s.mu.Lock()
	s.hungUp = true
	s.mu.Unlock()
}

// sentNothing reports whether a logout failed before it reached WhatsApp, which is the
// case where the stored credentials are still exactly as good as they were.
func sentNothing(err error) bool {
	return errors.Is(err, wm.ErrNotConnected) ||
		errors.Is(err, wm.ErrNotLoggedIn) ||
		errors.Is(err, wm.ErrClientIsNil)
}

// rebuild puts the session on a fresh client.
//
// whatsmeow's Logout marks the device deleted rather than emptying it, and every later
// call on that client answers ErrDeviceDeleted. Without this the documented next step,
// pairing again, fails on a session the manager is still perfectly happy to run, and
// the only way out is to release and re-adopt it.
// rebuildWithin is rebuild on a bound, for the paths that have something to publish
// afterwards. The session's own lifetime is not a deadline: a database that stalls on the
// device lookup would hold this for as long as the process runs, and the event waiting
// behind it is the one telling the client WhatsApp revoked the account.
func (s *Session) rebuildWithin() error {
	ctx, cancel := context.WithTimeout(s.ctx, s.storeLimit)
	defer cancel()
	return s.rebuild(ctx)
}

func (s *Session) rebuild(ctx context.Context) error {
	if s.isClosed() {
		// Nothing left to pair with. A session that closed while this was on its way is
		// one the layer above has already given up.
		return nil
	}

	device, err := s.store.Device(ctx, s.sid)
	if err != nil {
		return fmt.Errorf("whatsmeow: rebuild %s: %w", s.sid, err)
	}

	s.mu.Lock()
	previous, handlerID := s.client, s.handlerID
	s.mu.Unlock()
	s.detach(previous, handlerID)
	previous.Disconnect()

	// A false here is the session having closed while this ran, which adopt has already
	// cleaned up after. There is nothing left to do either way.
	_ = s.adopt(wm.NewClient(device, s.waLog))
	return nil
}

// recover puts a session that was logged out back where a fresh pairing can start:
// the revoked credentials gone, and a client that is not the deleted one.
//
// Both halves, because a cleanup that failed leaves the mapping pointing at the revoked
// device, and rebuilding from that hands the session the very credentials WhatsApp
// threw away. Doing them together is what makes the retry a retry.
func (s *Session) recover(ctx context.Context) error {
	if err := s.store.Forget(ctx, s.sid); err != nil {
		return err
	}
	return s.rebuild(ctx)
}

// markStale records that this session is on a client nothing works on. The next connect
// tries the rebuild again rather than talking to it.
func (s *Session) markStale() {
	s.mu.Lock()
	s.stale = true
	s.mu.Unlock()
}

func (s *Session) isStale() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stale
}

// Execute carries out one command.
//
// What is not here is refused rather than answered with a plausible shape: a connector
// that acknowledged a send it cannot make would lose the message and report success.
func (s *Session) Execute(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	switch command.Type {
	case protocol.CommandSessionStatus:
		return json.Marshal(s.status())
	case protocol.CommandPairingPasskeyResponse:
		return nil, s.answerPasskey(ctx, command)
	case protocol.CommandPairingPasskeyConfirm:
		return nil, s.confirmPasskey(ctx, command)
	case protocol.CommandPairingRequestCode:
		return nil, s.requestCode(ctx, command)
	case protocol.CommandMessageSend:
		return s.send(ctx, command)
	case protocol.CommandMessageEdit:
		return s.edit(ctx, command)
	case protocol.CommandMessageRevoke:
		return s.revoke(ctx, command)
	case protocol.CommandMessageReact:
		return s.react(ctx, command)
	case protocol.CommandMessageDownloadMedia:
		return s.downloadMedia(ctx, command)
	case protocol.CommandMessageMarkRead:
		return s.markRead(ctx, command)
	case protocol.CommandPresenceSet:
		return s.setPresence(ctx, command)
	case protocol.CommandPresenceSubscribe:
		return s.subscribePresence(ctx, command)
	case protocol.CommandChatPresence:
		return s.chatPresenceCommand(ctx, command)
	}
	return nil, engine.ErrNotSupported
}

// requestCode is `pairing.request_code`, which asks for the same thing a connect with
// `pairing: "code"` asks for and is answered the same way.
//
// Through Connect rather than straight to pairWithCode: everything that guards a connect
// guards this too. A disconnect still on its way down has to be waited out, the guard
// that refuses a socket this session is done with has to come off, and a client whose
// device was deleted underneath it has to be rebuilt first. Reaching past all of that to
// the pairing itself is a second way into the same state with none of the rules.
func (s *Session) requestCode(ctx context.Context, command *protocol.Command) error {
	var body struct {
		Phone string `json:"phone"`
	}
	if err := json.Unmarshal(command.Payload, &body); err != nil {
		return protocol.NewError(protocol.ErrorInvalidPayload, "the pairing request could not be read")
	}
	// The subscription comes along, because this is a connect like any other and the
	// client is not sending one: leaving it out would turn group traffic off on a
	// session that had asked for it, at the moment it asked for a pairing code.
	return s.Connect(ctx, engine.ConnectRequest{
		Pairing: "code", Phone: body.Phone, Groups: s.wantsGroups(),
	})
}

// Close ends the session. Events is closed before it returns.
func (s *Session) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.drain()
		return nil
	}
	s.closed = true
	run := s.pairing
	s.pairing = nil
	closing := s.closing
	s.mu.Unlock()

	// Every placeholder still waiting is given up on here rather than left to fire into a
	// session that is closing: the publisher is going with it, and a goroutine holding a
	// timer past the session's own life is one nothing is left to answer for.
	s.forgetAwaited()

	// Announced first, while the teardown below still has a socket to close. The engine
	// only drops a cache entry here, and a session it can no longer hand out is the
	// point: an Open racing this must build a new client rather than get this one back.
	if closing != nil {
		closing()
	}

	if run != nil {
		run.cancel()
	}
	// Down before anything else, and before the context: a write already past its own
	// context check is one this still catches, and a session that is stopping owns
	// nothing whatever the reason it stopped.
	s.fence.Drop()
	// Cancelled first, and that order is the whole point: whatsmeow holds its socket
	// lock for the length of a dial, and Disconnect waits for the same lock. Cancelling
	// afterwards would never run, and a lease handover would wait out the handshake.
	s.cancel()

	s.mu.Lock()
	client, handlerID := s.client, s.handlerID
	s.mu.Unlock()

	// Closed before the handler is removed, and that order matters as much as the one
	// above. whatsmeow holds its handler lock while a handler runs and RemoveEventHandler
	// waits for the same lock; a handler sitting in emit with a full inbox is released
	// only by this channel. Removing first would have the two wait on each other, with
	// the socket still open and the lease already gone.
	close(s.done)

	s.detach(client, handlerID)
	client.Disconnect()

	s.drain()
	return nil
}

// drain waits for the forwarder to close Events, which is how Close keeps its promise
// that a reader of Events always terminates before Close returns.
func (s *Session) drain() {
	//nolint:revive // an empty body is the whole point: read until the forwarder closes it
	for range s.events {
	}
}

// Closed reports whether this session has been shut down.
func (s *Session) Closed() bool { return s.isClosed() }

func (s *Session) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// isCurrentPairing reports whether a run is still the one this session is on.
func (s *Session) isCurrentPairing(run *pairingRun) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pairing == run
}

// pairingActive reports whether a pairing conversation is open, which is what decides
// who publishes an outcome both paths are told about.
func (s *Session) pairingActive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pairing != nil
}

// startPairing makes a run the current one and ends whatever it replaces.
func (s *Session) startPairing(ctx context.Context, cancel context.CancelFunc) *pairingRun {
	s.pairingMu.Lock()
	defer s.pairingMu.Unlock()

	s.runs++
	run := &pairingRun{
		id:     s.sid + "-" + s.nonce + "-" + strconv.FormatUint(s.runs, 10),
		cancel: cancel,
		done:   ctx.Done(),
	}
	s.mu.Lock()
	previous := s.pairing
	s.pairing = run
	s.mu.Unlock()
	if previous != nil {
		previous.cancel()
	}
	return run
}

// endPairing clears a run that has finished and reports whether it was still the
// current one. A run that has been replaced owns nothing any more.
func (s *Session) endPairing(run *pairingRun) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pairing != run {
		return false
	}
	s.pairing = nil
	return true
}

// cancelPairing ends whatever conversation is open.
func (s *Session) cancelPairing() {
	s.mu.Lock()
	previous := s.pairing
	s.pairing = nil
	s.mu.Unlock()
	if previous != nil {
		previous.cancel()
	}
}

// status is the `connection_state` the contract answers session.status and
// session.connect with. Its key is `connection` and its number is `phone_number`: the
// event that reports the same change spells both differently, and answering an RPC
// with the event's shape leaves the caller without the one field the result requires.
func (s *Session) status() map[string]any {
	phone, lid := s.identity()
	state := map[string]any{"connection": s.state()}
	if phone != "" {
		state["phone_number"] = phone
	}
	if lid != "" {
		state["lid"] = lid
	}
	return state
}

// sessionState is the `session.state` event for the same connection, which spells the
// state under `state` and the number under `phone`.
func (s *Session) sessionState() map[string]any {
	phone, lid := s.identity()
	payload := map[string]any{"state": s.state()}
	if phone != "" {
		payload["phone"] = phone
	}
	if lid != "" {
		payload["lid"] = lid
	}
	return payload
}

// state answers without asking the client anything.
//
// Every question whatsmeow answers about its socket takes the lock a dial holds for its
// whole length, so asking here would put every status behind a connect nobody is
// waiting for. Its one lock-free answer, IsLoggedIn, is set on authentication and
// cleared only by a stream error: it stays true through a Disconnect, and a session
// that believed it would never reconnect again.
func (s *Session) state() string {
	s.mu.Lock()
	closed, dialing, connected := s.closed, s.dialing, s.connected
	pairing, reconnecting := s.pairing != nil, s.reconnecting
	s.mu.Unlock()

	switch {
	case closed:
		return "close"
	case connected:
		return "open"
	case reconnecting:
		return "reconnecting"
	case dialing, pairing:
		// The dial returns as soon as the socket is up, and the pairing conversation
		// runs on from there. Calling that closed would have the reply to session.connect
		// overwrite the `connecting` it just published, while the operator is looking at
		// a code.
		return "connecting"
	default:
		return "close"
	}
}

// forward moves emissions onto the channel the pump reads, and owns closing it.
func (s *Session) forward() {
	defer close(s.events)
	for {
		select {
		case <-s.done:
			return
		case item := <-s.inbox:
			emission := item.event
			if item.key != "" {
				// A marker, and this is the moment it stands for. The value is read now
				// rather than when it was posted, so what goes out is the chat's newest
				// state and not the one that happened to be queued.
				resolved, waiting := s.resolve(item.key, item.seq)
				if !waiting {
					continue
				}
				emission = resolved
			}
			if !s.handOn(emission) {
				return
			}
		}
	}
}

// presenceLife is how long a moment is worth publishing for. Past it the session knows
// more than the event does: whatever came next either replaced it on the board or was
// published, and a client shown the old one has no way to learn better.
const presenceLife = 10 * time.Second

// perishableHandoff bounds how long a moment waits on a reader that is busy. Short,
// because everything past it is added to how stale the event is by the time somebody
// sees it, and a moment is worth nothing stale.
const perishableHandoff = time.Second

// pending is what the forwarder takes off the inbox: an event to hand on, or the key of
// a board entry whose value is read when its turn comes.
//
// The marker is what keeps presence in step with everything else without ever waiting
// for room. It takes its place in the queue at the moment the state happens, and carries
// nothing, so the value it resolves to is whatever that chat's newest state is by the
// time the forwarder gets there.
type pending struct {
	event engine.Emission
	key   string
	// seq names the value this marker stands for, and the board entry holds the same
	// number for as long as that value is the chat's newest. A marker the chat has moved
	// on from resolves to nothing, which is what keeps a state at the place it happened
	// at rather than at the place an older one is waiting in.
	seq int64
}

// posted is what the board holds for one chat: its newest state, and enough about where
// that state has got to for a failure to be told apart from a supersession.
//
// `seq` names this value: the marker that will resolve it carries the same number, and
// so does the callback the publisher owes it, so both can tell that the chat has moved on
// since. `sent` says the marker has already been resolved. `retried` is what makes the
// retry one more go rather than a loop.
type posted struct {
	emission engine.Emission
	seq      int64
	sent     bool
	retried  bool
	// transitions is what the connection counter read when this state happened, so a
	// failure coming back later can tell that the socket it describes is gone.
	transitions int64
}

// handOn gives one emission to the reader, and reports whether the forwarder should
// carry on.
//
// A moment gets two things a fact does not: it is dropped if it went stale waiting, and
// its handoff is bounded. The second is what makes the first mean anything -- an
// unbounded handoff would pass the freshness check and then sit on a reader that is
// busy, and what came out would be exactly the stale event the check is for.
func (s *Session) handOn(emission engine.Emission) bool {
	if s.picked != nil {
		// Taken, and about to be handed on. A test that needs the forwarder parked here
		// rather than racing it reads this. Never waits: a hook that can hold the
		// forwarder is a hook that can hang the thing it was put there to watch.
		select {
		case s.picked <- struct{}{}:
		default:
		}
	}
	if emission.Expires == nil {
		select {
		case s.events <- emission:
			return true
		case <-s.done:
			return false
		}
	}
	if emission.Expires() <= 0 {
		s.log.Debug().Str("type", string(emission.Type)).
			Msg("dropping a transient event that waited too long to still be true")
		return true
	}
	handoff := time.NewTimer(s.handoffWait)
	defer handoff.Stop()
	select {
	case s.events <- emission:
	case <-handoff.C:
		s.log.Debug().Str("type", string(emission.Type)).
			Msg("dropping a transient event the reader was not there for")
	case <-s.done:
		return false
	}
	return true
}

// post makes a presence the newest state of its chat and puts a marker for it in the
// inbox, and never waits.
//
// Waiting is the whole reason presence does not go through the inbox as a value: it
// would hold WhatsApp's node handler for as long as the publisher is down, and what came
// out the other side would be a fact about a minute that has passed. A marker costs a
// queue slot and is taken without one being free only when there is none, which is a
// publisher that has already stopped answering.
//
// A chat whose marker has not been resolved yet needs no second one -- the one already
// in the queue reads whatever is newest when it gets there -- so a burst of typing costs
// one slot rather than one per event.
func (s *Session) post(key string, eventType protocol.EventType, payload any, life time.Duration) {
	body, err := json.Marshal(payload)
	if err != nil {
		s.log.Error().Err(err).Str("type", string(eventType)).Msg("failed to render an event payload")
		return
	}
	// The connection as it is now, and not as it will be when this reaches the publisher:
	// what the rest of this has to know is which socket reported the state, and a drop
	// while the marker waits its turn is exactly the case where that socket is gone. Read
	// at hand-over instead, the drop would be counted as having happened before the state
	// rather than after it, and both the publish and the retry would go out behind the
	// close.
	//
	// A node from a socket that is already down is refused outright. whatsmeow runs the
	// node handlers and the connection ones on separate goroutines, so a presence from
	// the old socket can arrive after the disconnect has been dealt with -- and then no
	// ordering the posting end could arrange would help, because what it describes has
	// been over since before it got here. A message in that position is still a message;
	// a presence is a claim about right now.
	generation, up := s.connection()
	if !up {
		s.log.Debug().Str("type", string(eventType)).
			Msg("dropping presence reported by a connection that is already down")
		return
	}
	emission := engine.Emission{Type: eventType, Payload: body, At: s.learned()}
	if life > 0 {
		perishes := s.since() + life
		emission.Expires = func() time.Duration { return perishes - s.since() }
	}

	s.boardMu.Lock()
	defer s.boardMu.Unlock()
	s.boardSeq++
	entry := posted{emission: emission, seq: s.boardSeq, transitions: generation}
	if life == 0 {
		// A state that corrects something the client has already been shown, and there
		// is nothing after it: a stop is the end of a typing burst, and somebody going
		// away is the end of them being there. Lost to a publisher having a bad second
		// it is not sent again by WhatsApp and not superseded by anything, so the client
		// is left with the state before it until that person does something else --
		// which may be never.
		entry.emission.Settle = s.settled(key, entry.seq)
	}
	// A place of its own, every time, and never the one a marker for this chat is already
	// holding. Taking that place would publish this state where the older one stood, and
	// whether anything has queued between the two since is not a question this can answer:
	// a reservation and a send are two steps, and another producer can land between them,
	// so a count of what has been queued is a guess. The older marker resolves to nothing
	// when its turn comes, which costs a slot in the queue and buys the one thing the
	// marker exists for -- a state published where it happened, and not earlier.
	select {
	case s.inbox <- pending{key: key, seq: entry.seq}:
		s.board[key] = entry
	default:
		// The queue presence shares with the messages is full, which is a publisher that
		// has stopped answering while 256 messages piled up behind it. Presence waits for
		// nothing, so this is dropped -- and whatever the chat had before is left where it
		// is, because that one is already on its way and this one never started.
		//
		// A stop dropped here is a stop nothing replaces, which is the cost of sharing the
		// queue and what buys the order. Registered as #47.
		s.log.Debug().Str("type", string(eventType)).
			Msg("dropping presence the inbox had no room for")
	}
}

// resolve reads what a marker stands for, and is the last moment that value can still
// change. A moment leaves the board here, because nothing comes back about one; a
// durable state stays, marked as gone, so its callback can tell a failure from having
// been replaced.
func (s *Session) resolve(key string, seq int64) (engine.Emission, bool) {
	s.boardMu.Lock()
	defer s.boardMu.Unlock()
	entry, waiting := s.board[key]
	if !waiting || entry.seq != seq {
		// A place the chat has moved on from: the state this marker was for was replaced,
		// and the one that replaced it has a place of its own further along.
		return engine.Emission{}, false
	}
	if entry.transitions != s.transitions.Load() {
		// The connection this state was reported on has gone since it was posted, and
		// the event that says so is in this same queue. Published after it, this lands
		// on a client that clears presence when it sees a session go, and nothing comes
		// to correct it a second time; published before it, that same event clears it
		// anyway. There is nothing to lose by dropping it and one thing to lose by not.
		//
		// This is also the answer to the two handlers racing. A presence node and a
		// disconnect reach this from different goroutines with no order between them, so
		// no amount of care at the posting end decides which of the two queues first --
		// but whichever way it lands, the state is not published on the far side of the
		// connection that produced it.
		delete(s.board, key)
		s.log.Debug().Str("type", string(entry.emission.Type)).
			Msg("dropping a presence whose connection went before its turn came")
		return engine.Emission{}, false
	}
	if entry.emission.Settle == nil {
		delete(s.board, key)
		return entry.emission, true
	}
	entry.sent = true
	s.board[key] = entry
	return entry.emission, true
}

// settled returns the callback the publisher owes a durable presence, which gives it one
// more go when the publish it was handed to failed.
//
// One more, and not a loop: a publisher that is down stays down for longer than any
// number of immediate retries, and the point here is a bad second rather than an outage.
// And only while this is still the chat's newest state -- anything posted after it is
// what the client should end up with, and the sequence says so whether that newer state
// is still waiting for its turn or has already gone out.
func (s *Session) settled(key string, seq int64) func(error) {
	return func(err error) {
		s.boardMu.Lock()
		defer s.boardMu.Unlock()
		entry, waiting := s.board[key]
		if !waiting || entry.seq != seq {
			// Replaced by a newer state for the same chat, which is the one the client
			// should end up with.
			return
		}
		if err == nil || entry.retried {
			delete(s.board, key)
			return
		}
		if s.transitions.Load() != entry.transitions {
			// The session has been through a state of its own since this was handed over,
			// and presence does not survive one: the subscription that produced it is
			// gone, and a client that cleared presence when it saw the session go would
			// have this put back on top with nothing coming to correct it again.
			delete(s.board, key)
			s.log.Debug().Str("type", string(entry.emission.Type)).
				Msg("dropping a presence whose session changed under it before it could be tried again")
			return
		}
		entry.retried, entry.sent = true, false
		select {
		case s.inbox <- pending{key: key, seq: entry.seq}:
			s.board[key] = entry
			s.log.Debug().Str("type", string(entry.emission.Type)).
				Msg("giving a presence another go after a publish that failed")
		default:
			delete(s.board, key)
			s.log.Debug().Str("type", string(entry.emission.Type)).
				Msg("dropping a presence the inbox had no room to try again for")
		}
	}
}

func (s *Session) emit(eventType protocol.EventType, payload any) {
	body, err := json.Marshal(payload)
	if err != nil {
		// Everything reaching this is built a few lines above, so a failure is a
		// programming error rather than something a session should carry on through.
		s.log.Error().Err(err).Str("type", string(eventType)).Msg("failed to render an event payload")
		return
	}
	select {
	case s.inbox <- pending{event: engine.Emission{Type: eventType, Payload: body, At: s.learned()}}:
	case <-s.done:
	}
}

// readPairing publishes the QR codes and the outcome of the pairing.
func (s *Session) readPairing(run *pairingRun, codes <-chan wm.QRChannelItem) {
	s.readPairingWith(run, codes, nil, true)
}

// readPairingWith drains the pairing channel. `onFirst` fires once the server has
// answered at all, which is what code pairing waits for, and `publishCodes` is false
// for the code flow, which has no image to show.
func (s *Session) readPairingWith(run *pairingRun, codes <-chan wm.QRChannelItem, onFirst func(bool), publishCodes bool) {
	// Cleared on the way out, however it ends. A run left marked open after a successful
	// pairing goes on claiming every later outcome, and the events it claims are then
	// published by nobody: the QR channel's own handler is long gone.
	defer func() { _ = s.endPairing(run) }()

	first := true
	// Called on every way out but the one where a code arrived: whoever is waiting for
	// the connection is waiting for something that will never come.
	nothingArrived := func() {
		if first && onFirst != nil {
			onFirst(false)
		}
	}

	for {
		var item wm.QRChannelItem
		var open bool
		select {
		case item, open = <-codes:
			if !open {
				nothingArrived()
				return
			}
		case <-run.done:
			// Given up on. Ranging the channel alone would not end here: whatsmeow closes
			// it from the goroutine that emits codes, and a conversation whose dial failed
			// before the first code never started one. This reader would then wait on it
			// for the life of the process, holding the session and the client behind it,
			// once per failed attempt.
			nothingArrived()
			return
		case <-s.done:
			nothingArrived()
			return
		}

		if item.Event == "code" && first {
			first = false
			if onFirst != nil {
				onFirst(true)
			}
		}
		s.publishPairing(run, item, publishCodes)
	}
}

func (s *Session) publishPairing(run *pairingRun, item wm.QRChannelItem, publishCodes bool) {
	if !s.isCurrentPairing(run) {
		// The operator disconnected or started over while this item was being rendered.
		// An expired code or a terminal error from an attempt nobody is watching lands
		// after the state that replaced it.
		return
	}

	switch item.Event {
	case "code":
		if !publishCodes {
			return
		}
		image, err := qrDataURL(item.Code)
		if err != nil {
			s.log.Error().Err(err).Msg("failed to render a pairing code")
			return
		}
		s.emit(protocol.EventPairingQR, map[string]any{
			"png_data_url":  image,
			"expires_in_ms": item.Timeout.Milliseconds(),
		})
	case "success":
		// pairing.success is published from the PairSuccess event, which is the one
		// that carries the address that was paired.
	case wm.QRChannelEventPasskeyRequest:
		// Progress, not an outcome. WhatsApp is asking the operator's browser to sign a
		// WebAuthn challenge, and the conversation carries on once it answers: treating
		// this as terminal is a pairing that dies on every account WhatsApp routes this
		// way, with a reason that names none of it.
		s.publishPasskeyRequest(run, item.PasskeyRequest)
	case wm.QRChannelEventPasskeyResponse:
		// Also progress: the code the operator checks against the one on the phone.
		// whatsmeow answers by itself when WhatsApp says the check can be skipped, and
		// then this item never arrives.
		if item.PasskeyConfirmation == nil {
			return
		}
		s.emit(protocol.EventPairingPasskeyConfirmation, map[string]any{
			"request_id": run.id, "code": item.PasskeyConfirmation.Code,
		})
	case "err-client-outdated":
		s.emit(protocol.EventSessionClientOutdated, map[string]any{})
	case "timeout":
		s.finishPairing(run, "timeout", nil)
	case "error":
		s.finishPairing(run, "error", item.Error)
	default:
		s.finishPairing(run, item.Event, item.Error)
	}
}

// publishPasskeyRequest hands the operator's client the challenge WhatsApp wants signed.
//
// The public key travels exactly as whatsmeow parsed it: the contract calls it WebAuthn
// PublicKeyCredentialRequestOptions with base64url fields, which is what these types
// already marshal to. Re-shaping it here would be a second place for the two to drift.
func (s *Session) publishPasskeyRequest(run *pairingRun, request *waEvents.PairPasskeyRequest) {
	if request == nil || request.PublicKey == nil {
		s.finishPairing(run, "passkey_error", errors.New("WhatsApp asked for a passkey and sent no challenge"))
		return
	}
	publicKey, err := json.Marshal(request.PublicKey)
	if err != nil {
		s.finishPairing(run, "passkey_error", err)
		return
	}
	s.emit(protocol.EventPairingPasskeyRequest, map[string]any{
		"request_id": run.id, "public_key": json.RawMessage(publicKey),
	})
}

// answerPasskey hands WhatsApp the assertion the operator's browser produced.
func (s *Session) answerPasskey(ctx context.Context, command *protocol.Command) error {
	var body struct {
		RequestID  string                    `json:"request_id"`
		Credential *waTypes.WebAuthnResponse `json:"credential"`
	}
	if err := json.Unmarshal(command.Payload, &body); err != nil {
		return protocol.NewError(protocol.ErrorInvalidPayload, "the passkey credential could not be read")
	}
	if body.Credential == nil {
		return protocol.NewError(protocol.ErrorInvalidPayload, "a passkey response has to carry a credential")
	}
	run, err := s.pairingNamed(body.RequestID)
	if err != nil {
		return err
	}
	if err := s.current().SendPasskeyResponse(ctx, body.Credential); err != nil {
		s.finishPairing(run, "passkey_error", err)
		return protocol.NewError(protocol.ErrorWaError, "WhatsApp would not accept that passkey")
	}
	return nil
}

// confirmPasskey tells WhatsApp the operator checked the code against their phone. A
// operator who says it does not match is one WhatsApp must not be told anything by: the
// attempt ends here instead.
func (s *Session) confirmPasskey(ctx context.Context, command *protocol.Command) error {
	var body struct {
		RequestID string `json:"request_id"`
		Confirmed *bool  `json:"confirmed"`
	}
	if err := json.Unmarshal(command.Payload, &body); err != nil {
		return protocol.NewError(protocol.ErrorInvalidPayload, "the passkey confirmation could not be read")
	}
	// An absent flag is not a refusal. Read into a plain bool it becomes one, so a
	// truncated payload ends the pairing the operator is watching and answers success:
	// the same answer the connector gives when they genuinely said the code did not
	// match, and nothing downstream could tell a client bug from a decision.
	if body.Confirmed == nil {
		return protocol.NewError(protocol.ErrorInvalidPayload,
			"a passkey confirmation has to say whether the code matched")
	}
	run, err := s.pairingNamed(body.RequestID)
	if err != nil {
		return err
	}
	if !*body.Confirmed {
		s.finishPairing(run, "passkey_refused", nil)
		return nil
	}
	if err := s.current().SendPasskeyConfirmation(ctx); err != nil {
		s.finishPairing(run, "passkey_error", err)
		return protocol.NewError(protocol.ErrorWaError, "WhatsApp would not accept that confirmation")
	}
	return nil
}

// pairingNamed is the conversation an answer belongs to, if it is still the one running.
//
// An answer that names an attempt the operator has already replaced is refused rather
// than sent: WhatsApp would take it as this attempt's, and the operator would be watching
// a pairing that fails for a reason belonging to the one before it.
func (s *Session) pairingNamed(id string) (*pairingRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pairing == nil {
		return nil, protocol.NewError(protocol.ErrorNotPaired, "this session has no pairing waiting for an answer")
	}
	if id != s.pairing.id {
		return nil, protocol.NewError(protocol.ErrorNotPaired,
			"that answer belongs to a pairing attempt this session has moved on from")
	}
	return s.pairing, nil
}

// finishPairing publishes the end of a conversation, once, and only while it is still
// the one this session is on.
//
// Retiring it and publishing have to be one step in that order. A terminal outcome takes
// the connection down and raises the guard that refuses a late connect: done a moment
// after the operator's replacement attempt has started, that is this attempt closing the
// socket the next one just opened, and a retry at the end of a pairing failing for
// reasons of its own.
func (s *Session) finishPairing(run *pairingRun, reason string, err error) {
	s.pairingMu.Lock()
	defer s.pairingMu.Unlock()

	if !s.endPairing(run) {
		return
	}
	s.publishPairingFailure(reason, err)
	// The socket does not always go with the outcome. A code scanned on a phone without
	// multidevice leaves the client connected with its pairing channel live, and
	// whatsmeow will not open a second one on a live socket: the operator's corrected
	// attempt is then refused until WhatsApp's own codes run out, for a reason that has
	// nothing to do with it.
	s.tearDownPairing(run, s.current())
}

// bind records the pairing before whatsmeow writes the device, which is what keeps a
// crash between the two from leaving credentials no session claims. Refusing here
// cancels the pairing, which is the right outcome: a device we cannot attribute is one
// no restart can find again.
func (s *Session) bind(jid waTypes.JID, _, _ string) bool {
	// From the session's own lifetime: a pairing that lands after this instance lost
	// the account would otherwise overwrite the mapping the new owner is writing.
	ctx, cancel := context.WithTimeout(s.ctx, bindTimeout)
	defer cancel()

	if err := s.store.Bind(ctx, s.sid, jid); err != nil {
		s.log.Error().Err(err).Msg("failed to record a pairing; refusing it")
		return false
	}
	return true
}

// bindTimeout bounds the write that stands between a scanned code and a paired
// session. It is short because WhatsApp is waiting on the other side of it.
const bindTimeout = 5 * time.Second

// codeForPairPhone names the refusals the caller can fix. Left as an internal error
// they reach the dashboard as "the connector could not carry out the command", which
// tells an operator nothing about the number they typed.
func codeForPairPhone(err error) error {
	switch {
	case errors.Is(err, wm.ErrPhoneNumberTooShort):
		return protocol.NewError(protocol.ErrorInvalidPayload, "that number is too short to pair")
	case errors.Is(err, wm.ErrPhoneNumberIsNotInternational):
		return protocol.NewError(protocol.ErrorInvalidPayload,
			"that number needs its country code and no leading zero")
	default:
		return nil
	}
}

func qrDataURL(code string) (string, error) {
	png, err := qrcode.Encode(code, qrcode.Medium, qrSize)
	if err != nil {
		return "", fmt.Errorf("whatsmeow: render a pairing code: %w", err)
	}
	return "data:image/png;base64," + base64.StdEncoding.EncodeToString(png), nil
}

// publishPairingFailure names what went wrong without putting the library's own words
// on the wire. A `PairDatabaseError` or a protobuf failure carries SQL and internals
// that mean nothing to an operator and should not reach a client's UI; the detail stays
// in the log, where whoever is debugging it can find it.
func (s *Session) publishPairingFailure(reason string, err error) {
	if err != nil {
		s.log.Warn().Err(err).Str("reason", reason).Msg("a pairing failed")
	}
	s.emit(protocol.EventPairingError, map[string]any{
		"reason": reason, "message": pairingFailureMessage(reason),
	})

	// The connection went with it. whatsmeow closes the socket when the codes run out
	// and publishes no Disconnected for an unpaired device, so nothing else would take
	// the state down: the dial flag would stand and `session.status` would answer
	// `connecting` for a pairing that ended minutes ago.
	s.transition.Lock()
	defer s.transition.Unlock()

	s.refuseLateConnect()
	s.offline()
	s.emit(protocol.EventSessionState, map[string]any{"state": "close", "reason": "pairing_" + reason})
}

// pairingFailureMessage is the stable sentence a client shows for each reason.
func pairingFailureMessage(reason string) string {
	switch reason {
	case "timeout":
		return "nobody scanned the code before it ran out"
	case "pair_error":
		return "WhatsApp accepted the code but the pairing could not be completed"
	case "err-scanned-without-multidevice":
		return "that account still has to turn multi-device on before it can be linked"
	case "code_refused":
		return "WhatsApp would not send a pairing code to that number"
	case "connect_failed":
		return "the connector could not reach WhatsApp"
	default:
		return "the pairing did not complete"
	}
}

// handle turns what whatsmeow reports into what the contract names.
//
// It reports whether whatsmeow may acknowledge what it just delivered. A false leaves
// the message unacknowledged on WhatsApp's side, so the account keeps it and delivers
// it again, which is the only honest answer while this build has nowhere to put it: an
// acknowledged message nobody published is a message that is simply gone.
func (s *Session) handle(rawEvent any) bool {
	switch event := rawEvent.(type) {
	case *waEvents.Message:
		// The one handler that blocks, and the only place the ack invariant is decided:
		// WhatsApp is told the account has the message after the client does, never
		// before. Everything this build cannot render yet is still refused, which is
		// what keeps it on the phone for a later milestone.
		return s.receive(event)
	case *waEvents.UndecryptableMessage:
		// A message that arrived with nothing in it to read. Unlike everything else on
		// this path it cannot be kept on the phone for a later build: whatsmeow has
		// already acknowledged the node by the time this runs, so refusing here buys no
		// redelivery and the choice is between publishing something and publishing
		// nothing.
		return s.unreadable(event)
	case *waEvents.ChatPresence:
		// Published and acknowledged whatever happens, which is the one place on this
		// path that does not withhold: a moment redelivered is a lie, and the state that
		// corrects it was published while the stale one was being retried.
		return s.chatPresence(event)
	case *waEvents.Presence:
		return s.presence(event)
	case *waEvents.Receipt:
		// The other handler that can withhold an acknowledgement, and for the same
		// reason: a tick nobody published never turns, and the client cannot ask again.
		return s.receipt(event)
	case *waEvents.Connected:
		// whatsmeow dispatches from whichever goroutine produced the event, so a
		// Disconnected and the Connected that follows it can be handled at the same
		// time. Holding this across both the state change and the event it announces is
		// what keeps the two from crossing: without it one handler can write its state,
		// be overtaken, and publish afterwards, leaving `session.status` saying one
		// thing and the last event on the stream saying the other. Which of two
		// simultaneous transitions lands last is whatsmeow's to decide and not knowable
		// here; that they agree is not.
		s.transition.Lock()
		defer s.transition.Unlock()

		if s.undoHangUp() {
			// A reconnect that was already past its wait when the disconnect landed. The
			// command has answered `close`, so this socket is one nobody asked for.
			s.log.Info().Msg("closing a socket that came back after a disconnect")
			go s.current().Disconnect()
			return true
		}
		s.setConnected(true)
		s.emit(protocol.EventSessionState, s.sessionState())
	case *waEvents.Disconnected:
		s.transition.Lock()
		defer s.transition.Unlock()

		s.setConnected(false)
		if s.hangUpStanding() {
			// A drop from a socket this session is already done with. whatsmeow
			// dispatches this one from a goroutine of its own, so a remote drop landing
			// just before a disconnect completes is handled just after it: the `close`
			// that settled has `reconnecting` published on top of it, for a socket
			// whatsmeow was told to stay off and is not going to dial again. And the
			// flag outlives the event, because the Connected that follows is answered by
			// closing the socket rather than by clearing it — so resume reads the session
			// as one whatsmeow is already recovering and returns without dialling, for
			// good.
			return true
		}
		// whatsmeow only reconnects a device it has an id for, so a drop before pairing
		// finishes is the end of that attempt. Reporting it as reconnecting leaves the
		// dashboard waiting on something nothing is going to do.
		state := "reconnecting"
		if phone, _ := s.identity(); phone == "" {
			state = "close"
		}
		s.setReconnecting(state == "reconnecting")
		s.emit(protocol.EventSessionState, map[string]any{"state": state, "reason": "disconnected"})
	case *waEvents.LoggedOut:
		s.loggedOut(event)
	case *waEvents.StreamReplaced:
		s.transition.Lock()
		defer s.transition.Unlock()

		// whatsmeow suppresses the ordinary Disconnected for this one, so nothing else
		// would take the connection down and the session would report itself open over a
		// stream somebody else now holds. The guard goes with it: a Connected this socket
		// produced before it was replaced would otherwise put the session back up.
		s.refuseLateConnect()
		s.offline()
		s.emit(protocol.EventSessionStreamReplaced, map[string]any{})
	case *waEvents.TemporaryBan:
		// Terminal, all three of these: whatsmeow only publishes them from the branch
		// that told the socket to stay down, so nothing is going to reconnect and
		// nothing else is going to take the state with it. A Connected the socket
		// produced on its way out would otherwise put the session back up over a
		// connection whatsmeow will not make again, and a resume would then find
		// nothing to do.
		s.transition.Lock()
		defer s.transition.Unlock()

		s.refuseLateConnect()
		s.offline()
		ban := map[string]any{"kind": "temporary", "reason": event.Code.String()}
		if event.Expire > 0 {
			// A zero duration is whatsmeow saying it does not know when the ban lifts.
			// Publishing now+0 would read as one that already has.
			ban["expires_at"] = time.Now().Add(event.Expire).UnixMilli()
		}
		s.emit(protocol.EventSessionTemporaryBan, map[string]any{"ban": ban})
	case *waEvents.ClientOutdated:
		s.transition.Lock()
		defer s.transition.Unlock()

		s.refuseLateConnect()
		s.offline()
		if s.pairingActive() {
			// The pairing reader publishes this one: whatsmeow delivers it here and to
			// the QR channel both, and two canonical events for one outcome is worse
			// than either.
			return true
		}
		s.emit(protocol.EventSessionClientOutdated, map[string]any{})
	case *waEvents.ConnectFailure:
		s.transition.Lock()
		defer s.transition.Unlock()

		s.refuseLateConnect()
		s.offline()
		s.emit(protocol.EventSessionConnectFailure, map[string]any{
			"reason": event.Reason.String(), "code": int(event.Reason),
		})
	case *waEvents.PairSuccess:
		s.paired(event)
	case *waEvents.PairError:
		// Whatever the QR channel does with this, the client is on a device whatsmeow
		// may have half-written: an id with no credentials, or one it marked deleted.
		// The next attempt would be refused by the library rather than by WhatsApp.
		s.markStale()
		if s.pairingActive() {
			return true
		}
		s.publishPairingFailure("pair_error", event.Error)
	}
	return true
}

func (s *Session) loggedOut(event *waEvents.LoggedOut) {
	// Settled like a logout this session asked for, because from here it is the same
	// thing: whatsmeow expects this disconnect and publishes nothing more for it, so a
	// Connected it had already queued would set the state back to connected and publish
	// `open` after session.logged_out, over an account WhatsApp has revoked, with nothing
	// arriving later to correct it.
	s.settleLogout()

	// The credentials are gone on WhatsApp's side, so keeping them here would have
	// every reconnect fail with a session that looks resumable and is not.
	// On the session's own lifetime: `Forget` unbinds the mapping, so a stale owner
	// finishing this after the account moved would erase what the new owner wrote.
	ctx, cancel := context.WithTimeout(s.ctx, s.storeLimit)
	defer cancel()
	cleaned := true
	if err := s.store.Forget(ctx, s.sid); err != nil {
		cleaned = false
		s.log.Error().Err(err).Msg("failed to forget the device of a session that was logged out")
	}
	// Off this goroutine, and that is not a preference: whatsmeow dispatches events
	// holding its handler lock for read, and rebuilding takes the same lock for write.
	// Doing it here is a deadlock against ourselves, and the shutdown behind it.
	//
	// The event waits for the replacement. A client that reacts to session.logged_out
	// by pairing again is the expected next step, and reaching a session still holding
	// the deleted device answers it with an error for a state that had already passed.
	go func() {
		// The event goes out either way: the account was logged out whatever happened
		// here, and a client left waiting for that news is worse off than one told late
		// that its next connect has to try again.
		//
		// Rebuilding on top of a cleanup that failed would be worse than not rebuilding:
		// the mapping still points at the revoked device, so the fresh client would be
		// handed the very credentials WhatsApp threw away, and `adopt` would call the
		// session healthy again.
		if !cleaned {
			s.markStale()
		} else if err := s.rebuildWithin(); err != nil {
			s.markStale()
			s.log.Error().Err(err).Msg("failed to put a logged-out session back on a fresh client")
		}
		s.emit(protocol.EventSessionLoggedOut, map[string]any{
			"reason": event.Reason.String(), "on_connect": event.OnConnect,
		})
	}()
}

func (s *Session) paired(event *waEvents.PairSuccess) {
	lid := ""
	if !event.LID.IsEmpty() {
		lid = event.LID.User
	}
	s.setIdentity(event.ID.User, lid)

	payload := map[string]any{"phone": event.ID.User, "platform": event.Platform}
	if lid != "" {
		payload["lid"] = lid
	}
	s.emit(protocol.EventPairingSuccess, payload)
}
