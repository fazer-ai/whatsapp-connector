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
	"go.mau.fi/whatsmeow/appstate"
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
	store *store.Scoped
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
	download func(context.Context, *wm.Client, wm.DownloadableMessage, media.File) error

	// onWhatsApp and profilePicture are the two contact queries. Fields for the same
	// reason as download: both are a single IQ to WhatsApp's own servers, so without
	// them a test can reach the payload this connector refuses and nothing past it --
	// not the ordering a check has to answer in, and neither of the two refusals a
	// picture query answers with, which are the parts that decide what a client shows.
	// sendAppState hands WhatsApp a patch to this account's own state. A field for the
	// same reason as the two below it: nothing outside a real socket can answer one, so
	// a test cannot otherwise reach what a failed patch is reported as.
	sendAppState func(context.Context, *wm.Client, appstate.PatchInfo) error

	// groupInfo reads a group's metadata. A field for the same reason as the queries
	// below it: it is one IQ, so a test can otherwise reach the payload this connector
	// refuses and nothing of what it makes of an answer.
	groupInfo func(context.Context, *wm.Client, waTypes.JID) (*waTypes.GroupInfo, error)

	// joinedGroups reads every group this account is in. One IQ, like groupInfo above it.
	joinedGroups func(context.Context, *wm.Client) ([]*waTypes.GroupInfo, error)

	// createTheGroup makes one. A seam like the queries above it: one IQ, and a test can
	// otherwise reach the payload this connector refuses and nothing of what it makes of
	// an answer.
	createTheGroup func(context.Context, *wm.Client, wm.ReqCreateGroup) (*waTypes.GroupInfo, error)

	onWhatsApp func(context.Context, *wm.Client, []string) ([]waTypes.IsOnWhatsAppResponse, error)
	//nolint:lll // one line per seam reads better than a wrapped signature
	updateParticipants func(context.Context, *wm.Client, waTypes.JID, []waTypes.JID, wm.ParticipantChange) ([]waTypes.GroupParticipant, error)
	inviteLink         func(context.Context, *wm.Client, waTypes.JID, bool) (string, error)
	joinRequests       func(context.Context, *wm.Client, waTypes.JID) ([]waTypes.GroupParticipantRequest, error)
	//nolint:lll // one line per seam reads better than a wrapped signature
	decideJoinRequests func(context.Context, *wm.Client, waTypes.JID, []waTypes.JID, wm.ParticipantRequestChange) ([]waTypes.GroupParticipant, error)
	leave              func(context.Context, *wm.Client, waTypes.JID) error
	setName            func(context.Context, *wm.Client, waTypes.JID, string) error
	setPhoto           func(context.Context, *wm.Client, waTypes.JID, []byte) error
	setDescription     func(context.Context, *wm.Client, waTypes.JID, string, string) error
	// The two calls writeTheDescription picks between. Seams of their own because which
	// one a write takes is the whole of what it decides, and only one of them is bounded.
	setTopic        func(context.Context, *wm.Client, waTypes.JID, string, string, string) error
	setAnnounce     func(context.Context, *wm.Client, waTypes.JID, bool) error
	setLocked       func(context.Context, *wm.Client, waTypes.JID, bool) error
	setJoinApproval func(context.Context, *wm.Client, waTypes.JID, bool) error
	setAddMode      func(context.Context, *wm.Client, waTypes.JID, waTypes.GroupMemberAddMode) error
	profilePicture  func(context.Context, *wm.Client, waTypes.JID, *wm.GetProfilePictureParams) (*waTypes.ProfilePictureInfo, error)

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

	// unsealReaction opens a reaction sealed under the secret of the message it is on,
	// and nil is the socket's own. Replaced by the tests that stand in for the two ways
	// it fails, which are opposite answers and neither is reachable from a stanza alone.
	unsealReaction func(context.Context, *waEvents.Message) (*waE2E.ReactionMessage, error)

	// askReupload asks the sender's phone to upload a file again, for one whose bytes
	// WhatsApp has dropped. A field for the same reason as the seams around it: only a
	// real socket answers a receipt, so a test cannot otherwise reach either side of
	// what a phone says when it is asked.
	askReupload func(context.Context, *wm.Client, *waTypes.MessageInfo, []byte) error

	// sendPresence tells WhatsApp what the account is. A field for the same reason as
	// the seams above it: a presence node is only answered by a real socket, so a test
	// cannot otherwise see that the account is told again on a new connection what it
	// last said it was.
	sendPresence func(context.Context, *wm.Client, waTypes.Presence) error

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

	// availability is the last presence WhatsApp took from this account, and nil while
	// the client has asked for none. It is kept because WhatsApp forgets it when the
	// connection goes and whatsmeow does not send it again, so a connection that comes
	// back has to be told.
	//
	// Its own lock rather than mu, and held for the field and nothing else -- never
	// across the write, so the one path that must not wait on a socket does not: a
	// rebuild clears this, and a rebuild is what a logout is waiting on.
	availability   *asked
	availabilityMu sync.Mutex

	// aliases is the one place that turns a JID into the address the wire carries, so
	// every path publishes a conversation under the same one. See addressing.go.
	aliases *alias

	// presenceWait bounds the presence node nobody is waiting on: the one a connection
	// that came back is told. A field for the same reason as the waits above it, and for
	// no other.
	//
	// Bounded at all because it is the one presence write with no caller behind it to
	// carry a deadline, and whatsmeow bounds a frame write by the socket's own life
	// rather than by the context it is given -- so on a connection that authenticated
	// and then stopped taking bytes, this would hold the right to the wire for as long
	// as the process runs, and every `presence.set` after it with the same.
	//
	// It bounds the wait for that right and the write once whatsmeow has its socket
	// lock, and not the wait for the lock itself: `NoiseSocket.SendFrame` takes it
	// before it reads the context, and a mutex cannot be given a deadline. A write
	// already stuck behind that lock outlives this, and nothing at this layer can reach
	// it. That is #74, and it is the same for every node this connector writes.
	presenceWait time.Duration

	// presenceWrite is the right to have a presence node on the wire, and there is one.
	// A channel rather than a mutex because both holders have a deadline to keep and a
	// mutex cannot be given one: `presence.set` runs on the session's executor, so a
	// command parked on an uncancellable lock is every command behind it parked too.
	//
	// What it buys is that the two writers cannot interleave. Each reads the remembered
	// availability after taking this and writes it before letting go, so whichever goes
	// last leaves WhatsApp holding the same state this session remembers -- where
	// unsynchronised, a reapplication that read before a `presence.set` can land after
	// it, and the account is left as the state its client had just replaced.
	presenceWrite chan struct{}
	// transitions counts the writes to `connected`, which is what a presence that failed
	// to publish is checked against before it is given another go. Everything presence
	// describes belongs to the socket that reported it -- WhatsApp forgets subscriptions
	// when a connection goes and nothing replays them -- so a state re-asserted across
	// one of these is a fact nothing warrants any more, put back on top of a client that
	// cleared presence when it saw the session go. This account's own availability is
	// the one thing here that is put back, and `reapplyAvailability` is where; it is
	// this session's own word rather than a report about somebody else.
	//
	// The connection flag and not the events, because they are not the same set: an
	// ordinary drop publishes `session.state`, while a stream replaced, a ban, an
	// outdated client and a connect failure each publish an event of their own and no
	// state. Counting event types would have to name all six and would miss the seventh
	// somebody adds. `setConnected` and `offline` are the two functions that own the
	// flag, and every one of those paths goes through one of them.
	transitions atomic.Int64
	// connectedAt is the earliest moment the socket this session is on could have come
	// up, which is what tells a keepalive timeout about the current connection from one
	// about a connection that is already gone. Written under mu beside `connected`, by
	// every path that can tell the socket is a new one: the dials this process asks for,
	// the reconnects it watches whatsmeow start, and a connection that announces itself
	// while the session still believes it is on the previous one.
	//
	// Those three are all of them. whatsmeow dispatches `events.Connected` from exactly
	// one place, once per authenticated socket, so a socket that reaches this session at
	// all reaches it through the third even when the first two miss it -- which is what
	// keeps this from being a list of library paths to keep up with.
	connectedAt time.Time

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

	// reuploadWait is the longest this session waits for a sender's phone to say it has
	// uploaded a file again. A field for the same reason as the waits above it, and for
	// no other.
	//
	// A ceiling and not the wait itself: the caller's own deadline still cuts it short.
	// It exists because the contract makes a command's `deadline` optional, and one that
	// omits it runs on the session's lifetime -- so a phone that is switched off would
	// hold the executor, and every command behind it, until the session closed.
	reuploadWait time.Duration

	// reuploads holds a channel per message whose file this session has asked the
	// sender's phone to upload again, so the answer reaches the command that is waiting
	// for it. Keyed by message id, which is what the phone answers under.
	//
	// It exists because the two halves are on different goroutines and neither owns the
	// other: the request goes out from the session's executor, under a command's own
	// deadline, and the answer arrives on whatsmeow's node handler. There is nothing to
	// return it to except a place the asker left for it.
	reuploads   map[string]chan *waEvents.MediaRetry
	reuploadsMu sync.Mutex

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

	// window is told when a message is found unreadable and given one, and again when
	// that window is decided, so both ends of a recovery are reported from the path that
	// performs it. A seam because neither instant reaches anything else: the placeholder
	// row carries the first and is deleted at the second, so anything watching from
	// outside is racing a row that lives about a second.
	//
	// Read under the mutex like every other field here, because it is called from
	// whatsmeow's event goroutine while a test may still be setting it. Nil is the real
	// one.
	window func(messageID string, learnedAt int64, opened bool)

	// groupMode is how a group addresses its members, which decides the namespace a
	// message key in it names a sender by. A field because reading it is a round trip to
	// WhatsApp, and a test cannot otherwise reach either branch of what depends on it.
	//
	// The second return says the answer came out of what was remembered rather than off
	// the wire, which is what makes it stale-able and is the only reason to ask twice.
	groupMode func(context.Context, waTypes.JID) (mode waTypes.AddressingMode, remembered bool, err error)

	// groupModes remembers what the round trip above answered, per group, for as long as
	// the session is up. Written under the mutex; never held across the round trip that
	// fills it, so two callers can ask about the same group at once and the second
	// overwrites the first with the same answer.
	//
	// Bounded by the groups the account is in, which is a number a phone also holds.
	groupModes map[waTypes.JID]waTypes.AddressingMode

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
	// running is how many commands are inside Execute. The keepalive handler reads it
	// before taking a mute socket down, because `ResetConnection` clears whatsmeow's
	// response waiters and `sendIQ` answers a disconnect node by resending the identical
	// frame, same stanza id (`request.go:166`). WhatsApp does not deduplicate an IQ across
	// connections: measured, a `group.create` caught by that resend leaves the account with
	// two groups, and the caller is told about the second one only.
	running int
	// owed is a socket takedown the keepalive handler decided on and could not perform,
	// because of the above. It runs when the command in flight is answered.
	owed *owedReset
	// dropAnnounced is a drop this session brought on itself and has already published. Put
	// up by the handler that causes the reset, taken down by the next `Disconnected` or by
	// the reset itself when it finds nothing to take down.
	//
	// It names no connection, which bounds what it can do: it suppresses the next
	// `Disconnected` handled, whichever that turns out to be. Telling two drops apart would
	// need a connection identity the event does not carry and whatsmeow does not expose,
	// which is #179.
	//
	// Nothing retires it early, and that is a decision rather than an omission. Every rule
	// for retiring it is a guess about whether a drop is still coming, and the two guesses
	// fail in opposite directions. Guess wrong towards keeping it and one future drop is
	// swallowed: the session reports `open` over a socket on the floor until whatsmeow's own
	// reconnect announces itself, which it always starts on the same branch that dispatched
	// the drop. Guess wrong towards dropping it and a late `Disconnected` writes
	// `reconnecting` over a healthy replacement, and nothing after it says otherwise --
	// commands refused until an operator reconnects the session by hand. One self-corrects
	// and the other does not, so this keeps the mark.
	// The reset the keepalive handler performs leads to a `Disconnected` dispatched from a
	// goroutine of its own, and whatsmeow starts the reconnect from the same instant, so
	// the two race: a `Connected` handled first leaves the late `Disconnected` writing
	// `reconnecting` over a socket that is up and healthy, and nothing after it corrects
	// that -- the replacement is fine, so it produces no further event, and every command
	// is refused from then on. Consumed by the next `Disconnected`, and dropped on a fresh
	// dial so a reset that produced no event at all cannot leave it standing.
	dropAnnounced bool
	// keepAliveAnsweredAt is when the socket was last seen answering a ping, which is what
	// tells a timeout that still describes the present from one whose run of failures is
	// already over. Written under mu beside `connected`.
	keepAliveAnsweredAt time.Time
	// hungUp is an explicit disconnect this session performed and has not been asked to
	// undo. whatsmeow's own reconnect can already be past its wait when that lands, and
	// it then opens a socket nobody asked for, after the command has answered `close`.
	hungUp bool
	// terminal is whatsmeow having put this socket down for good: a temporary ban, a
	// build WhatsApp will not talk to, a connect it refused. Read by whatever publishes
	// the session's last event, because that is the one that has to hand the lease back
	// -- an outcome reaches this session twice when a pairing is running, once through
	// the handler and once through the QR channel, and only the second of the two knows
	// it is last. Cleared with the guard beside it, on the next connect.
	terminal bool
	// givenUp counts the times this session has been given up on, and never goes back:
	// the connector compares the count an emission was made under with the one standing
	// when it is read, and two givings-up either side of a retry have to be told apart.
	givenUp uint64
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

	// revoked is the account being gone, which is not the same as stale: stale is a
	// client nothing works on, and the next connect repairs it by forgetting the device
	// and rebuilding. Fusing the two would have a connect arriving during a logout run
	// that cleanup a second time, alongside the one the logout is already running.
	revoked bool

	// pushName and businessName are this account's own display names. They live here
	// rather than being read off `client.Store` where they are wanted, because whatsmeow
	// writes those fields from its own goroutines: the copy is taken where an ordering
	// exists -- building a client nothing else holds yet, and the events that announce
	// each change -- and read from here under the lock like every other session field.
	pushName     string
	businessName string
	// pushUnfiled says the push name in hand has not made it into the contact table, so
	// the row is behind it. The table answers over the session everywhere else, and a
	// write that failed is exactly the case where that would answer with a name the
	// account has already left behind.
	pushUnfiled bool
	// verifiedUnfiled says the same about the verified name. whatsmeow files that one
	// itself and files it first, so the row the change arrived on is written by the time
	// the event exists; the other row is best effort and is the one a read goes to first.
	verifiedUnfiled bool
	// naming serialises the filing of the push name, which is the one session field whose
	// write reaches further than this struct. It is not `mu`: the write is a store round
	// trip, and holding the session lock across one would stall every other reader for as
	// long as the database takes.
	naming sync.Mutex
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
	sid string, client *wm.Client, scoped *store.Scoped,
	blobs MediaOptions, log zerolog.Logger, wa waLog.Logger,
) *Session {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Session{
		sid:        sid,
		aliases:    newAlias(),
		store:      scoped,
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
		download: func(ctx context.Context, client *wm.Client, part wm.DownloadableMessage, file media.File) error {
			return client.DownloadToFile(ctx, part, file) //nolint:wrapcheck // classified by downloadFailure, which needs the sentinels
		},
		retrieve:     retrieveOverHTTP,
		uploadFile:   uploadOverClient,
		sendAppState: sendAppStateOverClient,
		groupInfo:    groupInfoOverClient,
		joinedGroups: joinedGroupsOverClient,
		createTheGroup: func(
			ctx context.Context, client *wm.Client, req wm.ReqCreateGroup,
		) (*waTypes.GroupInfo, error) {
			return client.CreateGroup(ctx, req) //nolint:wrapcheck // classified by contactFailure, which needs the sentinels
		},
		onWhatsApp: func(ctx context.Context, client *wm.Client, phones []string) ([]waTypes.IsOnWhatsAppResponse, error) {
			return client.IsOnWhatsApp(ctx, phones) //nolint:wrapcheck // wrapped by its caller
		},
		updateParticipants: func(
			ctx context.Context, client *wm.Client, group waTypes.JID,
			participants []waTypes.JID, action wm.ParticipantChange,
		) ([]waTypes.GroupParticipant, error) {
			return client.UpdateGroupParticipants(ctx, group, participants, action) //nolint:wrapcheck // classified by contactFailure, which needs the sentinels
		},
		inviteLink: func(ctx context.Context, client *wm.Client, group waTypes.JID, revoke bool) (string, error) {
			return client.GetGroupInviteLink(ctx, group, revoke) //nolint:wrapcheck // the sentinels are read by inviteFailure
		},
		joinRequests: func(
			ctx context.Context, client *wm.Client, group waTypes.JID,
		) ([]waTypes.GroupParticipantRequest, error) {
			return client.GetGroupRequestParticipants(ctx, group) //nolint:wrapcheck // classified by contactFailure, which needs the sentinels
		},
		decideJoinRequests: func(
			ctx context.Context, client *wm.Client, group waTypes.JID,
			participants []waTypes.JID, action wm.ParticipantRequestChange,
		) ([]waTypes.GroupParticipant, error) {
			return client.UpdateGroupRequestParticipants(ctx, group, participants, action) //nolint:wrapcheck // classified by contactFailure, which needs the sentinels
		},
		leave: func(ctx context.Context, client *wm.Client, group waTypes.JID) error {
			return client.LeaveGroup(ctx, group) //nolint:wrapcheck // classified by contactFailure, which needs the sentinels
		},
		setPhoto: func(ctx context.Context, client *wm.Client, group waTypes.JID, picture []byte) error {
			_, err := client.SetGroupPhoto(ctx, group, picture)
			return err //nolint:wrapcheck // classified by contactFailure, which needs the sentinels
		},
		setName: func(ctx context.Context, client *wm.Client, group waTypes.JID, subject string) error {
			return client.SetGroupName(ctx, group, subject) //nolint:wrapcheck // classified by contactFailure, which needs the sentinels
		},
		setAnnounce: func(ctx context.Context, client *wm.Client, group waTypes.JID, on bool) error {
			return client.SetGroupAnnounce(ctx, group, on) //nolint:wrapcheck // classified by contactFailure, which needs the sentinels
		},
		setLocked: func(ctx context.Context, client *wm.Client, group waTypes.JID, on bool) error {
			return client.SetGroupLocked(ctx, group, on) //nolint:wrapcheck // classified by contactFailure, which needs the sentinels
		},
		setJoinApproval: func(ctx context.Context, client *wm.Client, group waTypes.JID, on bool) error {
			return client.SetGroupJoinApprovalMode(ctx, group, on) //nolint:wrapcheck // classified by contactFailure, which needs the sentinels
		},
		setAddMode: func(
			ctx context.Context, client *wm.Client, group waTypes.JID, mode waTypes.GroupMemberAddMode,
		) error {
			return client.SetGroupMemberAddMode(ctx, group, mode) //nolint:wrapcheck // classified by contactFailure, which needs the sentinels
		},
		profilePicture: func(
			ctx context.Context, client *wm.Client, party waTypes.JID, params *wm.GetProfilePictureParams,
		) (*waTypes.ProfilePictureInfo, error) {
			return client.GetProfilePictureInfo(ctx, party, params) //nolint:wrapcheck // the sentinels are read by its caller
		},
		askReupload: func(ctx context.Context, client *wm.Client, info *waTypes.MessageInfo, key []byte) error {
			return client.SendMediaRetryReceipt(ctx, info, key) //nolint:wrapcheck // wrapped by its caller
		},
		sendPresence: func(ctx context.Context, client *wm.Client, state waTypes.Presence) error {
			return client.SendPresence(ctx, state) //nolint:wrapcheck // classified by presenceFailure, which needs the sentinels
		},
		sendLimit: cmp.Or(blobs.SendMax, media.DefaultSendMax),
		blobs:     blobs.Blobs,
		blobBase:  blobs.BaseURL,

		storeLimit:  bindTimeout,
		deliverWait: deliverTimeout,
		handoffWait: perishableHandoff,
		awaited:     make(map[string]*awaiting),

		reuploads:      make(map[string]chan *waEvents.MediaRetry),
		reuploadWait:   reuploadTimeout,
		rerequestWait:  rerequestTimeout,
		rerequestRetry: rerequestRetry,
		presenceWrite:  make(chan struct{}, 1),
		presenceWait:   presenceWriteTimeout,
		board:          make(map[string]posted),
		downloadWait:   downloadTimeout,
		uploadWait:     uploadTimeout,
	}
	s.setTopic = func(
		ctx context.Context, client *wm.Client, group waTypes.JID, previous, revision, description string,
	) error {
		return client.SetGroupTopic(ctx, group, previous, revision, description) //nolint:wrapcheck // classified by contactFailure, which needs the sentinels
	}
	// Assigned after the literal, not in it: this one reads the group before it writes,
	// and it reads it through the seam beside it rather than off the client, so a test
	// can make the lookup fail the way a disconnection does.
	s.setDescription = func(
		ctx context.Context, client *wm.Client, group waTypes.JID, description, revision string,
	) error {
		return s.writeTheDescription(ctx, client, group, description, revision)
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
// account is what a device record says about the account on it.
type account struct {
	phone        string
	lid          string
	pushName     string
	businessName string
}

// addressesOf is the pair of names WhatsApp addresses an account by.
//
// Its own reader, because the display names next to them on the device record are written
// by whatsmeow's app-state goroutine: reading a field and discarding it is the same race
// as reading it and using it, so a path that only wants the addresses must not go past
// them.
func addressesOf(client *wm.Client) account {
	var named account
	if id := client.Store.ID; id != nil {
		named.phone = id.User
	}
	if stored := client.Store.LID; !stored.IsEmpty() {
		named.lid = stored.User
	}
	return named
}

// identityOf is addressesOf plus the display names, and it is only safe where nothing
// else holds the client yet.
func identityOf(client *wm.Client) account {
	named := addressesOf(client)
	named.pushName = client.Store.PushName
	named.businessName = client.Store.BusinessName
	return named
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

	// Read here and not later: this client was built for this session and nothing else
	// holds it yet, so whatsmeow's own goroutines are not writing to it.
	named := identityOf(client)

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
	// A mark about the client being replaced describes a drop from a socket this session no
	// longer holds, and the argument for keeping one otherwise does not survive here: it
	// rests on whatsmeow announcing its own reconnect, and a client adopted after a logout
	// has no device to reconnect with. A drop swallowed during the pairing that follows is
	// swallowed for good.
	s.dropAnnounced = false
	s.handlerID = handlerID
	s.phone = named.phone
	s.lid = named.lid
	// A rebuilt client brings the device record's copy of these names back, and the marker
	// that a row would not take one survives only where it still describes that row: the
	// name coming back has to be the one the row refused, or nothing here knows anything
	// about what the table is holding.
	s.pushUnfiled = s.pushUnfiled && s.pushName == named.pushName
	s.verifiedUnfiled = s.verifiedUnfiled && s.businessName == named.businessName
	s.pushName = named.pushName
	s.businessName = named.businessName
	s.stale = false
	s.revoked = false
	s.connected = false
	// Under the same lock as the swap. A new client is a new device store, and the alias
	// cache mirrors a table in the one it replaces -- rebuilding happens on a logout, and
	// what is paired after it may be another account entirely. Cleared after the swap was
	// published, there is an instant where a reader sees the new account and the old
	// cache. The order is safe: nothing takes the session lock while holding the alias
	// one, so the two are only ever acquired this way round.
	s.aliases.forget()
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
	at := s.now()
	s.mu.Lock()
	s.dialing = dialing
	if dialing {
		// Dated from the attempt and not from the authentication that follows it. What
		// this stamp is compared against is whatsmeow's keepalive clock, and that starts
		// with the socket: its loop dates its first "last answered" from the moment the
		// connection is up, while `Connected` waits for prekeys and the passive switch
		// after it. A stamp taken there can be seconds or tens of seconds later than the
		// clock it is compared against, and every timeout on that socket would then read
		// as one about an older connection.
		s.connectedAt = at
	}
	s.mu.Unlock()
}

// setConnected dates the connection from the moment this session found out about it,
// which for everything that is not an event is now.
func (s *Session) setConnected(connected bool) {
	s.setConnectedAt(connected, s.now())
}

// setConnectedAt is setConnected with that moment handed in, for the one caller that
// learns it well before it can write it down. `Connected` is handled under the transition
// lock, and whichever arm holds that lock may be waiting on a publish into a full inbox:
// an instant read here would date the socket from whenever the lock came free rather than
// from when the session heard about it, and a stamp more than `keepAliveStaleAfter` past
// the new keepalive loop makes every real timeout on that socket read as stale.
func (s *Session) setConnectedAt(connected bool, at time.Time) {
	s.mu.Lock()
	replaced := connected && s.connected
	s.transitions.Add(1)
	s.connected = connected
	// Either way the dial is over: whatsmeow has answered for it, with an
	// authenticated session or with the socket going down again.
	s.dialing = false
	if connected {
		if replaced {
			// A socket announcing itself while the session still believes it is on one can
			// only be a socket that replaced the previous one without anything telling this
			// session so. whatsmeow's 515 path does exactly that: it disconnects and
			// reconnects inside itself, and the disconnect it marks as expected publishes no
			// event, so nothing before this moment is observable from here. Keeping the
			// stamp of the socket that is gone would make every timeout its dead keepalive
			// loop dispatches read as current, and the healthy replacement would be taken
			// down for them.
			//
			// Later than the socket it dates, and that is the wrong direction to be wrong in:
			// a stamp more than `keepAliveStaleAfter` past the new loop's first tick makes
			// real timeouts on this socket read as stale and leaves it to whatsmeow's own
			// three minutes, which is where main already is.
			//
			// Two things make up that gap. The wait for the transition lock has no bound at
			// all, which is why the instant is handed in rather than read here. The rest is
			// whatsmeow's, and it is bigger than a handshake: the prekey count, the prekey
			// upload and the passive IQ all run before `Connected` is dispatched, up to four
			// round trips at `defaultRequestTimeout` each. `Client.LastSuccessfulConnect` is
			// set before those and would cut it to one, but it is an unsynchronised field
			// and the next connection writes it on this very path, so reading it would trade
			// the window for a race. That residue is issue #181.
			s.connectedAt = at
		}
		s.reconnecting = false
		// A new socket is a new answer about every group. What was remembered outlives a
		// disconnection, and so does whatsmeow's own cache of the same groups -- which
		// nothing clears on connect and which `sendGroup` encrypts to, member list and
		// all. Membership that changed while the account was offline reaches neither, and
		// whatsmeow only notices after the server disagrees with the participant hash it
		// sent under, by which point that message has already gone out to the old list.
		//
		// Forgetting here puts the first group action after a reconnect back on
		// `GetGroupInfo`, which refills both caches. One round trip per group per
		// connection, and the ones after it still cost nothing.
		clear(s.groupModes)
	}
	s.mu.Unlock()
}

// setReconnecting also re-dates the connection, because this is the other way a session
// gets a new socket: `dial` is only the ones this process asks for, and whatsmeow redials
// on its own after a drop, straight into its own `connect` without passing through here.
// A stamp left behind from the socket that dropped describes a connection that is gone, so
// every timeout dispatched by its keepalive loop -- and those come from goroutines of their
// own, outliving the socket they are about -- would read as current and take down the
// replacement. The moment the retry starts is the latest instant that is still earlier than
// any socket it can produce, which is what this comparison needs it to be.
func (s *Session) setReconnecting(reconnecting bool, at time.Time) {
	s.mu.Lock()
	s.reconnecting = reconnecting
	if reconnecting {
		s.connectedAt = at
	}
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
	s.owed = nil
	// And the mark that went with it. This is the session saying the connection is over and
	// nothing is coming back on its own, so no `Disconnected` is owed to it -- an explicit
	// disconnect is marked expected inside whatsmeow and publishes none, and a remote drop
	// that raced the hang-up is answered by the guard the hang-up itself raises.
	s.dropAnnounced = false
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

// lastKnownAlive is the latest moment this session has evidence the socket it is on was
// alive, and the zero time when it is not on one. Two things say so and the later of them
// wins: the connection being dated, and the socket answering a ping after having stopped.
func (s *Session) lastKnownAlive() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.connected {
		return time.Time{}
	}
	if s.keepAliveAnsweredAt.After(s.connectedAt) {
		return s.keepAliveAnsweredAt
	}
	return s.connectedAt
}

// now is this session's clock: the real one, or the one a test drives.
func (s *Session) now() time.Time {
	if s.wallClock != nil {
		return s.wallClock()
	}
	return time.Now()
}

// learned is the moment an event says the session found out about the thing it reports,
// which is what the frame's `ts` carries.
func (s *Session) learned() int64 {
	return s.now().UnixMilli()
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

// reportWindow tells the seam, if there is one, under the lock that guards it.
func (s *Session) reportWindow(messageID string, learnedAt int64, opened bool) {
	s.mu.Lock()
	report := s.window
	s.mu.Unlock()
	if report != nil {
		report(messageID, learnedAt, opened)
	}
}

func (s *Session) setIdentity(phone, lid string) {
	s.mu.Lock()
	s.phone = phone
	s.lid = lid
	s.mu.Unlock()
}

// isSelf reports whether a JID names the account this session is paired with.
//
// Namespace and digits both, because the digits alone are not an identity: a LID and a
// phone number are two numbers drawn from two spaces, and nothing stops one account's LID
// reading like another account's number. Matching on digits would then take a stranger's
// name for this account's own.
func (s *Session) isSelf(jid waTypes.JID) bool {
	address, named := addressOf(jid)
	if !named {
		return false
	}
	phone, lid := s.identity()
	switch address.Kind {
	case protocol.AddressPhone:
		return phone != "" && address.ID == phone
	case protocol.AddressLID:
		return lid != "" && address.ID == lid
	default:
		return false
	}
}

// setVerifiedName records the name a business account is verified under.
func (s *Session) setVerifiedName(businessName string) {
	s.mu.Lock()
	s.businessName = businessName
	s.mu.Unlock()
}

// reverify records a verified name the account changed while the session was up.
//
// whatsmeow files this one itself, and the event is proof that it filed it: the row under
// the address the change arrived on took the write, or `updateBusinessName` would have
// returned before dispatching. The other row is where that stops being true -- it resolves
// the alternate address afterwards and logs a failure there rather than reporting it --
// and when the change arrives on the LID, the row left behind is the one a read goes to
// first.
func (s *Session) reverify(businessName string) {
	if businessName == "" {
		return
	}
	s.mu.Lock()
	s.businessName = businessName
	s.verifiedUnfiled = true
	s.mu.Unlock()

	s.recordOwnVerifiedName(businessName)
}

// rename records a push name the account changed while the session was up.
func (s *Session) rename(pushName string) {
	if pushName == "" {
		return
	}
	s.mu.Lock()
	s.pushName = pushName
	s.pushUnfiled = true
	s.mu.Unlock()

	// Written to the contact table as well, which is the only thing that keeps the two
	// copies of this name from disagreeing after a restart. A rename arrives two ways and
	// each writes one of them: an app-state sync writes the device record, and the notify
	// on a message the account sent writes the table. Neither writes the other, so a
	// session rebuilt from the record has no way to tell which of the two it is holding.
	// Writing here makes the table the one that is never behind.
	s.recordOwnName(pushName)
}

// recordOwnName files the account's own push name where the people it has met are filed.
//
// A failure is logged rather than retried: the name is an annotation and the session's own
// copy is already current. What it costs is that the row is behind until the next rename,
// which is why the session remembers that it is.
func (s *Session) recordOwnName(pushName string) {
	client := s.current()
	if client == nil || client.Store == nil || client.Store.Contacts == nil {
		return
	}
	// One filing at a time, and only for the name the session is still holding. Two
	// renames can be in flight on different goroutines -- an app-state sync dispatches on
	// one of its own, the notify on a message the account sent on the one that read it --
	// and without this the older write can land after the newer one and leave the table
	// holding a name the session has already stopped saying is unfiled.
	s.naming.Lock()
	defer s.naming.Unlock()
	if s.names().push != pushName {
		return
	}
	if !s.fileOwnName(client.Store.Contacts.PutPushName, pushName, "push name") {
		return
	}
	s.mu.Lock()
	// Only while the name is still the one that was written: a rename that landed during
	// this has a write of its own behind it.
	if s.pushName == pushName {
		s.pushUnfiled = false
	}
	s.mu.Unlock()
}

// recordOwnVerifiedName files the account's own verified name under the address whatsmeow
// may have left without it.
func (s *Session) recordOwnVerifiedName(businessName string) {
	client := s.current()
	if client == nil || client.Store == nil || client.Store.Contacts == nil {
		return
	}
	s.naming.Lock()
	defer s.naming.Unlock()
	if s.names().verified != businessName {
		return
	}
	if !s.fileOwnName(client.Store.Contacts.PutBusinessName, businessName, "verified name") {
		return
	}
	s.mu.Lock()
	if s.businessName == businessName {
		s.verifiedUnfiled = false
	}
	s.mu.Unlock()
}

// fileOwnName writes one of the account's own display names under every address the
// account answers under, and says whether every one of them took it.
//
// All of them, because a read takes the first row that holds a name and the phone row is
// read first: one row left behind is enough to answer with a name the account has left.
func (s *Session) fileOwnName(
	put func(context.Context, waTypes.JID, string) (bool, string, error),
	name, what string,
) bool {
	phone, lid := s.identity()
	writing, done := context.WithTimeout(s.ctx, s.storeLimit)
	defer done()
	filed, missed := false, false
	for _, address := range []protocol.Address{
		{Kind: protocol.AddressPhone, ID: phone},
		{Kind: protocol.AddressLID, ID: lid},
	} {
		if address.ID == "" {
			continue
		}
		jid, err := jidOf(address)
		if err != nil {
			missed = true
			continue
		}
		if _, _, err := put(writing, jid, name); err != nil {
			s.log.Debug().Err(err).Str("kind", string(address.Kind)).Str("name", what).
				Msg("could not file one of the account's own names")
			missed = true
			continue
		}
		filed = true
	}
	return filed && !missed
}

// selfNames is what this account calls itself: the push name every recipient sees, and the
// verified name a business account carries.
type selfNames struct {
	push string
	// pushUnfiled and verifiedUnfiled say the name beside them has not reached the contact
	// table, which is otherwise the copy that answers.
	pushUnfiled     bool
	verified        string
	verifiedUnfiled bool
}

func (s *Session) names() selfNames {
	s.mu.Lock()
	defer s.mu.Unlock()
	return selfNames{
		push:            s.pushName,
		pushUnfiled:     s.pushUnfiled,
		verified:        s.businessName,
		verifiedUnfiled: s.verifiedUnfiled,
	}
}

// relearn takes the account's own details off the client again.
//
// The LID is why. whatsmeow learns it from the connection rather than from the device it
// resumed -- `handleConnectSuccess` writes `Store.LID` and saves -- so a device stored
// before the account had one runs with none until this. Nothing else puts it back: the
// only other writer is pairing, and a resumed session never pairs.
//
// Called from the Connected handler, which is where the ordering is: whatsmeow writes the
// LID and then starts the goroutine that dispatches the event, so what this reads is what
// that write left.
//
// The addresses and nothing else, for the same reason. That ordering covers the LID write
// and covers nothing about the display names, which an app-state sync writes from its own
// goroutine and may be writing right now -- reading them here would be the data race this
// whole arrangement exists to avoid. They arrive on their own events instead.
func (s *Session) relearn(client *wm.Client) {
	if client == nil || client.Store == nil {
		return
	}
	named := addressesOf(client)
	s.mu.Lock()
	s.phone = named.phone
	s.lid = named.lid
	s.mu.Unlock()
}

// Events is the emission channel, closed once the session is done.
func (s *Session) Events() <-chan engine.Emission { return s.events }

// Connect starts pairing or resumes a stored session.
func (s *Session) Connect(ctx context.Context, req engine.ConnectRequest) error {
	s.startCommand()
	defer s.endCommand()

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

	// A pairing already in flight is the operator changing their mind, not an error: they
	// switched from the QR to a code, or hit connect again on a screen whose code had
	// visibly run out. Left standing, its socket is live and whatsmeow refuses to open a
	// second pairing channel on one -- `GetQRChannel must be called before connecting` --
	// which the caller receives as "the connector could not carry out the command", about
	// a request that was perfectly reasonable.
	//
	// Only for the two modes that need a channel of their own. A resume asks to carry on
	// with credentials that already exist, and tearing a live pairing down for one would
	// have a client's periodic reconnect cancel the operator's scan.
	if req.Pairing != "resume" {
		s.replacePairing()
	}

	if s.isStale() {
		// The device behind this client was deleted and its replacement could not be
		// built at the time -- or the pairing just replaced above took its client with
		// it. Nothing on it works, so the connect that would have failed is the connect
		// that repairs it.
		if err := s.recover(ctx); err != nil {
			return fmt.Errorf("whatsmeow: %s is still without a usable device: %w", s.sid, err)
		}
	}

	// Recorded once the request is one the session is going to act on, because the
	// client sends it on every connect and it is a property of the subscription rather
	// than of the device. A refused connect leaves the subscription alone: the caller
	// sees a failed command, and a session that had quietly turned group traffic off
	// underneath it would go on acknowledging and dropping group messages until the
	// next connect that happened to succeed.
	s.setGroups(req.Groups)

	// Recorded at the same point and for the same reason: this is where the request stops
	// being one the session might refuse. It is what makes the account survive the
	// instance running it -- a lease dies with its holder and a wake is a frame read once,
	// so without this there is nothing anywhere that says a paired, unowned account should
	// be in the air, and it stays down until somebody opens the inbox and asks again.
	//
	// Logged rather than returned. The connection is what the client asked for and it is
	// happening; a memory that could not be written is a session that will not be brought
	// back by itself later, which is worse than it was but not a reason to refuse what is
	// working now. The next connect writes it again.
	if err := s.store.PutDesired(ctx, store.DesiredConnected); err != nil {
		s.log.Warn().Err(err).Str("sid", s.sid).
			Msg("could not record that this session should be connected; it will not be resumed on its own")
	}

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
	// The guard comes down with the session's own giving-up, and that is what is handed
	// back here: a request refused below never dialled anything, and a session left
	// looking as though it had something to try is one nothing hands back.
	standing, gaveUp := s.dropHangUp()

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
		s.restoreTerminal(gaveUp)
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
	s.startCommand()
	defer s.endCommand()

	s.cancelPairing()
	// Before the socket goes down, because what this records is the answer to "should
	// anything bring it back": written after, an instance that died in between would
	// leave an account the operator turned off looking like one that should be resumed.
	//
	// Logged rather than returned, like the one in Connect: the disconnect is what was
	// asked for and it is going to happen either way. What a failure here costs is a
	// session the resume sweep may bring back, which the next disconnect corrects.
	if err := s.store.PutDesired(ctx, store.DesiredDisconnected); err != nil {
		s.log.Warn().Err(err).Str("sid", s.sid).
			Msg("could not record that this session was asked to stay down; a sweep may bring it back")
	}
	return s.hangUp(ctx, s.current())
}

// Logout ends the session on WhatsApp's side and forgets the credentials here, so the
// next connect has to pair again.
func (s *Session) Logout(ctx context.Context) error {
	s.startCommand()
	defer s.endCommand()

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

	if err := s.recoverWithin(); err != nil {
		// The client here is on a deleted device whatever happens next, and a cleanup that
		// stopped halfway leaves the mapping still pointing at it: rebuilding on top of
		// that would hand the fresh client the very credentials WhatsApp threw away, so
		// the next connect has to try the cleanup again rather than talk to this client.
		s.markStale()
		return err
	}
	return nil
}

// Delete tears the account down for a client that has already stopped addressing it.
//
// The unlink is attempted first, because a device this connector forgets while WhatsApp
// still lists it is a row on somebody's phone that nothing can ever remove: the
// credentials that would authorise the unlink are exactly what is about to be deleted.
// Trying first is the only order in which the unlink is possible at all.
//
// What is different from Logout is what happens next. Logout leaves credentials that
// still resume when the request never left; here they are deleted anyway. The caller
// destroyed the inbox before sending this, so there is no operator left to spare a
// fresh pairing, and what keeping them buys is a session this connector goes on
// adopting, reconnecting and publishing events for, addressed to nobody.
//
// A refused unlink is not a failed delete, and answering that it was is the mistake
// this comment exists to prevent. The two ways to get here are an account that was
// never paired, where there is nothing on WhatsApp's side to remove and nothing is
// left behind, and a paired account whose socket is down, where the device stays
// listed on the phone. Neither improves on a retry -- the second cannot, because the
// credentials a later attempt would sign with are gone by then -- so a failure
// answered here is a command the client republishes forever over a teardown that
// already happened. What the second one leaves is named in the log instead.
func (s *Session) Delete(ctx context.Context) error {
	s.startCommand()
	defer s.endCommand()

	s.cancelPairing()
	_, paired, pairedErr := s.store.JID(ctx)
	unlink := s.logout(ctx, s.current())
	// Whatever WhatsApp answered, this session is not coming back. settleLogout is what
	// keeps a reconnect from dialling on credentials that are about to be gone.
	s.settleLogout()
	switch {
	case unlink == nil:
	case pairedErr == nil && !paired:
		// Nothing was linked, so there is nothing WhatsApp has to be told about and no
		// residue to name. The commonest way here is the redelivery of a delete that
		// already ran.
		s.log.Debug().Str("sid", s.sid).
			Msg("nothing was linked for this session, so the teardown had no device to unlink")
	default:
		// The one case that leaves something behind, and the operator is the only one who
		// can act on it: the device goes on being listed on the phone until they remove
		// it there. Logged rather than returned, because returning it asks the client to
		// retry a teardown that is finished and an unlink that can never succeed again.
		s.log.Warn().Err(unlink).Str("sid", s.sid).
			Msg("the device could not be unlinked before the session was deleted; it may still be listed on the phone")
	}

	if err := s.recoverWithin(); err != nil {
		// Answered as a failure, unlike the refused unlink above, because here the retry
		// has something to do: whichever half did not land is the half the next delete
		// finishes, the credentials still sitting in the store or the client still being
		// the deleted one. Answering success is what would turn a teardown that stopped
		// halfway into the silence this exists to end.
		s.markStale()
		return fmt.Errorf("whatsmeow: delete %s: %w", s.sid, err)
	}

	// Said the way every other giving-up is said, and for the same reason: there is
	// nothing left for this session to try, so the connector hands the lease back once
	// the event is out and the account stops belonging to an instance that will not use
	// it. The number can be paired again straight away rather than after the lease
	// expires -- and a connect arriving anyway takes the mark down, which is right:
	// there are no credentials left, so what it gets is a fresh pairing.
	s.markTerminal()
	s.emitLast(protocol.EventSessionLoggedOut, map[string]any{"reason": "session_deleted"})
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
	// Revoked from here, not from the end of the cleanup after it. Forgetting the device
	// and rebuilding take a store round trip each, and until one of them lands the session
	// still holds the identity it was paired with: a command that reads local state rather
	// than the socket -- `contact.resolve` is the one -- would answer for an account
	// WhatsApp has already taken away. `adopt` clears it again when a fresh client
	// arrives, which is the only thing that makes the session paired once more.
	//
	// Its own flag rather than `stale`, which reads as "this client needs repairing" and
	// is what a connect acts on: a connect arriving here would then run the forget and
	// the rebuild a second time, next to the ones the logout is already running, and the
	// two would take each other's client out from under a pairing.
	s.setRevoked()
}

// dropHangUp takes the guard down and answers with the state as it stood at that moment.
//
// One step, under transition, because a caller that drops the guard and then asks
// separately can be answered by the very event the guard was there to refuse. Held
// across both, a Connected the library had queued for the old socket is either handled
// entirely before — refused, and the socket it came from closed — or entirely after,
// by which point this request has already decided to dial and the worst it costs is a
// second `open` behind the first.
func (s *Session) dropHangUp() (state string, gaveUp uint64) {
	s.transition.Lock()
	defer s.transition.Unlock()

	s.mu.Lock()
	s.hungUp = false
	// The operator is asking for a connection, which is the answer to "is there anything
	// left to try". Left standing, it would retire the session on the next pairing that
	// merely ran out of codes.
	//
	// Handed back rather than read separately by the caller: the branch that gives up
	// takes the same lock, so a giving-up that lands either side of this is either taken
	// down here and reported, or not taken down at all. Read before, one landing in
	// between is cleared by this and reported to nobody -- and a connect refused before it
	// dialled would then have nothing to put back.
	if s.terminal {
		gaveUp = s.givenUp
		s.terminal = false
	}
	s.mu.Unlock()
	return s.state(), gaveUp
}

// owedReset is a takedown waiting for the command in flight to be answered.
type owedReset struct {
	client *wm.Client
	judged int64
}

func (s *Session) startCommand() {
	s.mu.Lock()
	s.running++
	s.mu.Unlock()
}

// endCommand releases the takedown the keepalive handler left waiting, if this was the
// command it was waiting for.
func (s *Session) endCommand() {
	s.mu.Lock()
	s.running--
	owed := s.owed
	if s.running > 0 {
		owed = nil
	} else {
		s.owed = nil
	}
	s.mu.Unlock()
	if owed != nil {
		go s.resetUnlessReplaced(owed.client, owed.judged)
	}
}

// takeDownSoon takes the socket down, or writes down that it has to come down as soon as
// whatever is waiting on WhatsApp has been answered.
//
// Taking it down under an in-flight command is what turns one write into two. The state is
// already `reconnecting` and the client already told, so nothing new is accepted while this
// waits, and what it waits for is bounded: `sendIQ` answers within its own 75s ceiling, the
// one main reaches first and this design otherwise gets in front of.
func (s *Session) takeDownSoon(client *wm.Client, judged int64) {
	s.mu.Lock()
	waiting := s.running > 0
	if waiting {
		s.owed = &owedReset{client: client, judged: judged}
	}
	s.mu.Unlock()
	if waiting {
		s.log.Info().Msg("a command is still waiting on WhatsApp; the mute socket comes down with its answer")
		return
	}
	go s.resetUnlessReplaced(client, judged)
}

// resetUnlessReplaced takes the socket down unless a connection landed while this was
// waiting to be scheduled.
//
// `ResetConnection` reads the client's socket at the moment it runs, so a goroutine that
// sat while whatsmeow replaced the socket on its own would close the replacement. A
// connection write since the judgement is what says that happened, and it is also what put
// the state back to `open`, so standing down here leaves nothing to repair.
//
// Neither question is asked under the same lock as the reset itself, which leaves the width
// of those statements as a window: a connection or a command that lands inside it is not
// seen. Holding a lock across the reset would close that and reopen worse -- `ResetConnection`
// blocks on the close handshake holding whatsmeow's socket lock, and the session's own mutex
// is taken by every read of its state.
func (s *Session) resetUnlessReplaced(client *wm.Client, judged int64) {
	s.mu.Lock()
	replaced := s.transitions.Load() != judged
	// A command that started between the decision and this goroutine being scheduled. Only
	// the lifecycle three can: everything else is refused at the gate by then. `logout`
	// sends its removal IQ over the socket that is still up, and cutting that off is the
	// resend this whole guard exists to avoid, so the takedown goes back to waiting.
	started := !replaced && s.running > 0
	if started {
		s.owed = &owedReset{client: client, judged: judged}
	}
	s.mu.Unlock()
	if started {
		s.log.Info().Msg("a command started before the mute socket could be taken down; waiting for its answer")
		return
	}
	if replaced {
		s.log.Info().Msg("a connection landed before the mute socket could be taken down; leaving it alone")
		return
	}
	// Asked here and not in the handler, because reading it takes whatsmeow's socket lock
	// and a redial holds that for the length of an attempt: off the handler's goroutine
	// that is a wait, on it that would be the session's whole state machine behind a
	// redial. It is the precondition for the reset producing anything at all -- with no
	// socket under it, `ResetConnection` returns having done nothing.
	if !client.IsConnected() {
		s.log.Info().Msg("the mute socket was already gone before it could be taken down")
		return
	}
	client.ResetConnection()
}

// forgetOwedReset drops a takedown whose socket is already gone, without any of what
// cancelling one means: nothing recovered, so nothing is published and the mark stands for
// the drop that is being handled.
func (s *Session) forgetOwedReset() {
	s.mu.Lock()
	s.owed = nil
	s.mu.Unlock()
}

// cancelOwedReset drops a takedown that was still waiting for a command to be answered, and
// reports whether the connection it was judged on is the one that recovered.
//
// Only that one cancels it, and the mark is why. A recovery about a socket that has since
// been replaced -- the replacement's `Connected` handled first, which moves the count -- has
// nothing to bring back, and the `Disconnected` of the socket it describes is still on its
// way: that drop is what the mark was raised for and what it has to swallow. Taking the mark
// down on a recovery that arrived too late is how the drop of the old socket gets written
// over the healthy new one, with nothing after it to put that right.
//
// The takedown goes either way. Its socket is gone, and `resetUnlessReplaced` would find the
// count moved and leave it alone in any case.
func (s *Session) cancelOwedReset() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.owed == nil {
		return false
	}
	superseded := s.owed.judged != s.transitions.Load()
	s.owed = nil
	if superseded {
		return false
	}
	s.dropAnnounced = false
	return true
}

// recovered puts the session back on the socket it had given up on. Not `setConnected`,
// because this is the same connection and not a new one: what was remembered about it,
// group modes included, still describes it.
func (s *Session) recovered() {
	s.mu.Lock()
	s.transitions.Add(1)
	s.connected = true
	s.reconnecting = false
	s.mu.Unlock()
}

// announceDrop marks the `Disconnected` this session's own reset is about to produce as
// one that has already been published. The caller holds transition: this is one half of a
// socket transition, not one of its own.
func (s *Session) announceDrop() {
	s.mu.Lock()
	s.dropAnnounced = true
	s.mu.Unlock()
}

// dropWasAnnounced reports whether the drop being handled is the one this session brought
// on itself, clearing the mark either way: one reset is answered once.
func (s *Session) dropWasAnnounced() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	announced := s.dropAnnounced
	s.dropAnnounced = false
	return announced
}

// answeredKeepAlive records that the socket answered a ping again.
//
// Unconditional, and the date is what makes that safe: `at` is when whatsmeow dispatched the
// recovery, not when this session got round to it, so a recovery about a socket that has
// since been replaced carries an instant from before the replacement was dated. The rule
// that reads this takes the later of the two, so the superseded one loses.
func (s *Session) answeredKeepAlive(at time.Time) {
	s.mu.Lock()
	s.keepAliveAnsweredAt = at
	s.mu.Unlock()
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

// markTerminal records that whatsmeow will not bring this connection back on its own.
func (s *Session) markTerminal() {
	s.mu.Lock()
	if !s.terminal {
		s.terminal = true
		s.givenUp++
	}
	s.mu.Unlock()
}

// Finished names the giving-up this session is on, for the connector to compare against
// the one an emission was made under. Zero while there is still something to try: a
// connect that ran after the emission was queued clears it, and an account whose socket
// is back up is not one to hand over.
func (s *Session) Finished() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.terminal {
		return 0
	}
	return s.givenUp
}

// restoreTerminal puts back a giving-up that a connect took down and then did not act on.
//
// The same one, not another: the emission reporting it is on its way with that number on
// it, and a fresh giving-up here would be a different one, which the connector reads as an
// answer about an attempt the session has moved on from. Skipped when something has moved
// on since, which has its own number and its own emission.
func (s *Session) restoreTerminal(was uint64) {
	if was == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.terminal || s.givenUp != was {
		return
	}
	s.terminal = true
}

// isTerminal reports whether there is anything left for this session to try.
func (s *Session) isTerminal() bool { return s.Finished() != 0 }

// finishing publishes an outcome the session does not come back from, and marks it as
// the session's last unless a pairing is still running.
//
// whatsmeow hands a ban and a refused connect to the QR channel as well as to the
// handler, so a pairing publishes its own end after this one -- the error and the state
// that closes it. Retiring here would hand the lease back with those still behind it in
// the queue, and a client would be left watching a pairing that never resolves. The
// pairing's closing state carries the mark instead, and reads `terminal` to know it has
// to. Whichever of the two runs second is the one that finds the other's mark.
func (s *Session) finishing(eventType protocol.EventType, payload any) {
	// Held across the question and the answer, or the pairing can end between the two and
	// publish its own closing state -- marked, because the mark above is already set --
	// ahead of the event this call has not enqueued yet. The supposedly last event would
	// then not be last, and the account can go back before the reason it went back is out.
	//
	// TryLock and not Lock: the caller holds transition, and a pairing ending takes
	// transition from under this very lock, so waiting here would be waiting on the
	// goroutine that is waiting on us. Failing to take it answers the question anyway --
	// only a pairing that is ending holds it, and it is blocked behind this transition, so
	// it publishes after this does and the mark is its to carry.
	if !s.pairingMu.TryLock() {
		s.emit(eventType, payload)
		return
	}
	defer s.pairingMu.Unlock()

	if s.pairingActive() {
		s.emit(eventType, payload)
		return
	}
	s.emitLast(eventType, payload)
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

	device, err := s.store.Device(ctx)
	if err != nil {
		return fmt.Errorf("whatsmeow: rebuild %s: %w", s.sid, err)
	}

	s.mu.Lock()
	previous, handlerID := s.client, s.handlerID
	s.mu.Unlock()
	s.detach(previous, handlerID)
	previous.Disconnect()

	// Dropped with the client it was filed under. Every path here has just forgotten the
	// device, so the account that asked for it is gone, and carried over it would mark
	// whatever pairs next available without that client ever having asked.
	s.forgetAvailability()

	// A false here is the session having closed while this ran, which adopt has already
	// cleaned up after. There is nothing left to do either way.
	_ = s.adopt(wm.NewClient(device, s.waLog))
	return nil
}

// recoverWithin is recover on a bound of its own, for the teardowns a command asked for.
//
// On the session's own lifetime rather than the command's, and that is the whole reason
// it exists. The command carries the client's ceiling, and the ceiling is about how long
// to wait on WhatsApp, not about what this process still owes its own database once the
// answer is in. An unlink that spends the budget -- the ordinary shape of a logout on a
// socket that is down -- would leave every store call after it running on a context that
// is already dead, and then nothing is cleaned up at all: the account keeps credentials
// WhatsApp has revoked, goes on being adopted and resumed on them, and the client is told
// to retry a teardown whose remote half can never succeed again.
//
// Still a short bound, and still one the session's own close ends, because a database
// that stopped answering must not be able to hold a teardown open either.
func (s *Session) recoverWithin() error {
	ctx, cancel := context.WithTimeout(s.ctx, s.storeLimit)
	defer cancel()
	return s.recover(ctx)
}

// recover puts a session that was logged out back where a fresh pairing can start:
// the revoked credentials gone, and a client that is not the deleted one.
//
// Both halves, because a cleanup that failed leaves the mapping pointing at the revoked
// device, and rebuilding from that hands the session the very credentials WhatsApp
// threw away. Doing them together is what makes the retry a retry.
func (s *Session) recover(ctx context.Context) error {
	if err := s.store.Forget(ctx); err != nil {
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

// setRevoked records that WhatsApp has taken the account away. Cleared only by adopting
// the client of a session that has one again.
func (s *Session) setRevoked() {
	s.mu.Lock()
	s.revoked = true
	s.mu.Unlock()
}

func (s *Session) isRevoked() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.revoked
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
	// Counted for the whole of it, so a socket this session decides to take down waits for
	// whatever is already out at WhatsApp. What it must not interrupt is an answer that has
	// not arrived: whatsmeow resends the frame it was cut off from, and WhatsApp applies it
	// again. The lifecycle commands count themselves, because the session layer routes
	// those to their own engine methods rather than through here.
	s.startCommand()
	defer s.endCommand()

	// Stamped here, before the command spends a round trip at WhatsApp: a pairing that
	// comes back belongs to the account that asked for it, and a logout landing in that
	// window has already rebuilt the session on another one.
	ctx = s.aliases.stamp(ctx)
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
	case protocol.CommandContactCheck:
		return s.checkContacts(ctx, command)
	case protocol.CommandContactProfilePicture:
		return s.contactPicture(ctx, command)
	case protocol.CommandContactResolve:
		return s.resolveContact(ctx, command)
	case protocol.CommandMessageMarkUnread:
		return s.markUnread(ctx, command)
	case protocol.CommandGroupLeave, protocol.CommandGroupPhotoSet, protocol.CommandGroupNameSet,
		protocol.CommandGroupDescriptionSet, protocol.CommandGroupSettingsSet,
		protocol.CommandGroupInviteGet, protocol.CommandGroupJoinRequestsList,
		protocol.CommandGroupJoinRequestsUpdate, protocol.CommandGroupCreate,
		protocol.CommandGroupList, protocol.CommandGroupInfo,
		protocol.CommandGroupParticipantsUpdate:
		return s.aboutAGroup(ctx, command)
	}
	return nil, engine.ErrNotSupported
}

// groupIQWait is how long a group command may spend at WhatsApp before this connector
// stops waiting for it.
//
// Every one of them is an info query, and whatsmeow gives an info query 75 seconds. The
// session executor is serial -- one goroutine takes one command off the queue and does not
// take the next until that one has answered -- so an info query WhatsApp decides not to
// answer does not cost the command that made it, it costs the account: every message,
// receipt and read marker queued behind it waits out the whole minute and a quarter. That
// is what #163 measured, four times, on a description that could not be removed.
//
// Fifteen seconds is far above what these actually take. Measured live on 10/09/2026: a
// group created, renamed, its settings changed, its description written and removed, all
// between 370 ms and 1.3 s, and a refusal -- a 409 over a description WhatsApp will not
// let this account replace -- in 364 ms. A query still running at fifteen seconds is not
// slow, it is one that is not coming back, and answering `timeout` then costs the caller
// one command instead of costing the account a minute of its queue.
//
// A caller that sends a shorter deadline of its own still wins: this bounds the wait, it
// does not extend one.
const groupIQWait = 15 * time.Second

// boundedGroupCommand reports whether the ceiling is safe for this command.
//
// Giving up on a wait does not undo what WhatsApp did with the request, and `carryOut`
// writes the ledger only on success, so a command answered `timeout` here and redelivered
// under the same idempotency key runs a second time. That is fine where running twice
// changes nothing and costs nothing, and it is not fine anywhere else -- invariant 5, and
// the thing a ceiling would otherwise be trading a stalled queue for. So the ceiling is
// allowed in exactly two places:
//
//   - **Reads.** `group.info`, `group.list`, `group.join_requests.list`, and a
//     `group.invite.get` that is not revoking. Asked again they answer again; the only
//     thing a truncated wait costs is the answer, which is what the caller is told.
//
// `group.description.set` is not decided here at all, and that is the one asymmetry worth
// spelling out: whether it can be bounded depends on which call it turns out to need,
// which is known only after the group has been read. `writeTheDescription` bounds the half
// that carries a revision id and leaves the legacy half alone.
//
// Everything else keeps whatsmeow's own bound, which is longer, and the account can still
// be held by one of them. That is not an oversight -- it is what is left after refusing to
// truncate a mutation nothing can reconcile, and the general answer to it is #165 rather
// than a shorter number here. `group.create` makes another group, `group.photo.set` has a
// new picture id assigned and another change announced, a revoking `group.invite.get`
// rotates the link again; `group.leave`, `group.participants.update` and
// `group.join_requests.update` answer a retry with "not in the group" or "no such request"
// for work the first attempt did; and `group.name.set` and `group.settings.set` have no id
// to write twice under, so a retry publishes a second `group.updated` for one command.
func boundedGroupCommand(command *protocol.Command) bool {
	switch command.Type {
	case protocol.CommandGroupInfo, protocol.CommandGroupList, protocol.CommandGroupJoinRequestsList:
		return true
	case protocol.CommandGroupInviteGet:
		return !command.ChangesSomething()
	}
	return false
}

// aboutAGroup carries out the group commands, under a ceiling none of the others need.
func (s *Session) aboutAGroup(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	if boundedGroupCommand(command) {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, groupIQWait)
		defer cancel()
	}

	switch command.Type {
	case protocol.CommandGroupLeave:
		return s.leaveGroup(ctx, command)
	case protocol.CommandGroupPhotoSet:
		return s.setGroupPhoto(ctx, command)
	case protocol.CommandGroupNameSet:
		return s.setGroupName(ctx, command)
	case protocol.CommandGroupDescriptionSet:
		return s.setGroupDescription(ctx, command)
	case protocol.CommandGroupSettingsSet:
		return s.setGroupSetting(ctx, command)
	case protocol.CommandGroupInviteGet:
		return s.groupInviteOf(ctx, command)
	case protocol.CommandGroupJoinRequestsList:
		return s.listJoinRequests(ctx, command)
	case protocol.CommandGroupJoinRequestsUpdate:
		return s.updateJoinRequests(ctx, command)
	case protocol.CommandGroupCreate:
		return s.createGroup(ctx, command)
	case protocol.CommandGroupList:
		return s.listGroups(ctx, command)
	case protocol.CommandGroupInfo:
		return s.groupInfoOf(ctx, command)
	case protocol.CommandGroupParticipantsUpdate:
		return s.updateGroupParticipants(ctx, command)
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
	// Inside the same transition that publishes the close, and not after it. That flag is
	// what an Open racing this reads to decide it may build the replacement, so a moment
	// where the session is closed and the fence is still up is a moment where two sessions
	// can write one device. Dropping is a single atomic store, so it costs the lock
	// nothing to hold it for.
	s.store.Drop()
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
	// A pairing is starting, which is the answer to "is there anything left to try", and
	// it is given here as well as where the guard comes down. Between those two the
	// attempt this one replaces can report a build WhatsApp will not talk to -- it holds
	// this lock to do it, so it either gets there first and is undone here, or finds this
	// run current and stands aside. Left standing, the giving-up belongs to the attempt
	// that was replaced and the account is handed over on the strength of it, however the
	// one that is running now ends.
	s.terminal = false
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

// replacePairing ends the pairing conversation this session is in, so that the one about
// to start gets a socket of its own.
//
// Quietly: no `pairing.error`, because nothing failed. The attempt is being replaced by
// its own operator, and reporting it would put a red sentence on the screen underneath
// the fresh code they are waiting for.
func (s *Session) replacePairing() {
	s.pairingMu.Lock()
	defer s.pairingMu.Unlock()

	s.mu.Lock()
	run := s.pairing
	s.pairing = nil
	s.mu.Unlock()
	if run == nil {
		return
	}
	s.tearDownPairing(run, s.current())
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
			if !s.handOn(&emission) {
				return
			}
		}
	}
}

// presenceLife is how long a moment is worth publishing for. Past it the session knows
// more than the event does: whatever came next either replaced it on the board or was
// published, and a client shown the old one has no way to learn better.
const presenceLife = 10 * time.Second

// presenceWriteTimeout bounds a presence node nobody is waiting on. Short, because the
// socket it goes out over has just finished a handshake: one that will not take forty
// bytes in this long is not one the next node is going to reach either.
const presenceWriteTimeout = 10 * time.Second

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
func (s *Session) handOn(emission *engine.Emission) bool {
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
		case s.events <- *emission:
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
	case s.events <- *emission:
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
	s.emitting(&engine.Emission{Type: eventType}, payload)
}

// emitLast is emit for a state whatsmeow does not come back from. It says so on the
// emission, so the connector hands the lease back once the event is out and the account
// stops belonging to an instance with nothing left to try.
func (s *Session) emitLast(eventType protocol.EventType, payload any) {
	s.emitting(&engine.Emission{Type: eventType, Retires: true, Attempt: s.Finished()}, payload)
}

func (s *Session) emitting(emission *engine.Emission, payload any) {
	eventType := emission.Type
	body, err := json.Marshal(payload)
	if err != nil {
		// Everything reaching this is built a few lines above, so a failure is a
		// programming error rather than something a session should carry on through.
		s.log.Error().Err(err).Str("type", string(eventType)).Msg("failed to render an event payload")
		return
	}
	emission.Payload = body
	emission.At = s.learned()
	select {
	case s.inbox <- pending{event: *emission}:
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
		s.outdatedPairing(run)
	case "timeout":
		s.finishPairing(run, "timeout", nil)
	case "error":
		s.finishPairing(run, "error", item.Error)
	default:
		s.finishPairing(run, item.Event, item.Error)
	}
}

// outdatedPairing reports a build WhatsApp will not talk to, and finishes the session on
// it.
//
// The last thing this channel carries: whatsmeow sends it from the branch that closes the
// channel, so the reader's next turn finds it shut and returns without an outcome of its
// own. There is no `pairing.error` after this and no state to close, which makes this
// event the pairing's end as much as the session's -- nothing follows it that could carry
// the mark instead.
//
// Only while this is still the attempt the session is on, and under the lock a replacement
// takes to start: the operator can have replaced it already, and marking the session
// finished with then is marking the attempt that is running now. Nothing clears that --
// the replacement's own connect came before the mark -- so a pairing that goes on to
// succeed is handed over on the strength of an answer about the attempt it replaced.
func (s *Session) outdatedPairing(run *pairingRun) {
	s.pairingMu.Lock()
	defer s.pairingMu.Unlock()

	if !s.endPairing(run) {
		s.log.Warn().Msg("WhatsApp refused a pairing for this build after the attempt was replaced")
		return
	}
	s.markTerminal()
	s.emitLast(protocol.EventSessionClientOutdated, map[string]any{})
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

	if err := s.store.Bind(ctx, jid); err != nil {
		s.log.Error().Err(err).Msg("failed to record a pairing; refusing it")
		return false
	}
	return true
}

// bindTimeout bounds the write that stands between a scanned code and a paired
// session. It is short because WhatsApp is waiting on the other side of it.
const bindTimeout = 5 * time.Second

// reuploadTimeout is the longest a caller waits on somebody else's phone. Generous,
// because that is what it is waiting for: a phone that is asleep takes a moment to wake
// up and notice. Bounded all the same, because one that is switched off never will.
const reuploadTimeout = 30 * time.Second

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
	closing := map[string]any{"state": "close", "reason": "pairing_" + reason}
	// Asked here and not taken from the caller. Every way a pairing ends publishes this
	// same closing state, and each of them can be the one that ends a session whatsmeow
	// will not bring back: a dial that failed, a channel that reported an outcome, an
	// error WhatsApp named. A caller that answers for itself is a caller that can be
	// added without the question being asked at all, and then the account is held by an
	// instance with nothing left to try.
	if s.isTerminal() {
		// The run is over and WhatsApp refused this build, so nothing here is going to
		// connect: the account goes back rather than being held by an instance whose
		// image is the reason it cannot pair. On the run's last event, so the pairing's
		// own outcome is out first.
		s.emitLast(protocol.EventSessionState, closing)
		return
	}
	s.emit(protocol.EventSessionState, closing)
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
	// Taken before anything here can wait. An arm that moves the connection dates it, and
	// every one of them takes the transition lock first: a handler already holding it
	// across a publish that waits on a full inbox delays this one by as long as that takes,
	// and a connection dated from then reads as later than the socket it describes.
	// whatsmeow calls this from the goroutine that dispatched the event, so this is as
	// early as this session can know anything.
	dispatched := s.now()
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
	case *waEvents.MediaRetry:
		// A sender's phone answering a request this connector made for a file WhatsApp
		// had dropped. Handed to the command waiting for it, which is the only thing
		// that ever asks.
		return s.reupload(event)
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

		// Before the socket is judged, because the addresses do not depend on whether this
		// connection is one this session still wants: whatsmeow has already written and
		// saved the LID by the time this event exists, and a socket that is about to be
		// closed produces no second Connected to learn it from.
		s.relearn(s.current())
		if s.undoHangUp() {
			// A reconnect that was already past its wait when the disconnect landed. The
			// command has answered `close`, so this socket is one nobody asked for.
			s.log.Info().Msg("closing a socket that came back after a disconnect")
			go s.current().Disconnect()
			return true
		}
		s.setConnectedAt(true, dispatched)
		// Off this goroutine, because this writes a node and the transition lock is
		// held for the length of this case: a socket slow to take it would hold every
		// state change behind it, Close included.
		//
		// Started before the event rather than after it, which is as far as the ordering
		// can be taken: WhatsApp wants an account available before it will report
		// anybody's presence to it, and the client re-subscribing is the first thing the
		// event it is about to get asks of it. Whether the two nodes land in that order
		// is not decidable here -- the client's subscribe has a whole round trip to make
		// and this has none -- so what this buys is a head start, not a guarantee.
		go s.reapplyAvailability(s.ctx, s.current())
		s.emit(protocol.EventSessionState, s.sessionState())
	case *waEvents.KeepAliveTimeout:
		// The socket is open and the server stopped answering on it. Nobody else is going
		// to say so for a while: whatsmeow's own patience here is KeepAliveMaxFailTime,
		// three minutes, and the disconnect it forces at the end of it is one it marks as
		// expected -- so `onDisconnect` publishes nothing and this session goes on
		// reporting `open` over a socket on the floor. Everything the client sends in that
		// window is accepted by `readyToSend`, queued behind the dead socket, and pays its
		// own ceiling there, one command at a time.
		//
		// Reset rather than Disconnect, and that is the whole of it: ResetConnection is
		// the one that dispatches the event, so the session below turns `reconnecting`,
		// the client is told, and `readyToSend` starts refusing in microseconds instead of
		// accepting work for a socket that cannot carry it.
		//
		// What it does not do is give up on the command already in flight: the pending
		// query is answered with a disconnect node and whatsmeow resends the same frame
		// under the same id once the socket is back. Deciding a write failed is what
		// invariant 5 forbids, and nothing here decides that.
		if !keepAliveIsLost(event) {
			return true
		}
		// Taken for the same reason every other arm here takes it: what follows reads the
		// connection and then acts on it, and a hand-back or a reconnect settling in
		// between would leave this publishing `reconnecting` over a session that is
		// closing, or resetting a socket that replaced the one these pings were about.
		s.transition.Lock()
		defer s.transition.Unlock()

		if keepAliveIsStale(s.lastKnownAlive(), event) {
			// Either a timeout about a connection that is already gone, arriving after the
			// socket it is about was replaced, or one whose run of failures the socket
			// recovered from. whatsmeow dispatches each timeout, each recovery and each drop
			// from a goroutine of its own, so any of them can be handled after the thing it
			// describes stopped being true. Acting on one would take down a healthy socket.
			return true
		}
		s.log.Warn().Int("missed", event.ErrorCount).
			Time("last_answered", event.LastSuccess).
			Msg("no keepalive answered on an open socket; taking it down rather than waiting")
		// Read here, so what goes down is the socket this session just judged and not
		// whatever it is on by the time the reset below runs.
		client := s.current()
		// The state goes first, and the order is the point. The `Disconnected` a reset
		// leads to is dispatched only after the close handshake returns, so a session that
		// waited for the event would spend those seconds still reporting `open`, accepting
		// commands into the very lock the close is holding.
		s.setConnected(false)
		s.setReconnecting(true, dispatched)
		// The `Disconnected` this is about to cause is already published, and saying so is
		// what keeps it from being applied late: whatsmeow starts the reconnect from the
		// same instant it dispatches that event, and a `Connected` handled first would
		// leave the drop writing `reconnecting` over the socket that replaced it.
		s.announceDrop()
		judged := s.transitions.Load()
		// Ordered before the publish and started off this goroutine, and both halves of
		// that matter.
		//
		// Before, because the publish can wait: `emit` blocks for as long as the inbox is
		// full, and a reset left behind it would run whenever that cleared, against
		// whatever socket the client had by then -- the one that answered again, or the one
		// whatsmeow put in its place while this handler sat holding the transition lock and
		// could not be told. What it would take down is a healthy socket.
		//
		// Off this goroutine, because `ResetConnection` blocks on the close handshake
		// holding whatsmeow's socket lock: waiting for it here would spend those seconds
		// with the session already refusing commands and its last published state still
		// saying `open`, which is a session the client has no way to make sense of.
		//
		// A goroutine that is ready still has to be scheduled, though, and `ResetConnection`
		// reads the client's socket when it runs rather than when it is asked for, so the
		// count above is carried along and checked on the other side.
		s.takeDownSoon(client, judged)
		s.emit(protocol.EventSessionState, map[string]any{"state": "reconnecting", "reason": "keepalive"})
	case *waEvents.KeepAliveRestored:
		// Nothing to announce: the socket never went down, so no state changed. What this
		// is for is the timeouts that came before it and have not been handled yet.
		//
		// Taken for the same reason the timeout arm takes it, and it is the other half of
		// the same decision: that one reads this stamp and then acts on what it read, so a
		// recovery landing in between would be recorded too late to stop a reset of the
		// socket it says is answering again. With the lock this one is either wholly before
		// that read or wholly after the action.
		s.transition.Lock()
		defer s.transition.Unlock()

		s.answeredKeepAlive(dispatched)
		if !s.cancelOwedReset() {
			return true
		}
		// The socket this session had given up on is answering again, and the takedown it
		// was owed never ran: it was waiting for a command that is still out at WhatsApp.
		// Nothing was closed, so there is nothing to bring back -- what is left is a session
		// reporting `reconnecting` and refusing commands over a connection that works, for
		// as long as that command takes.
		//
		// The narrow half of this is not covered: once the command is answered the takedown
		// is already on its way, and a recovery landing after that resets a socket that is
		// answering again. One reconnect, against the minutes this window can run to.
		s.log.Info().Msg("the mute socket answered again before it could be taken down")
		s.recovered()
		s.emit(protocol.EventSessionState, s.sessionState())
	case *waEvents.Disconnected:
		s.transition.Lock()
		defer s.transition.Unlock()

		// The socket is gone, so a takedown still owed for it has nothing left to do, and
		// leaving it owed is worse than useless: a recovery dispatched just before the drop
		// and handled just after it would cancel that debt and put the session back on a
		// connection that no longer exists. Dropped before the mark is read, because the
		// branch that consumes the mark returns without touching anything else.
		s.forgetOwedReset()
		if s.dropWasAnnounced() {
			// The drop this session brought on itself, published by the handler that caused
			// it. Applying it here as well would be harmless in the order it usually
			// arrives and would wedge the session in the order it sometimes does: handled
			// after the replacement announced itself, it writes `reconnecting` over a
			// healthy socket, and nothing comes after it to put that right.
			return true
		}
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
		s.setReconnecting(state == "reconnecting", dispatched)
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
		s.markTerminal()
		s.offline()
		ban := map[string]any{"kind": "temporary", "reason": event.Code.String()}
		if event.Expire > 0 {
			// A zero duration is whatsmeow saying it does not know when the ban lifts.
			// Publishing now+0 would read as one that already has.
			ban["expires_at"] = time.Now().Add(event.Expire).UnixMilli()
		}
		s.finishing(protocol.EventSessionTemporaryBan, map[string]any{"ban": ban})
	case *waEvents.ClientOutdated:
		s.transition.Lock()
		defer s.transition.Unlock()

		s.refuseLateConnect()
		s.markTerminal()
		s.offline()
		if s.pairingActive() {
			// The pairing reader publishes this one: whatsmeow delivers it here and to
			// the QR channel both, and two canonical events for one outcome is worse
			// than either. It is the reader's copy that retires the session, because for
			// this outcome the reader's copy is the last thing the pairing publishes:
			// whatsmeow closes the channel on the same item.
			return true
		}
		s.emitLast(protocol.EventSessionClientOutdated, map[string]any{})
	case *waEvents.ConnectFailure:
		s.transition.Lock()
		defer s.transition.Unlock()

		s.refuseLateConnect()
		s.markTerminal()
		s.offline()
		s.finishing(protocol.EventSessionConnectFailure, map[string]any{
			"reason": event.Reason.String(), "code": int(event.Reason),
		})
	case *waEvents.PairSuccess:
		s.paired(event)
	case *waEvents.PushNameSetting:
		// The account renamed itself, from this phone or another one. Taken off the event
		// rather than off `client.Store`, which whatsmeow writes on this same path: the
		// event carries the new name, so there is nothing to go and read.
		s.rename(event.Action.GetName())
	case *waEvents.PushName:
		// The account's own name, learned from a message it sent from another device
		// rather than from an app-state sync. whatsmeow writes it to the contact table and
		// dispatches this, which may be the only notice there is: a rename seen this way
		// need not be followed by a `PushNameSetting`.
		if s.isSelf(event.JID) || s.isSelf(event.JIDAlt) {
			s.rename(event.NewPushName)
		}
	case *waEvents.BusinessName:
		// A verified name change, for whoever it is about. whatsmeow puts it in the
		// contact table and does not touch the device record, so the account's own is the
		// one nothing else here would ever hear about: the copy taken at pairing would
		// stand for the life of the session, and it is the copy `contact.resolve`
		// answers with.
		if s.isSelf(event.JID) {
			s.reverify(event.NewBusinessName)
		}
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
	if err := s.store.Forget(ctx); err != nil {
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
	// The verified name arrives here and nowhere else until a reconnect: the client this
	// session was built with had no account on it, so what `adopt` copied was empty. A
	// business account resolving itself before its first reconnect would answer without
	// one, and a reconnect that keeps failing never comes.
	s.setVerifiedName(event.BusinessName)

	payload := map[string]any{"phone": event.ID.User, "platform": event.Platform}
	if lid != "" {
		payload["lid"] = lid
	}
	s.emit(protocol.EventPairingSuccess, payload)
}
