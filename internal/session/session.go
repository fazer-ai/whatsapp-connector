// Package session runs one WhatsApp account: it holds the lease, pumps what the
// engine has to say onto the event stream, and carries out commands one at a time.
package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/cluster"
	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/transport"
)

// IDFunc mints the unique id every frame carries. It is a field rather than a call to
// a uuid package so a test can read the ids it is asserting on.
type IDFunc func() string

// Session is one account, owned by this instance for as long as its lease is fresh.
type Session struct {
	sid       string
	instance  string
	lease     cluster.Lease
	leases    *cluster.Leases
	engine    engine.Session
	publisher transport.Publisher
	replier   transport.Replier
	ledger    Ledger
	newID     IDFunc
	now       func() time.Time
	log       zerolog.Logger

	commands chan *transport.Delivery

	// seq is written by the pump goroutine alone, which is what keeps it monotonic
	// without a lock: two writers would have to agree on an order to be ordered, and
	// the order is exactly what seq is for.
	seq uint64

	stop     context.CancelFunc
	done     chan struct{}
	stopOnce sync.Once

	// queueMu guards the door to commands rather than the channel itself: the executor
	// has to be able to say "nothing more comes in" and then empty what is left,
	// without a command slipping in between the two.
	queueMu  sync.Mutex
	stopping bool
}

// Config is what a session needs to run.
type Config struct {
	Instance  string
	Lease     cluster.Lease
	Leases    *cluster.Leases
	Engine    engine.Session
	Publisher transport.Publisher
	Replier   transport.Replier
	Ledger    Ledger
	NewID     IDFunc
	Now       func() time.Time
	Logger    zerolog.Logger
	// QueueDepth bounds how many commands wait for this session. Beyond it a client
	// is told the session is busy rather than being queued behind a backlog whose
	// deadlines have all passed by the time it is reached.
	QueueDepth int
}

// DefaultQueueDepth is the command backlog one session accepts.
const DefaultQueueDepth = 64

// New starts the session's two goroutines: the pump, which publishes what the engine
// emits, and the executor, which runs commands in the order they arrived.
func New(ctx context.Context, cfg *Config) *Session {
	if cfg.QueueDepth <= 0 {
		cfg.QueueDepth = DefaultQueueDepth
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	runCtx, cancel := context.WithCancel(ctx)
	s := &Session{
		sid:       cfg.Lease.SID,
		instance:  cfg.Instance,
		lease:     cfg.Lease,
		leases:    cfg.Leases,
		engine:    cfg.Engine,
		publisher: cfg.Publisher,
		ledger:    cfg.Ledger,
		replier:   cfg.Replier,
		newID:     cfg.NewID,
		now:       cfg.Now,
		log:       cfg.Logger.With().Str("sid", cfg.Lease.SID).Uint64("epoch", cfg.Lease.Epoch).Logger(),
		commands:  make(chan *transport.Delivery, cfg.QueueDepth),
		stop:      cancel,
		done:      make(chan struct{}),
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); s.pump(runCtx) }()
	go func() { defer wg.Done(); s.execute(runCtx) }()
	go func() { wg.Wait(); close(s.done) }()

	return s
}

// SID is the session this runs.
func (s *Session) SID() string { return s.sid }

// Epoch is the ownership this session publishes under.
func (s *Session) Epoch() uint64 { return s.lease.Epoch }

// Offer hands a command to the session. It reports false when the backlog is full, so
// the caller answers `rate_limited` instead of queueing work whose deadline will have
// passed before it is reached.
// Offer hands a command to this session's executor.
//
// The three answers are three different things for the caller to do, which is why this
// is not a bool: a busy session refuses the command, a stopping one has to leave it
// pending for whoever takes the account next, and only the first two are the same from
// the client's side.
type Offer int

const (
	// OfferAccepted means the command is queued and this session owns answering it.
	OfferAccepted Offer = iota
	// OfferBusy means the queue is full. The client is told so.
	OfferBusy
	// OfferStopped means this session is going away. The command is not this
	// instance's to answer or to refuse.
	OfferStopped
)

// Offer queues a command, or says why it did not.
func (s *Session) Offer(delivery *transport.Delivery) Offer {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	if s.stopping {
		return OfferStopped
	}
	select {
	case s.commands <- delivery:
		return OfferAccepted
	default:
		return OfferBusy
	}
}

// abandonQueue lets go of everything still waiting when the executor stops.
//
// A queued delivery has been read from Redis and not acknowledged, so as far as the
// transport is concerned this process is still carrying it out and will not reclaim it.
// Dropping it silently means the command runs nowhere until another instance claims it
// or this one restarts. Releasing says out loud that nothing here is going to run it.
func (s *Session) abandonQueue() {
	s.queueMu.Lock()
	s.stopping = true
	s.queueMu.Unlock()

	for {
		select {
		case delivery := <-s.commands:
			if delivery.Release != nil {
				delivery.Release()
			}
		default:
			return
		}
	}
}

// Stop ends the session and waits for both goroutines. Safe to call twice, because
// both a lost lease and a shutdown reach it.
func (s *Session) Stop() {
	s.stopOnce.Do(func() {
		s.stop()
		_ = s.engine.Close()
	})
	<-s.done
}

// Done is closed once both goroutines have returned.
func (s *Session) Done() <-chan struct{} { return s.done }

// pump publishes what the engine emits, in the order it emits it.
//
// It is the only writer of seq, and the only publisher for this session, which is
// what makes the client's ordering hold: one shard, one consumer, one writer.
func (s *Session) pump(ctx context.Context) {
	events := s.engine.Events()
	for {
		if ctx.Err() != nil {
			// Checked ahead of the select rather than inside it, because a select whose
			// cases are both ready picks at random: a pump that is being stopped would
			// publish an event under an epoch it is giving up, or not, depending on the
			// scheduler. Stopping means stopping.
			s.abandonPending(events)
			return
		}
		select {
		case <-ctx.Done():
			s.abandonPending(events)
			return
		case emission, ok := <-events:
			if !ok {
				return
			}
			s.publish(ctx, emission)
		}
	}
}

func (s *Session) publish(ctx context.Context, emission engine.Emission) {
	if _, owned := s.leases.Owned(s.sid); !owned {
		// Publishing under a lease this instance no longer holds writes a lower epoch
		// after a higher one has already been seen, which is the one thing a client
		// cannot recover from. Dropping it costs an event; the new owner republishes
		// the state it finds.
		s.log.Warn().Str("type", string(emission.Type)).Msg("dropped an emission from a session owned elsewhere")
		settle(emission, errLostOwnership)
		return
	}

	s.seq++
	event := protocol.Event{
		V:       protocol.Version,
		ID:      s.newID(),
		Type:    emission.Type,
		SID:     s.sid,
		Epoch:   s.lease.Epoch,
		Seq:     s.seq,
		TS:      s.now().UnixMilli(),
		Inst:    s.instance,
		Payload: emission.Payload,
	}
	err := s.publisher.Publish(ctx, &event)
	if err != nil && ctx.Err() == nil {
		s.log.Error().Err(err).Str("type", string(emission.Type)).Msg("failed to publish an event")
	}
	if err == nil {
		err = s.stillOwned()
	}
	settle(emission, err)
}

// stillOwned reports whether the epoch an event was just published under is the one
// this instance holds, and it is checked after the publish and not only before it.
//
// A lease can run out while the write is in flight, and a peer that takes the session
// publishes under a higher epoch straight away. The event then lands behind one the
// client has already seen from a newer owner, and the contract lets a client drop what
// comes from a stale owner: `wa:cursor:<sid>` is the last `epoch:seq` it processed. So
// a successful write is not proof the client has it, and an engine holding WhatsApp's
// acknowledgement on that answer would spend the redelivery that was the only way to
// get the message back. Saying so costs a redelivery, which the client deduplicates on
// the message id, and that is the trade this whole path is built on.
func (s *Session) stillOwned() error {
	lease, owned := s.leases.Owned(s.sid)
	if owned && lease.Epoch == s.lease.Epoch {
		return nil
	}
	s.log.Warn().Uint64("epoch", s.lease.Epoch).
		Msg("published an event under an epoch this instance no longer holds")
	return errLostOwnership
}

// abandonPending settles what the engine has already handed over and this pump is no
// longer going to publish. An engine waiting on a callback that never comes is one
// holding WhatsApp's acknowledgement for a message this instance is done with, and it
// would hold it until its own bound ran out rather than letting the account be
// redelivered to whoever takes the session next.
func (s *Session) abandonPending(events <-chan engine.Emission) {
	for {
		select {
		case emission, ok := <-events:
			if !ok {
				return
			}
			settle(emission, errStopped)
		default:
			return
		}
	}
}

// Ledger remembers what a command did, so a redelivery is answered with the first
// run's result instead of carrying it out a second time. Invariant 5 in AGENTS.md is
// this and nothing else.
type Ledger interface {
	// Recall answers what a command with this key did, and whether it ran at all.
	Recall(ctx context.Context, sid, key string) (json.RawMessage, bool, error)
	// Remember records what a command did, without overwriting an earlier answer.
	Remember(ctx context.Context, sid, key string, result json.RawMessage) error
}

// errStopped is what an emission this pump stopped before publishing settles with.
var errStopped = errors.New("session: the pump stopped before the event was published")

// errLostOwnership is what an emission dropped for a session this instance no longer
// owns settles with. The engine waiting on it has to hear something: an inbound
// message whose callback never fires is one WhatsApp is never told about either way,
// and the session would sit there until it timed out rather than letting the account
// be redelivered to whoever owns it now.
var errLostOwnership = errors.New("session: dropped an emission from a session owned elsewhere")

// settle reports a publish outcome to an engine that asked for one. Most emissions do
// not: they are things the client is told about, not things WhatsApp is waiting on.
func settle(emission engine.Emission, err error) {
	if emission.Settle != nil {
		emission.Settle(err)
	}
}

// execute runs one command at a time, which is what "commands for a session happen in
// the order they were sent" means. A slow send delays the next command of the same
// session and nothing else.
func (s *Session) execute(ctx context.Context) {
	defer s.abandonQueue()
	for {
		// Checked before the select, and not only inside it. With both cases ready the
		// select picks either one, so a session that is already stopping could start one
		// more command and answer it on behalf of an account this instance no longer
		// owns, instead of leaving it for whoever does.
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case delivery := <-s.commands:
			s.run(ctx, delivery)
		}
	}
}

func (s *Session) run(ctx context.Context, delivery *transport.Delivery) {
	command := delivery.Command
	log := s.log.With().Str("cmd_id", command.ID).Str("type", string(command.Type)).Logger()

	result, err := s.carryOut(ctx, &command)
	if errors.Is(err, errUnknownWhetherItRan) {
		// Neither answered nor retired: nobody knows whether this already ran, and both of
		// the other choices are wrong. Handed back so whoever claims it next can ask again
		// once the record is readable.
		log.Warn().Err(err).Msg("gave a command back rather than risk carrying it out twice")
		forfeit(delivery)
		return
	}
	if err != nil && ctx.Err() != nil {
		// The session went away underneath this command: the lease moved, or the process
		// is coming down. What failed is this instance's turn at it, not the command, and
		// the reply would go out on the same dead context and be dropped without a word,
		// leaving the caller with nothing and the entry retired.
		//
		// Handed back instead, which is the same trade invariant 4 makes for events: a
		// send that was cut off may already be in somebody's chat, and the next owner
		// resends under the id the caller picked, so the cost is a redelivery WhatsApp
		// discards rather than a message nobody ever hears about again.
		log.Warn().Err(err).Msg("gave a command back after the session ended under it")
		forfeit(delivery)
		return
	}
	// The answer and the acknowledgement both go out detached from the session's
	// context, because this session is exactly what may have just ended. The work is
	// over by this point and what is left has to happen: a send can succeed in the same
	// instant the lease moves, and answering that on the dying context drops the reply
	// while the acknowledgement retires the command, leaving the caller with nothing
	// about a message that is already in somebody's chat.
	//
	// Acknowledged whether the command worked or not. A command that failed has been
	// answered, and leaving it pending would have another instance run it again after
	// this one restarts, which for a send is a duplicate message rather than a retry.
	// An acknowledgement refused anyway leaves the entry marked as being carried out
	// here, on purpose, so a reclaim does not run it twice: every later claim then skips
	// it, and if this same instance adopts the session again the marker is still
	// standing. The command is neither retired nor retried until the process restarts.
	retire, cancel := context.WithTimeout(context.WithoutCancel(ctx), ackTimeout)
	defer cancel()
	s.answer(retire, &command, result, err)
	if ackErr := delivery.Ack(retire); ackErr != nil {
		log.Error().Err(ackErr).Msg("failed to acknowledge a command")
	}
}

func (s *Session) carryOut(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	if _, owned := s.leases.Owned(s.sid); !owned {
		return nil, protocol.NewError(protocol.ErrorOwnedElsewhere, "the session moved to another instance")
	}
	if expired(command, s.now()) {
		// The caller stopped waiting before this was reached. Running it anyway is a
		// side effect nobody is expecting the outcome of.
		return nil, protocol.NewError(protocol.ErrorExpired, "the command deadline passed before it was reached")
	}

	key := idempotencyKey(command)
	result, done, err := s.alreadyDid(ctx, key)
	if err != nil {
		return nil, err
	}
	if done {
		// Only successes are remembered, so a recalled command is one that worked.
		return result, nil
	}

	execCtx := ctx
	if command.Deadline > 0 {
		var cancel context.CancelFunc
		execCtx, cancel = context.WithDeadline(ctx, time.UnixMilli(command.Deadline))
		defer cancel()
	}

	result, err = s.lifecycle(execCtx, command)
	if err == nil && key != "" && s.ledger != nil {
		// Only a success is remembered. A failure is the caller's to try again, and a
		// remembered one would answer every later attempt with the same refusal.
		//
		// Written after the fact and not reserved before it, because a reservation
		// cannot be resolved: an entry saying an attempt was made says nothing about
		// whether it landed, so an instance reclaiming the command would have to choose
		// between dropping a message that never went out and sending one that already
		// did. What covers that window is the caller naming the message, so a resend
		// carries the id the first attempt used.
		// On a context of its own, because the command's deadline may have run out in
		// the same instant the work finished, and a record that is not written is a
		// command that gets carried out again.
		//
		// Tried more than once, because the side effect has already happened and this is
		// the only thing left that can stop it happening again. A Redis that refuses one
		// call and answers the next is the common shape of a failure here, and giving up
		// on the first refusal spends the whole window on it.
		//
		// A failure that outlasts the attempts is logged and the command is still
		// answered and retired, which is the one place this layer knowingly leaves a
		// window. Giving the delivery back instead would not close it: the side effect
		// has already happened, so the redelivery would find no record and do it a
		// second time, turning a risk into a certainty. What covers what is left is the
		// caller naming the message.
		s.remember(ctx, command, key, result)
	}
	return result, err
}

// remember writes the record of a command that ran, and keeps trying inside a bounded
// window rather than giving up on the first refusal.
//
// Detached from the command's context, because its deadline may have run out in the
// same instant the work finished, and a record that is not written is a command that
// gets carried out again. Bounded all the same: this runs on the session's executor, so
// every attempt is a command behind it that is not running.
func (s *Session) remember(ctx context.Context, command *protocol.Command, key string, result json.RawMessage) {
	write, cancel := context.WithTimeout(context.WithoutCancel(ctx), ledgerWindow)
	defer cancel()

	var err error
	for attempt := range ledgerAttempts {
		if attempt > 0 {
			select {
			case <-write.Done():
				// Out of window. The error from the last attempt is what gets reported.
			case <-time.After(ledgerBackoff):
			}
			if write.Err() != nil {
				break
			}
		}
		if err = s.ledger.Remember(write, s.sid, key, result); err == nil {
			return
		}
	}
	s.log.Error().Err(err).Str("cmd_id", command.ID).Str("key", key).
		Msg("carried a command out and could not record it; a redelivery will do it again")
}

// ledgerWindow bounds the whole of that, and ledgerAttempts and ledgerBackoff divide it
// up. Short in total for the same reason a single read is: the record sits in the same
// Redis the command arrived through, so one that is not answering promptly is one
// nothing else is getting through either, and the window is time the session spends not
// running the commands behind this one.
const (
	ledgerWindow   = 5 * time.Second
	ledgerAttempts = 3
	ledgerBackoff  = 200 * time.Millisecond
)

// ledgerTimeout bounds a read or a write of the record. It is short because the record
// sits in the same Redis the command arrived through: one that is not answering
// promptly is one nothing else is getting through either.
const ledgerTimeout = 3 * time.Second

// errUnknownWhetherItRan is what a command whose record cannot be read answers with. It
// never reaches a caller: the delivery is handed back instead, so whoever claims it next
// can ask again once Redis is answering.
var errUnknownWhetherItRan = errors.New("session: cannot tell whether the command has already run")

// alreadyDid answers a command this session has already carried out.
//
// A record that cannot be read is not a record saying no. Running the command anyway
// would carry out a side effect whose first outcome is unknown, which is the one thing
// invariant 5 forbids, so the command is neither run nor refused: it is handed back for
// another claim. The store is the same Redis the command arrived through, so one that
// cannot answer this is one the reply and the acknowledgement would not reach either,
// and the delivery was coming round again regardless.
func (s *Session) alreadyDid(ctx context.Context, key string) (json.RawMessage, bool, error) {
	if key == "" || s.ledger == nil {
		return nil, false, nil
	}
	read, cancel := context.WithTimeout(ctx, ledgerTimeout)
	defer cancel()

	result, found, err := s.ledger.Recall(read, s.sid, key)
	if err != nil {
		return nil, false, fmt.Errorf("%w: %w", errUnknownWhetherItRan, err)
	}
	if !found {
		return nil, false, nil
	}
	s.log.Info().Str("key", key).Msg("answered a redelivered command with what the first one did")
	return result, true, nil
}

// idempotencyKey is what a command is remembered under, and the empty string for one
// that names nothing to be remembered by.
//
// A command that names the message it is about to put on the wire is keyed by it,
// which is the contract's `msg:<message_id>`: the caller picks the id, so every
// redelivery of that command names the same one, and so does a resend the caller makes
// of its own accord. Everything else is keyed by the `idempotency_key` the frame
// carries, which is a field of the command and not of its payload.
func idempotencyKey(command *protocol.Command) string {
	if !command.ChangesSomething() {
		// A question, and the answer is only worth having if it is current. Answering a
		// redelivered `session.status` from a record would report the state the session
		// was in when it was first asked.
		return ""
	}
	// Only where the id is the command's own creation. `message.download_media` also
	// carries a `message_id`, and there it names a message somebody else's command
	// created, so keying by it would answer a download with the result of the send.
	if command.Type.NamesItsOwnMessage() {
		var body struct {
			MessageID string `json:"message_id"`
		}
		if err := json.Unmarshal(command.Payload, &body); err == nil && body.MessageID != "" {
			return "msg:" + body.MessageID
		}
	}
	if command.IdempotencyKey != "" {
		// Prefixed, because the caller picks this string and the schema takes any: one
		// reading `msg:m1` on a logout would be answered from the record of the send of
		// m1, and report an account unlinked that is still paired.
		return "idem:" + command.IdempotencyKey
	}
	// Neither, which the contract's own `session.logout` fixture is. The command's id
	// is what is left, and it is enough for the redelivery this exists to stop: the
	// transport hands back the same entry, so the same frame arrives carrying the same
	// id. A client that sends a second command of its own gets a new id and is not
	// covered, which is what `idempotency_key` is for. Prefixed for the same reason as
	// above: a command id is a caller's string too.
	return "cmd:" + command.ID
}

// The three lifecycle commands go to the engine's own methods rather than through
// Execute: they are not requests about a live session, they are what makes one live or
// ends it, and an engine that had to recognise them inside Execute would be answering
// two different kinds of question through one door.
func (s *Session) lifecycle(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	switch command.Type {
	case protocol.CommandSessionConnect:
		var request engine.ConnectRequest
		if err := json.Unmarshal(command.Payload, &request); err != nil {
			return nil, protocol.NewError(protocol.ErrorInvalidPayload, "session.connect payload is not readable")
		}
		if err := s.engine.Connect(ctx, request); err != nil {
			return nil, err
		}
		// The contract answers connect with a connection_state. Pairing is a
		// conversation that continues on the event stream; this is where it stands
		// when the call returns.
		return s.engine.Execute(ctx, &protocol.Command{
			V: protocol.Version, ID: command.ID, Type: protocol.CommandSessionStatus, SID: command.SID,
			Payload: json.RawMessage(`{}`),
		})
	case protocol.CommandSessionDisconnect:
		return nil, s.engine.Disconnect(ctx)
	case protocol.CommandSessionLogout:
		return nil, s.engine.Logout(ctx)
	default:
		return s.engine.Execute(ctx, command)
	}
}

// answer replies to an RPC command, and publishes a `command.failed` event for a
// fire-and-forget one that failed: nobody is blocked on it, so a failure with no event
// would be a command that silently did nothing.
func (s *Session) answer(ctx context.Context, command *protocol.Command, result json.RawMessage, err error) {
	if command.ReplyTo != "" {
		s.reply(ctx, command, result, err)
		return
	}
	if err == nil {
		return
	}
	failure := asProtocolError(err)
	s.publish(ctx, engine.Emission{
		Type:    protocol.EventCommandFailed,
		Payload: mustMarshal(map[string]any{"command_id": command.ID, "type": command.Type, "error": failure}),
	})
}

func (s *Session) reply(ctx context.Context, command *protocol.Command, result json.RawMessage, err error) {
	reply := protocol.Reply{V: protocol.Version, ID: command.ID, OK: err == nil, Result: result}
	if err != nil {
		reply.Error = asProtocolError(err)
	}
	if replyErr := s.replier.Reply(ctx, command.ReplyTo, reply); replyErr != nil && ctx.Err() == nil {
		s.log.Error().Err(replyErr).Str("cmd_id", command.ID).Msg("failed to answer a command")
	}
}

func expired(command *protocol.Command, now time.Time) bool {
	return command.Deadline > 0 && now.UnixMilli() > command.Deadline
}

// asProtocolError maps whatever went wrong onto the closed set the client branches on.
// An error with no code of its own becomes `internal` rather than reaching a client's
// UI as raw text.
func asProtocolError(err error) *protocol.Error {
	var coded *protocol.Error
	if errors.As(err, &coded) {
		return coded
	}
	switch {
	case errors.Is(err, engine.ErrNotSupported):
		return protocol.NewError(protocol.ErrorUnsupported, "this connector does not implement that command")
	case errors.Is(err, cluster.ErrNotOwner):
		return protocol.NewError(protocol.ErrorOwnedElsewhere, "the session moved to another instance")
	case errors.Is(err, context.DeadlineExceeded):
		return protocol.NewError(protocol.ErrorTimeout, "the command did not finish in time")
	default:
		return protocol.NewError(protocol.ErrorInternal, "the connector could not carry out the command")
	}
}

func mustMarshal(payload any) json.RawMessage {
	body, err := json.Marshal(payload)
	if err != nil {
		// Everything reaching this is a literal built two lines above, so a failure is
		// a programming error rather than a runtime one.
		panic(fmt.Sprintf("session: marshal: %v", err))
	}
	return body
}
