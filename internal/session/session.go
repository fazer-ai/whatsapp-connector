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
		select {
		case <-ctx.Done():
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
	settle(emission, err)
}

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
	s.answer(ctx, &command, result, err)

	// Acknowledged either way, and on a deadline of its own. A command that failed has
	// been answered, and leaving it pending would have another instance run it again
	// after this one restarts, which for a send is a duplicate message rather than a
	// retry.
	//
	// Detached from the session's context because this session is exactly what may have
	// just ended. An acknowledgement refused for the cancellation leaves the entry marked
	// as being carried out here, on purpose, so a reclaim does not run it twice: every
	// later claim then skips it, and if this same instance adopts the session again the
	// marker is still standing. The command is neither retired nor retried until the
	// process restarts. The work is over by this point; what is left has to happen.
	retire, cancel := context.WithTimeout(context.WithoutCancel(ctx), ackTimeout)
	defer cancel()
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

	execCtx := ctx
	if command.Deadline > 0 {
		var cancel context.CancelFunc
		execCtx, cancel = context.WithDeadline(ctx, time.UnixMilli(command.Deadline))
		defer cancel()
	}
	return s.lifecycle(execCtx, command)
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
