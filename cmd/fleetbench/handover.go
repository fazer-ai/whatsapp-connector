package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/transport/redisstream"
)

// The third measurement, and the one the item is named after: two processes, the owner
// killed with work in flight, and the invariants asserted over what reached the streams.
//
// Killed rather than stopped. A hand-back is the orderly case and it is already covered by
// the suite; what has never run is the case where the owner stops existing between one
// event and the next, which is what a machine losing power does and what every one of
// these invariants is written against.
func handover(ctx context.Context, active *run, group *fleet, cl *client, rep *report, plan benchPlan,
	owner *instance, sids []string, pairs []idempotentPair, counted *census) error {

	others := make([]*instance, 0, plan.processes-1)
	for i := 2; i < plan.processes+1; i++ {
		peer, err := group.start(ctx, fmt.Sprintf("bench-%s-%c", active.id, 'a'+i))
		if err != nil {
			return fmt.Errorf("%w: %w", errSetup, err)
		}
		rep.processes = append(rep.processes, noteOf(peer))
		others = append(others, peer)
	}
	if len(others) == 0 {
		return fmt.Errorf("%w: nenhum par subiu, entao nao ha para quem a posse mudar", errSetup)
	}

	// Work in flight, and it stays in flight on purpose: the replies are left on their
	// lists rather than popped, because what a duplicated side effect looks like from out
	// here is two answers to one command that do not agree.
	inFlight := make([]string, 0, len(sids)*plan.sends)
	sentAt := time.Now()
	for _, sid := range sids {
		for n := range plan.sends {
			id := fmt.Sprintf("send-%s-%s-%d", active.id, shortSID(sid), n)
			payload := fmt.Sprintf(`{"message_id":%q,"to":{"kind":"phone","id":"5511999990002"},`+
				`"content":{"type":"text","body":"carga %d"}}`, id, n)
			err := cl.send(ctx, cl.keys.Commands(sid), &protocol.Command{
				V: protocol.Version, ID: id, Type: protocol.CommandMessageSend, SID: sid,
				TS: time.Now().UnixMilli(), ReplyTo: cl.keys.Reply(id), Payload: json.RawMessage(payload),
			})
			if err != nil {
				return fmt.Errorf("%w: put a send in flight for %s: %w", errSetup, sid, err)
			}
			inFlight = append(inFlight, id)
		}
	}

	// From here to the end of the frozen phase the fleet never goes quiet. What a short
	// overlap of two owners writes is an event under the older epoch landing after one
	// under the newer, and that only exists if somebody is asking the session to publish.
	steady := startLoad(ctx, active, cl, sids, 200*time.Millisecond)

	// Parada em toda saida, e nao so na que le os streams. Qualquer fase abaixo pode
	// devolver erro, e as goroutines seguiriam mandando comando para os streams desta
	// corrida enquanto a limpeza adiada varre e apaga as chaves dela: a corrida passaria a
	// reportar chave propria como chave vazada que nao conseguiu remover. Chamar `end` duas
	// vezes e seguro (o cancel e idempotente e a espera volta na hora depois da primeira), e
	// a chamada la embaixo continua sendo a que conta, porque a ordem importa: a carga para
	// ANTES de qualquer leitura.
	defer steady.end()

	// Measured, not assumed: how many of those the owner had not retired when it died.
	//
	// The fake engine answers a send at once, and these go on the streams one after
	// another, so with a small run every command can already be answered and acknowledged
	// by the time the kill lands. Calling them "in flight" then describes a handover that
	// interrupted nothing, and the recovery it claims to have exercised never ran. The
	// pending entries of the consumer groups are what says otherwise, and they are read
	// from Redis rather than counted from what this bench sent.
	// The owner goes while those are being carried out. No wait for them first: a
	// handover with nothing in flight is the orderly case wearing a kill.
	if err := owner.kill(); err != nil {
		return fmt.Errorf("%w: %w", errSetup, err)
	}

	// Read AFTER the kill, because what this number has to say is what the dead owner left
	// behind.
	//
	// Read before, it was a reading of a moving fleet: `pendingOn` walks the streams one at
	// a time while the owner goes on working, so what it counted on the first stream could
	// be acknowledged before it reached the last, and all of it before the kill landed. The
	// count would then describe a handover that interrupted nothing while claiming it
	// interrupted that much. After the kill nobody is retiring those entries: the peers
	// have not claimed them yet, and the number is the work that was actually cut off.
	pending, err := cl.pendingOn(ctx, sids)
	if err != nil {
		return fmt.Errorf("%w: %w", errSetup, err)
	}
	cut, unread, aftermath := killAftermath(owner.name, len(inFlight), pending)
	rep.measure("troca de dono sob carga", "comandos entregues e nao confirmados quando o dono morreu",
		cut, "comandos")
	rep.measure("troca de dono sob carga", "comandos ainda nao lidos por ninguem nesse instante",
		unread, "comandos")
	rep.note(aftermath)

	moved := 0
	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		total := 0
		for _, peer := range others {
			found, err := peer.metrics(ctx, "wac_sessions_running")
			if err != nil {
				return fmt.Errorf("%w: %w", errSetup, err)
			}
			total += int(found["wac_sessions_running"])
		}
		moved = total
		// Counted while ownership is moving, which is where a fleet that lets two
		// instances hold one session shows the extra copies.
		counted.take(ctx, "troca de dono sob carga", others)
		if moved >= len(sids) {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	rep.measure("troca de dono sob carga", "sessoes que os pares assumiram", float64(moved), "sessoes")
	if moved < len(sids) {
		return fmt.Errorf("%w: so %d das %d sessoes mudaram de dono em 3 minutos; sem a troca nao ha "+
			"o que afirmar sobre epoch", errSetup, moved, len(sids))
	}

	// Waited for the thing that has to be true, and not for the first sign of it.
	//
	// The reading below is taken from the consumer groups, and a group with work still in
	// it is a group with pending entries. Stopping at the first reclaimed command and
	// sleeping a fixed three seconds would hand `assertConsumerGroups` a fleet that is
	// legitimately busy, and it would report ordinary work in progress as a hole. What
	// ends the wait is the drain; what ends the waiting is a deadline, and a deadline that
	// passes is said out loud instead of being spent as a pass.
	drained, reclaimed, passes, err := waitForDrain(ctx, cl, others, sids, 90*time.Second)
	if err != nil {
		return fmt.Errorf("%w: %w", errSetup, err)
	}
	rep.measure("troca de dono sob carga", "comandos reivindicados do pendente", reclaimed, "comandos")
	rep.measure("troca de dono sob carga", "passadas de reivindicacao que os pares completaram", passes, "passadas")
	if reclaimed == 0 && passes == 0 {
		rep.note("os pares nao completaram nenhuma passada de reivindicacao: um zero em 'comandos " +
			"reivindicados' aqui nao e 'nao havia o que reivindicar', e sim que ninguem contou")
	}
	// Dito em nota E levado a assercao. A nota e para quem le a corrida; o que a assercao
	// precisa e nao dar veredito sobre o que sobrou pendente, porque uma espera que terminou
	// pelo relogio nao separa "a frota estava ocupada" de "a entrega furou".
	stillWorking := ""
	if !drained {
		stillWorking = "o grupo consumidor nao drenou em 90 s depois da troca de dono, entao o que " +
			"sobrou pendente e trabalho em curso de uma frota ocupada, e nao um buraco na entrega"
		rep.note(stillWorking)
	}

	// Read here and not after the phase below, because a reply list has a TTL: the
	// connector puts one on it so an answer nobody came back for does not sit in Redis
	// forever. MEASURED: with the frozen phase in between, the assertion found 0 replies
	// where the same run without it found 8, and a series of zero is not an assertion.
	answers, err := cl.repliesToAll(ctx, inFlight)
	if err != nil {
		return fmt.Errorf("%w: %w", errSetup, err)
	}

	// How old the answers were when they were read, because a reply list expires.
	//
	// The connector puts a TTL on it so an answer nobody came back for does not sit in
	// Redis forever, and the waits above are not short: an ownership change can take
	// minutes and the drain has 90 s of its own. Past that TTL, a command that answered
	// twice reads exactly like a command that never answered, and the half of this
	// assertion that looks for two answers that disagree stops being measured. It stops
	// silently unless the run says so, which is what this passes down.
	expired := ""
	if age := time.Since(sentAt); age >= redisstream.DefaultReplyTTL {
		expired = fmt.Sprintf("os comandos em voo foram lidos %s depois de enviados, e a lista de "+
			"resposta expira em %s: uma resposta que existiu pode nao estar mais la, entao a metade "+
			"que procura duas respostas discordantes nao foi medida nesta corrida",
			age.Round(time.Second), redisstream.DefaultReplyTTL)
	}

	// The fence, and it comes last because it is the only phase that needs a live owner
	// to have lost the lease. Everything the thawed instance publishes lands in the
	// streams the assertions below read, in the order the shard kept.
	fence := &fenceOutcome{}
	if len(others) > 1 {
		if err := frozenOwner(ctx, active, cl, rep, plan, others[0], others[1:], sids, counted, fence); err != nil {
			return err
		}
	} else {
		// Same shape as the phase's own empty result: an assertion with no series, so the
		// run's exit code says the fence went unmeasured instead of a note saying it while
		// the code says VERDE.
		fence.claim = fenceCondition(0, "a fase que produz a cerca nao rodou",
			fmt.Sprintf("a corrida subiu %d processos, e congelar o unico par deixaria a frota sem "+
				"ninguem para assumir as sessoes dele. Um dono morto nao publica, entao nenhuma morte "+
				"desta corrida pode quebrar a cerca. Use -processes 3 ou mais", plan.processes))
		rep.assert(fence.claim)
	}

	// Stopped before anything is read, so nothing is still writing to a stream the
	// assertions are about to walk.
	loaded, refused := steady.end()
	rep.measure("troca de dono sob carga", "comandos da carga continua", float64(loaded), "comandos")
	if refused > 0 {
		rep.note(fmt.Sprintf("%d envio(s) da carga continua foram recusados pelo Redis e nao entraram "+
			"em nenhum stream. A medida acima conta o que chegou, entao ela nao os inclui; o que eles "+
			"significam e que a frota ficou menos ocupada do que esta corrida pediu", refused))
	}

	// What the idempotency reading does NOT cover, said with the numbers rather than left
	// for a reader to infer from a series of sixteen over a run of thousands.
	//
	// Two batches are probes, placed on purpose: the commands put in flight across the
	// handover, whose replies are read the moment the phase ends, and the paired sends of
	// phase 1, which ask for the same message twice and are judged by the timestamp the
	// engine puts on a send it really makes. Everything else is load, and its replies are
	// gone: a reply list carries a TTL of `redisstream.DefaultReplyTTL` and the frozen
	// phase alone outlasts it, so a second answer that existed is no longer there to be
	// compared against the first.
	rep.note(idempotencyScope(len(inFlight), len(pairs), plan.sessions*plan.sends, loaded))

	// Drained again, because the phase above put work in front of an instance that spent
	// a minute frozen. A command a thawed owner is still finishing is ordinary work in
	// progress, and reading it as a hole in the consumer group is the same mistake as
	// reading it before the first drain.
	drainedAgain, _, _, err := waitForDrain(ctx, cl, append([]*instance{}, others...), sids, 60*time.Second)
	if err != nil {
		return fmt.Errorf("%w: %w", errSetup, err)
	}
	if !drainedAgain {
		stillWorking = "o grupo consumidor nao drenou nos 60 s depois da fase do dono congelado, entao " +
			"o que sobrou pendente e trabalho em curso de uma frota ocupada, e nao um buraco na entrega"
		rep.note(stillWorking)
	}

	// Drained commands are not published events, and the assertions below walk the event
	// streams.
	//
	// The two are independent: `Session.pump` publishes on its own schedule, not on the
	// acknowledgement of the command that caused the work, and the fake engine buffers its
	// receipt before that. So a fleet with empty consumer groups can still have events on
	// the way, and reading the shards at that moment gives a sequence that is missing its
	// tail -- which reads exactly like a lost event, or hides one. What ends this wait is
	// the shards holding still, and a deadline that passes is said out loud rather than
	// spent as a pass.
	settled, err := waitForQuietStreams(ctx, cl, plan.shards, 30*time.Second)
	if err != nil {
		return fmt.Errorf("%w: %w", errSetup, err)
	}
	stillPublishing := ""
	if !settled {
		stillPublishing = "os shards de evento ainda cresciam quando o prazo de 30 s acabou, entao o que " +
			"esta lido aqui e uma sequencia que pode estar sem a cauda: o evento de dono velho que falta, " +
			"ou o buraco que fecharia, ainda pode estar a caminho"
		rep.note(stillPublishing)
	}

	return assertInvariants(ctx, cl, rep, plan, answers, sids, pairs, counted, fence,
		stillWorking, expired, stillPublishing)
}

// idempotencyScope says, with the numbers, which commands the idempotency reading covered
// and which it did not.
//
// Out of the phase so a test reaches it, and said at all because the claim's series is a
// count of sixteen over a run of tens of thousands, and a reader who does not divide those
// two numbers reads a verdict about the run. Two batches are probes placed on purpose: the
// commands put in flight across the handover, whose replies are read the moment that phase
// ends, and the paired sends of phase 1, which ask for the same message twice and are
// judged by the timestamp the engine puts on a send it really makes. The rest is load, and
// its answers are gone -- a reply list carries a TTL and the frozen phase alone outlasts
// it, so a second answer that existed is no longer there to disagree with the first.
func idempotencyScope(inFlight, pairs, frozen int, loaded int64) string {
	return fmt.Sprintf("a leitura de idempotencia cobre dois lotes por desenho: os %d comandos postos "+
		"em voo na troca de dono e os %d pedidos em par da fase 1. Os %d comandos postos na frente do "+
		"dono congelado e os %d da carga continua ficam de fora, porque a lista de resposta expira em "+
		"%s e a fase do congelado sozinha dura mais que isso. Um verde aqui nao diz que nenhum comando "+
		"da carga continua duplicou efeito: diz que nenhum dos lotes lidos duplicou",
		inFlight, pairs, frozen, loaded, redisstream.DefaultReplyTTL)
}

func shortSID(sid string) string {
	if cut := strings.LastIndex(sid, "-"); cut >= 0 {
		return sid[cut+1:]
	}
	return sid
}

// poolBackends is how many connections the fleet holds on this run's database, read from
// the server rather than from the connector.
//
// The server's view and not the process's, because there is no metric for it: the pool is
// a `database/sql` object nobody exports, so the only place the number exists is
// `pg_stat_activity`. The bench's own connections are excluded by name; counting them
// would inflate the reading by however many readers a phase happened to open.
func poolBackends(ctx context.Context, active *run) (int, error) {
	db, err := sql.Open("postgres", active.benchURL)
	if err != nil {
		return 0, fmt.Errorf("%w: open %s: %w", errSetup, active.database, err)
	}
	defer func() { _ = db.Close() }()
	var count int
	err = db.QueryRowContext(ctx,
		`SELECT count(*) FROM pg_stat_activity WHERE datname = $1 AND application_name = $2`,
		active.database, fleetApplicationName).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("%w: count the fleet's connections on %s: %w", errSetup, active.database, err)
	}
	return count, nil
}

// desiredConnected is the reading the store side of the starting state comes from, taken
// with a connection of its own so that it is the database answering and not the bench's
// memory of what it asked for.
func desiredConnected(ctx context.Context, active *run) (int, error) {
	db, err := sql.Open("postgres", active.benchURL)
	if err != nil {
		return 0, fmt.Errorf("%w: open %s: %w", errSetup, active.database, err)
	}
	defer func() { _ = db.Close() }()
	var count int
	err = db.QueryRowContext(ctx, `SELECT count(*) FROM wac_session_desired WHERE desired = 'connected'`).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("%w: count the sessions the fleet should be running: %w", errSetup, err)
	}
	return count, nil
}

// waitForDrain waits until every command stream of the run has nothing pending, and says
// whether it got there and how many commands the peers reclaimed on the way.
//
// Both numbers come from the fleet: the pending count from the consumer groups, the
// reclaimed count from the peers' own metric. Neither is inferred from what this bench
// sent, which is the difference between measuring a handover and describing one.
func waitForDrain(ctx context.Context, cl *client, peers []*instance, sids []string,
	within time.Duration) (drained bool, reclaimed, passes float64, err error) {

	deadline := time.Now().Add(within)
	for {
		pending, err := cl.pendingOn(ctx, sids)
		if err != nil {
			return false, 0, 0, err
		}
		// The drain waits on both halves: what is left unread is work the fleet still owes,
		// even if nobody has been handed it yet.
		left := pending.total()
		reclaimed, passes = 0, 0
		for _, peer := range peers {
			// The Vec is optional and the pass counter is not: a peer that reclaimed
			// nothing has no `wac_commands_reclaimed_total` row at all, and reading that
			// absence as an error would fail a run for the peer that had nothing to take.
			found, err := peer.metricsWithOptional(ctx,
				[]string{"wac_command_reclaim_passes_total"},
				[]string{"wac_commands_reclaimed_total"})
			if err != nil {
				return false, 0, 0, err
			}
			reclaimed += found["wac_commands_reclaimed_total"]
			passes += found["wac_command_reclaim_passes_total"]
		}
		if left == 0 {
			return true, reclaimed, passes, nil
		}
		if time.Now().After(deadline) {
			return false, reclaimed, passes, nil
		}
		select {
		case <-ctx.Done():
			return false, reclaimed, passes, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// waitForQuietStreams waits until the event shards stop growing, and says whether they did.
//
// Still for three readings in a row rather than for one: a publisher between two events is
// a stream that did not grow since the last look, and stopping there would be stopping in
// the middle of exactly the burst this bench produces on purpose.
func waitForQuietStreams(ctx context.Context, cl *client, shards int, within time.Duration) (bool, error) {
	deadline := time.Now().Add(within)
	var quiet stillness
	for {
		// The last entry id of every shard, and not the sum of their lengths.
		//
		// `Streams.Publish` trims with approximate MAXLEN, so once a shard reaches
		// `DefaultEventMaxLen` its length stops moving while new entries keep replacing old
		// ones: two readings agreeing on the length would then report a fleet at full
		// throughput as one that had gone quiet. A stream id only ever grows, so the pair
		// (length, last id) moves whenever anything was published.
		reads := make([]shardRead, 0, shards)
		for shard := range shards {
			read, err := cl.eventsOn(ctx, shard)
			if err != nil {
				return false, err
			}
			reads = append(reads, read)
		}
		if quiet.saw(streamMark(reads)) {
			return true, nil
		}
		if time.Now().After(deadline) {
			return false, nil
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// stillness counts how many readings in a row found the same total.
//
// Three and not one: a publisher between two events leaves a stream that did not grow
// since the last look, and stopping there stops in the middle of the burst this bench
// produces on purpose. Its own type so the count can be disproved in a table, instead of
// only by a four-minute run whose streams would have to be caught mid-burst.
type stillness struct {
	last  string
	runs  int
	begun bool
}

// saw records a reading and reports whether the streams have held still long enough.
func (s *stillness) saw(mark string) bool {
	if s.begun && mark == s.last {
		s.runs++
		return s.runs >= 3
	}
	s.last, s.runs, s.begun = mark, 1, true
	return false
}

// streamMark is what two readings are compared by: every shard's length AND the id of its
// last entry.
//
// The id, and not the length alone. `Streams.Publish` trims with approximate MAXLEN, so a
// shard that reached `DefaultEventMaxLen` keeps its length while new entries replace old
// ones -- and two readings agreeing on the length would report a fleet at full throughput
// as one that had gone quiet, which is the one thing this wait exists to prevent. A stream
// id only ever grows, so the pair moves whenever anything was published.
func streamMark(reads []shardRead) string {
	mark := strings.Builder{}
	for _, read := range reads {
		fmt.Fprintf(&mark, "%s:%d:%s|", read.stream, read.length, read.lastID)
	}
	return mark.String()
}

// killAftermath turns what the consumer groups held right after the kill into the two
// numbers the report carries and the note that says what they mean.
//
// The two halves stay apart because only one of them is work this kill interrupted.
// `Pending` is an entry handed to a consumer that never acknowledged it, which after a
// kill is what the dead owner was in the middle of; `lag` is an entry nobody has been
// handed at all, which under a steady load is the load still arriving. Summed, a fast
// owner that acknowledged its whole batch before dying would report "work interrupted"
// made entirely of commands that showed up afterwards -- and the phase that follows,
// which is about peers reclaiming what was cut off, would be described as exercised when
// nothing was cut off at all.
func killAftermath(owner string, sent int, left backlog) (cut, unread float64, note string) {
	if left.pending == 0 {
		return 0, float64(left.lag), fmt.Sprintf("troca de dono: %s morta com SIGKILL, e logo depois "+
			"nao havia entrada ENTREGUE e nao confirmada em grupo nenhum (havia %d ainda nao lidas por "+
			"ninguem, que sao carga chegando e nao trabalho cortado). A troca aconteceu, mas ela NAO "+
			"interrompeu trabalho: o que um par reivindica depois disso e nada, e a parte de reentrega "+
			"desta fase fica sem medida. Aumentar -sends e o que fecha essa janela.", owner, left.lag)
	}
	return float64(left.pending), float64(left.lag), fmt.Sprintf("troca de dono: %s morta com SIGKILL, "+
		"e logo depois %d entradas seguiam ENTREGUES e nao confirmadas nos grupos consumidores, do "+
		"lote de %d envios mais a carga continua. Esse e o trabalho que a morte cortou; as %d ainda "+
		"nao lidas por ninguem ficam de fora dele, porque ninguem as comecou",
		owner, left.pending, sent, left.lag)
}
