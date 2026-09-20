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
	pending, err := cl.pendingOn(ctx, sids)
	if err != nil {
		return fmt.Errorf("%w: %w", errSetup, err)
	}

	// The owner goes while those are being carried out. No wait for them first: a
	// handover with nothing in flight is the orderly case wearing a kill.
	if err := owner.kill(); err != nil {
		return fmt.Errorf("%w: %w", errSetup, err)
	}
	rep.measure("troca de dono sob carga", "comandos ainda pendentes quando o dono morreu",
		float64(pending), "comandos")
	if pending == 0 {
		rep.note(fmt.Sprintf("troca de dono: %s morta com SIGKILL, e nenhum dos %d comandos enviados "+
			"ainda estava pendente nesse instante. A troca aconteceu, mas ela NAO interrompeu trabalho: "+
			"o que um par reivindica depois disso e nada, e a parte de reentrega desta fase fica sem "+
			"medida. Aumentar -sends e o que fecha essa janela.", owner.name, len(inFlight)))
	} else {
		rep.note(fmt.Sprintf("troca de dono: %s morta com SIGKILL com %d dos %d comandos ainda pendentes "+
			"no grupo consumidor", owner.name, pending, len(inFlight)))
	}

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
	if len(others) > 1 {
		if err := frozenOwner(ctx, active, cl, rep, plan, others[0], others[1:], sids, counted); err != nil {
			return err
		}
	} else {
		rep.note(fmt.Sprintf("fase do dono congelado: pulada, porque a corrida subiu %d processos e "+
			"congelar o unico par deixaria a frota sem ninguem para assumir as sessoes dele. Sem ela, a "+
			"metade da cerca da invariante 1 (perder a lease para a sessao na hora) fica SEM MEDIDA: "+
			"um dono morto nao publica, entao nenhuma morte desta corrida pode quebra-la. Use "+
			"-processes 3 ou mais.", plan.processes))
	}

	// Stopped before anything is read, so nothing is still writing to a stream the
	// assertions are about to walk.
	rep.measure("troca de dono sob carga", "comandos da carga continua", float64(steady.end()), "comandos")

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

	return assertInvariants(ctx, cl, rep, plan, answers, sids, pairs, counted, stillWorking, expired)
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
		`SELECT count(*) FROM pg_stat_activity WHERE datname = $1 AND application_name <> $2`,
		active.database, benchApplicationName).Scan(&count)
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
		if pending == 0 {
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
