package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// The third measurement, and the one the item is named after: two processes, the owner
// killed with work in flight, and the invariants asserted over what reached the streams.
//
// Killed rather than stopped. A hand-back is the orderly case and it is already covered by
// the suite; what has never run is the case where the owner stops existing between one
// event and the next, which is what a machine losing power does and what every one of
// these invariants is written against.
func handover(ctx context.Context, active *run, group *fleet, cl *client, rep *report, plan benchPlan,
	owner *instance, sids []string, pairs []idempotentPair) error {

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

	// The owner goes while those are being carried out. No wait for them first: a
	// handover with nothing in flight is the orderly case wearing a kill.
	if err := owner.kill(); err != nil {
		return fmt.Errorf("%w: %w", errSetup, err)
	}
	rep.note(fmt.Sprintf("troca de dono: %s morta com SIGKILL com %d comandos em voo", owner.name, len(inFlight)))

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

	// A quiet moment before reading, so that what is in flight has landed. This is a wait
	// on the fleet catching up, not a wait dressed as a synchronisation: everything read
	// after it is read from the streams, which keep their own order.
	settle := time.Now()
	for time.Now().Before(settle.Add(20 * time.Second)) {
		reclaimed := 0.0
		for _, peer := range others {
			found, err := peer.metrics(ctx, "wac_commands_reclaimed_total")
			if err == nil {
				reclaimed += found["wac_commands_reclaimed_total"]
			}
		}
		if reclaimed > 0 {
			rep.measure("troca de dono sob carga", "comandos reivindicados do pendente", reclaimed, "comandos")
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	time.Sleep(3 * time.Second)

	return assertInvariants(ctx, cl, rep, plan, inFlight, sids, pairs)
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
