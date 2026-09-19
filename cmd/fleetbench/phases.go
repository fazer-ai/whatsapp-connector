package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// measure runs the three phases the issue asks for, in the order their preconditions
// allow: what one instance carries, what bringing it all back costs, and what an
// ownership change under load does to the invariants.
func measure(ctx context.Context, active *run, group *fleet, rep *report, plan benchPlan) error {
	cl := newClient(active.rdb, active.prefix, plan.shards)

	first, err := group.start(ctx, "bench-"+active.id+"-a")
	if err != nil {
		return fmt.Errorf("%w: %w", errSetup, err)
	}
	rep.processes = append(rep.processes, noteOf(first))

	advertised, err := cl.fleetShards(ctx)
	if err != nil {
		return fmt.Errorf("%w: %w", errSetup, err)
	}
	if advertised != plan.shards {
		return fmt.Errorf("%w: the fleet advertises %d event shards and this run asked for %d",
			errSetup, advertised, plan.shards)
	}

	sids := make([]string, plan.sessions)
	for i := range sids {
		sids[i] = fmt.Sprintf("2f1c6f0e-0264-4000-8000-%012d", i)
	}

	// --- Fase 1: capacidade numa instância só -------------------------------------
	//
	// A QR pairing and not a resume, and the reason is what it leaves behind: the fake
	// engine answers a resume with a single event, and a series of one point cannot
	// disprove an order. A pairing emits several, and it also writes the device row, so
	// the sweep has something to join in the phase after this one.
	connectStart := time.Now()
	for i, sid := range sids {
		id := fmt.Sprintf("connect-%s-%d", active.id, i)
		// The connect goes on the session's own stream and the wake on the control one,
		// which is the pair a client sends and not two ways of saying the same thing. A
		// command on a session's stream reaches whoever owns that session, and nobody owns
		// one that has never run; the wake is what asks the fleet to take it. Sent in this
		// order because the other one adopts a session whose first command has not arrived
		// yet, and then waits for it.
		err := cl.send(ctx, cl.keys.Commands(sid), &protocol.Command{
			V: protocol.Version, ID: id, Type: protocol.CommandSessionConnect, SID: sid,
			TS: time.Now().UnixMilli(), ReplyTo: cl.keys.Reply(id),
			Payload: json.RawMessage(`{"pairing":"qr","groups":true}`),
		})
		if err != nil {
			return fmt.Errorf("%w: ask for session %s: %w", errSetup, sid, err)
		}
		err = cl.send(ctx, cl.keys.Control(), &protocol.Command{
			V: protocol.Version, ID: "wake-" + id, Type: protocol.CommandSessionWake, SID: sid,
			TS: time.Now().UnixMilli(), Payload: json.RawMessage(`{"desired":"connected"}`),
		})
		if err != nil {
			return fmt.Errorf("%w: wake session %s: %w", errSetup, sid, err)
		}
	}
	for i, sid := range sids {
		id := fmt.Sprintf("connect-%s-%d", active.id, i)
		reply, err := cl.await(ctx, id, 60*time.Second)
		if err != nil {
			return fmt.Errorf("%w: %w", errSetup, err)
		}
		if !reply.OK {
			return fmt.Errorf("%w: the fleet refused to connect %s: %w", errSetup, sid, reply.Error)
		}
	}
	rep.measure("capacidade numa instancia so", "tempo ate as sessoes conectarem",
		time.Since(connectStart).Seconds(), "s")

	// The starting state, measured before anything is killed. A phase that begins without
	// this is a phase measuring a recovery of sessions that were never up, and the number
	// it produces looks exactly like a good one.
	settled, err := settleStartingState(ctx, active, cl, []*instance{first}, sids, plan.sessions, 90*time.Second)
	if err != nil {
		return err
	}
	// More sessions running than exist is not a setup that did not settle: it is two
	// instances running one session, which is invariant 1, and it is the earliest place in
	// the run where a lease that stopped being exclusive shows. Recorded as the assertion
	// it is -- so the run exits 1 naming the invariant in words -- rather than as the
	// "machine was not ready" the count below would otherwise file it under.
	if settled.running > plan.sessions || settled.leases > plan.sessions {
		detail := fmt.Sprintf("a frota pediu %d sessoes e tem %d leases no Redis e %d somados em "+
			"wac_sessions_running, contra %d sids distintos: alguma sessao esta sendo rodada por "+
			"duas instancias ao mesmo tempo. Por instancia: %v",
			plan.sessions, settled.leases, settled.running, plan.sessions, settled.byInst)
		rep.assert(&assertion{
			invariant: "1 (uma instancia dona da sessao por vez, arbitrada pela lease)",
			claim:     "a frota inteira nunca roda mais sessoes do que existem sids",
			series:    fmt.Sprintf("%d sids pedidos, lidos em %d instancias vivas", plan.sessions, len(settled.byInst)),
			points:    plan.sessions,
			held:      false,
			detail:    detail,
		})
		return fmt.Errorf("%w: %s", errInvariantBroken, detail)
	}
	if settled.running != plan.sessions || settled.leases != plan.sessions || settled.desired != plan.sessions {
		return fmt.Errorf("%w: o estado de partida nao se estabeleceu: pedidas %d sessoes, e o fleet tem "+
			"%d com desired='connected' no PostgreSQL, %d leases no Redis e %d em wac_sessions_running",
			errSetup, plan.sessions, settled.desired, settled.leases, settled.running)
	}
	rep.note(fmt.Sprintf("condicoes desta corrida: %d sessoes, %d shards, %d processos, %d comandos em voo "+
		"por sessao na troca de dono, WAC_ENGINE=fake, WAC_HEARTBEAT=%s, WAC_LEASE_TTL=%s, "+
		"WAC_CLAIM_MIN_IDLE=%s, WAC_DATABASE_MAX_CONNS=%d. O custo da adocao em massa e, antes de tudo, um WAC_LEASE_TTL: "+
		"a lease do processo morto so vence depois dele.",
		plan.sessions, plan.shards, plan.processes, plan.sends,
		benchHeartbeat, benchLeaseTTL, benchClaimMinIdle, benchMaxConns))
	rep.note(fmt.Sprintf("estado de partida afirmado antes de qualquer morte: %d sessoes com desired='connected', "+
		"%d leases, soma de wac_sessions_running = %d", settled.desired, settled.leases, settled.running))

	// What one instance costs while it carries the sessions: the four the issue names.
	//
	// All four are about the connector and none about the bench. A goroutine count of
	// this process would be a number about the measuring instrument, which is the kind
	// of figure that gets copied into an issue as if it described the fleet.
	capacity, err := first.metrics(ctx, "wac_sessions_running", "wac_events_published_total", "go_goroutines")
	if err != nil {
		return fmt.Errorf("%w: %w", errSetup, err)
	}
	const phase1 = "capacidade numa instancia so"
	rep.measure(phase1, "sessoes na instancia", capacity["wac_sessions_running"], "sessoes")
	rep.measure(phase1, "eventos publicados ate aqui", capacity["wac_events_published_total"], "eventos")
	rep.measure(phase1, "goroutines da instancia", capacity["go_goroutines"], "goroutines")

	// Asked for on its own, because this one can legitimately be absent. MEASURED on
	// darwin/arm64: with `CGO_ENABLED=0` the process collector's memory reader returns
	// `errNotImplemented` and the series is missing from a healthy endpoint. A deployment
	// runs Linux and has it. Reading a missing metric as zero would print "0.00 MiB" for a
	// running fleet; refusing the run over it would make the whole bench unusable on this
	// machine, and neither is the truth, which is that the number was not available.
	if resident, err := first.metrics(ctx, "process_resident_memory_bytes"); err == nil {
		rep.measure(phase1, "memoria residente da instancia",
			resident["process_resident_memory_bytes"]/(1<<20), "MiB")
	} else {
		rep.note("memoria residente: sem medida nesta corrida. O /metrics de " + first.name +
			" nao traz process_resident_memory_bytes, o que acontece num build darwin com " +
			"CGO_ENABLED=0. Um zero aqui seria uma frota sem memoria nenhuma.")
	}
	backends, err := poolBackends(ctx, active)
	if err != nil {
		return err
	}
	rep.measure(phase1, "conexoes da instancia no PostgreSQL", float64(backends), "conexoes")
	rep.measurements[len(rep.measurements)-1].expected = fmt.Sprintf("teto WAC_DATABASE_MAX_CONNS=%d", benchMaxConns)
	rep.measurements[len(rep.measurements)-1].outside = backends > benchMaxConns
	for _, depth := range shardDepths(ctx, cl, plan.shards) {
		rep.measure(phase1, depth.name, depth.entries, "entradas")
	}

	// Invariant 5 asked for on purpose, while one instance owns everything and nothing has
	// been killed yet. The handover below produces redeliveries, and a redelivery of a
	// command its owner never answered is carried out once: the ledger is never consulted,
	// so the claim would come out NAO MEDIDO with eight commands behind it.
	pairs, err := exerciseIdempotency(ctx, active, cl, sids)
	if err != nil {
		return err
	}

	// --- Fase 2: adoção em massa ---------------------------------------------------
	//
	// The process goes without a chance to hand anything back, which is the case the
	// sweep exists for: the leases die with their holder and what brings the accounts
	// back is the record each connect left in the store.
	if err := first.kill(); err != nil {
		return fmt.Errorf("%w: %w", errSetup, err)
	}
	rep.note("fase de adocao em massa: a instancia " + first.name + " foi morta com SIGKILL, sem hand-back")

	second, err := group.start(ctx, "bench-"+active.id+"-b")
	if err != nil {
		return fmt.Errorf("%w: %w", errSetup, err)
	}
	rep.processes = append(rep.processes, noteOf(second))

	adoptionStart := time.Now()
	back, adopted := 0, time.Duration(0)
	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		running, err := second.metrics(ctx, "wac_sessions_running")
		if err != nil {
			return fmt.Errorf("%w: %w", errSetup, err)
		}
		back = int(running["wac_sessions_running"])
		if back >= plan.sessions {
			adopted = time.Since(adoptionStart)
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if back < plan.sessions {
		return fmt.Errorf("%w: a adocao em massa trouxe %d das %d sessoes em %s; sem o estado de partida "+
			"de volta nao ha o que medir na fase seguinte", errSetup, back, plan.sessions, time.Since(adoptionStart))
	}
	rep.measure("adocao em massa", "tempo ate as sessoes voltarem", adopted.Seconds(), "s")
	rep.measure("adocao em massa", "sessoes trazidas de volta", float64(back), "sessoes")
	if plan.maxAdoption > 0 {
		outside := adopted > plan.maxAdoption
		rep.measurements[len(rep.measurements)-2].expected = "<= " + plan.maxAdoption.String()
		rep.measurements[len(rep.measurements)-2].outside = outside
	}

	return handover(ctx, active, group, cl, rep, plan, second, sids, pairs)
}

// startingState is the four readings s5 asks for, taken from the four places that hold
// them, so that a number the bench prints can be checked by hand against the server.
type fleetState struct {
	desired int
	leases  int
	running int
	byInst  map[string]int
}

// settleStartingState waits for the four readings to agree, and gives up saying so.
//
// A wait and not a read, because one of the four is a gauge the connector writes once a
// heartbeat: read the instant the last connect is answered, it still says what it said
// before any of them arrived. Bounded, because "not settled yet" and "not going to
// settle" have to be told apart by something, and a deadline is the only thing out here
// that can tell them apart.
//
// What it must not become is a retry that eventually agrees with itself. The verdict when
// the deadline passes is the last reading, whatever it says, and the caller reports SETUP
// INCOMPLETO with the numbers rather than starting a phase over sessions that never came
// up.
func settleStartingState(ctx context.Context, active *run, cl *client, live []*instance, sids []string,
	want int, within time.Duration) (fleetState, error) {

	deadline := time.Now().Add(within)
	for {
		state, err := startingState(ctx, active, cl, live, sids)
		if err != nil {
			return state, err
		}
		if state.desired == want && state.leases == want && state.running == want {
			return state, nil
		}
		// Waiting cannot bring an over-count down to the number of sids that exist, and
		// the caller has a verdict for it that is not "did not settle". Spending the
		// deadline first would only delay the red by a minute and a half.
		if state.running > want || state.leases > want {
			return state, nil
		}
		if time.Now().After(deadline) {
			return state, nil
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func startingState(ctx context.Context, active *run, cl *client, live []*instance, sids []string) (fleetState, error) {
	state := fleetState{byInst: map[string]int{}}

	held, err := cl.leaseHolders(ctx, sids)
	if err != nil {
		return state, fmt.Errorf("%w: %w", errSetup, err)
	}
	state.leases = len(held)

	for _, one := range live {
		found, err := one.metrics(ctx, "wac_sessions_running")
		if err != nil {
			return state, fmt.Errorf("%w: %w", errSetup, err)
		}
		state.running += int(found["wac_sessions_running"])
	}

	registry, err := cl.instances(ctx)
	if err != nil {
		return state, fmt.Errorf("%w: %w", errSetup, err)
	}
	for name, fields := range registry {
		if count, err := strconv.Atoi(fields["sessions"]); err == nil {
			state.byInst[name] = count
		}
	}

	desired, err := desiredConnected(ctx, active)
	if err != nil {
		return state, err
	}
	state.desired = desired
	return state, nil
}

// shardDepth is one shard's length, kept in shard order rather than in a map: a report
// whose lines move between two runs of the same tree is a report nobody diffs.
type shardDepth struct {
	name    string
	entries float64
}

func shardDepths(ctx context.Context, cl *client, shards int) []shardDepth {
	depths := make([]shardDepth, 0, shards)
	for shard := range shards {
		read, err := cl.eventsOn(ctx, shard)
		if err != nil {
			continue
		}
		depths = append(depths, shardDepth{
			name:    "profundidade do shard " + strconv.Itoa(shard),
			entries: float64(read.length),
		})
	}
	return depths
}

func noteOf(i *instance) processNote {
	return processNote{instance: i.name, pid: i.pid, httpAddr: i.httpAddr, logPath: i.logPath}
}
