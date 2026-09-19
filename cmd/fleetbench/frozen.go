package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// The fourth phase: the owner that lost the lease and did not die.
//
// # Why a kill is not enough
//
// Invariant 1 has two halves. One is that two instances never hold one lease, and the
// handover above reaches it. The other is the fence: an instance that loses the lease
// stops publishing that session at once, because an event under a lower epoch arriving
// after a higher one is the one thing a client cannot recover from.
//
// A killed owner cannot break the fence. It publishes nothing more, so the assertion runs
// over a series where it could not have failed -- and measured, that is exactly what
// happened: the mutant that removes the fence from `session.publish` came out green
// against a bench whose only handover was a SIGKILL.
//
// # What produces it
//
// A frozen owner. SIGSTOP leaves a live process that stops renewing; its lease expires on
// the server, a peer takes the sessions and publishes under a higher epoch, and SIGCONT
// hands the old owner back the work it was in the middle of. Whether it publishes that
// work is the fence, and the order of the shard's stream is what makes the answer visible
// from outside.
//
// The freeze has to outlast the lease, which is why it is the lease TTL plus a margin
// rather than a number picked to feel long enough.
func frozenOwner(ctx context.Context, active *run, cl *client, rep *report, plan benchPlan,
	owner *instance, peers []*instance, sids []string, counted *census) error {

	// Who is frozen is measured, not assumed. The handover leaves the sessions split
	// between the peers however the sweep happened to fall, and freezing one that owns
	// nothing proves nothing: nobody has to take anything from it, and the phase spends a
	// minute to report that nobody did.
	holding := map[string]int{}
	frozen, most := owner, -1
	for _, candidate := range append([]*instance{owner}, peers...) {
		found, err := candidate.metrics(ctx, "wac_sessions_running")
		if err != nil {
			return fmt.Errorf("%w: %w", errSetup, err)
		}
		count := int(found["wac_sessions_running"])
		holding[candidate.name] = count
		if count > most {
			frozen, most = candidate, count
		}
	}
	watching := make([]*instance, 0, len(peers))
	for _, candidate := range append([]*instance{owner}, peers...) {
		if candidate != frozen {
			watching = append(watching, candidate)
		}
	}
	held := 0
	for _, candidate := range watching {
		held += holding[candidate.name]
	}
	rep.note(fmt.Sprintf("fase do dono congelado: antes de congelar, a frota estava assim: %v. "+
		"Congelada a que mais segura, %s, com %d sessoes.", holding, frozen.name, most))
	if most == 0 {
		rep.note("fase do dono congelado: nenhuma instancia viva segurava sessao nenhuma, entao nao ha " +
			"posse a perder e a metade da cerca da invariante 1 fica SEM MEDIDA nesta corrida")
		return nil
	}
	owner, peers = frozen, watching

	// Work for the owner to be holding when it freezes. Sent and not waited for: a
	// command already answered is not work in progress, and what this phase needs is an
	// owner with something of its own to publish when it comes back.
	sent := 0
	for _, sid := range sids {
		for n := range plan.sends {
			id := fmt.Sprintf("frozen-%s-%s-%d", active.id, shortSID(sid), n)
			payload := fmt.Sprintf(`{"message_id":%q,"to":{"kind":"phone","id":"5511999990002"},`+
				`"content":{"type":"text","body":"congelado %d"}}`, id, n)
			err := cl.send(ctx, cl.keys.Commands(sid), &protocol.Command{
				V: protocol.Version, ID: id, Type: protocol.CommandMessageSend, SID: sid,
				TS: time.Now().UnixMilli(), ReplyTo: cl.keys.Reply(id), Payload: json.RawMessage(payload),
			})
			if err != nil {
				return fmt.Errorf("%w: put work in front of the owner about to freeze: %w", errSetup, err)
			}
			sent++
		}
	}

	if err := owner.freeze(); err != nil {
		return fmt.Errorf("%w: %w", errSetup, err)
	}
	frozenAt := time.Now()

	// Held until a peer has actually taken the sessions, and not for a number of seconds
	// picked to feel long enough.
	//
	// MEASURED on this tree: a fixed hold of `lease TTL + 20 s` expired the leases and no
	// peer adopted anything, because the fleet's resume sweep runs every 30 s and the
	// `wa:resume:<sid>` turn the frozen instance had taken lasts a minute. A wait on the
	// clock reported "nobody took them" about a fleet that was going to take them
	// shortly, and the phase's precondition failed for a reason with nothing to do with
	// the fence.
	//
	// Bounded, because "not yet" and "not going to" still have to be told apart, and a
	// deadline is the only thing out here that can. A deadline that passes is reported
	// rather than spent as a pass.
	//
	// The delta and not the total: a peer that already held sessions of its own reports a
	// number above zero whether or not it took anything from the frozen one, and reading
	// the total would let the phase find its own precondition met.
	deadline := time.Now().Add(benchLeaseTTL + 3*time.Minute)
	var taken int
	var after map[string]int
	for {
		taken, after = 0, map[string]int{}
		var failed error
		for _, peer := range peers {
			found, err := peer.metrics(ctx, "wac_sessions_running")
			if err != nil {
				failed = err
				break
			}
			after[peer.name] = int(found["wac_sessions_running"])
			taken += int(found["wac_sessions_running"])
		}
		if failed != nil {
			_ = owner.thaw()
			return fmt.Errorf("%w: %w", errSetup, failed)
		}
		taken -= held
		counted.take(ctx, "dono congelado", peers)
		if taken > 0 || time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			_ = owner.thaw()
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	hold := time.Since(frozenAt)
	rep.note(fmt.Sprintf("fase do dono congelado: depois de %s congelada, os pares estavam assim: %v "+
		"(seguravam %d antes, entao a diferenca e %d)", hold.Round(time.Second), after, held, taken))

	if err := owner.thaw(); err != nil {
		return fmt.Errorf("%w: %w", errSetup, err)
	}

	// Long enough for whatever the thawed owner is going to publish to reach the stream.
	// A wait on the fleet, and the assertion that follows reads the stream's own order
	// rather than trusting that this was long enough.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(15 * time.Second):
	}

	// Several, spaced, and not one. "Sustained" is decided by comparing two readings more
	// than two heartbeats apart, so a single census at the end of the phase can never be
	// sustained however wrong the number is: MEASURED, a mutant that leaves the thawed
	// owner running its sessions produced a fleet total of 8 over 4 sids here, in one
	// sample, and the rule discarded it as gauge lag.
	for range 8 {
		counted.take(ctx, "fim da fase do dono congelado", append([]*instance{owner}, peers...))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	rep.measure("dono congelado", "tempo congelado ate um par assumir", hold.Seconds(), "s")
	rep.measure("dono congelado", "sessoes que os pares assumiram com o dono congelado",
		float64(taken), "sessoes")
	rep.measure("dono congelado", "comandos postos na frente do dono antes do congelamento",
		float64(sent), "comandos")

	if taken == 0 {
		rep.note(fmt.Sprintf("fase do dono congelado: %s ficou parada %s, mais que o WAC_LEASE_TTL de %s, "+
			"e ainda assim nenhum par assumiu sessao nenhuma. Entao o que ela publicar ao voltar "+
			"nao e publicacao sem posse, e a metade da cerca da invariante 1 fica SEM MEDIDA nesta corrida.",
			owner.name, hold.Round(time.Second), benchLeaseTTL))
		return nil
	}
	rep.note(fmt.Sprintf("fase do dono congelado: %s ficou parada %s com %d comandos na frente dela; "+
		"os pares assumiram %d sessoes nesse intervalo e so entao ela foi solta. O que ela publicar "+
		"depois disso e publicacao de quem ja nao tem a lease, e a ordem do stream do shard e o que "+
		"torna isso visivel daqui.", owner.name, hold.Round(time.Second), sent, taken))
	return nil
}
