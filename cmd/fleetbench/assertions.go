package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/transport/redisstream"
)

// The claims the issue asks of the third measurement, each with the series it looked at
// and a verdict, or with the reason it was not measured.
//
// Never both and never neither: an item with no series is not an assertion that passed,
// it is an assertion that never ran, and printing it as a pass is how a bench ends up
// saying more than it measured.
func assertInvariants(ctx context.Context, cl *client, rep *report, plan benchPlan,
	answers map[string][]string, sids []string, pairs []idempotentPair, counted *census,
	fence *fenceOutcome, stillWorking, expired, stillPublishing string) error {

	published := map[string][]protocol.Event{}          // sid -> events, in stream order
	shardOf := map[string]map[string]bool{}             // sid -> streams it was seen on
	inOrder := map[string]map[string][]protocol.Event{} // sid -> stream -> that stream's order
	firstOn := map[string]map[string]string{}           // sid -> stream -> first event id there
	truncated := map[string]bool{}
	var totalEvents int

	for shard := range plan.shards {
		read, err := cl.eventsOn(ctx, shard)
		if err != nil {
			return fmt.Errorf("%w: %w", errSetup, err)
		}
		// A stream at the trim cap is a stream that has dropped its oldest entries. That
		// is a different finding from a lost event, and the two are told apart here
		// rather than after the fact: a hole under a trimmed stream says nothing about
		// the publisher.
		wasTrimmed := read.length >= redisstream.DefaultEventMaxLen
		for _, event := range read.events {
			published[event.SID] = append(published[event.SID], event)
			if shardOf[event.SID] == nil {
				shardOf[event.SID] = map[string]bool{}
			}
			shardOf[event.SID][read.stream] = true
			if inOrder[event.SID] == nil {
				inOrder[event.SID] = map[string][]protocol.Event{}
				firstOn[event.SID] = map[string]string{}
			}
			inOrder[event.SID][read.stream] = append(inOrder[event.SID][read.stream], event)
			if _, seen := firstOn[event.SID][read.stream]; !seen {
				firstOn[event.SID][read.stream] = event.ID
			}
			if wasTrimmed {
				truncated[event.SID] = true
			}
			totalEvents++
		}
		rep.measure("troca de dono sob carga", "entradas em "+read.stream, float64(read.length), "entradas")
	}

	// Read after the streams, and the order is the safe one. An acquire landing between the
	// two reads can only push a counter up, so the gap can hide a regression and cannot
	// invent one, and a false positive here would be a bench calling a healthy fleet broken.
	counters, err := cl.epochCounters(ctx, sids)
	if err != nil {
		return fmt.Errorf("%w: %w", errSetup, err)
	}

	// Marked from here, and not over the whole report: the fence claim was asserted by the
	// frozen phase and its series is adoptions, not stream entries. Blanketing the report
	// would say a claim went unmeasured because of something it never read.
	fromStreams := len(rep.assertions)
	assertOneOwner(rep, published, inOrder, counted, len(sids), fence)
	assertEpochRises(rep, published)
	assertEpochCounter(rep, published, counters, cl.keys.LeaseEpoch("<sid>"))
	assertSeqMonotonic(rep, published)
	assertOneShard(rep, shardOf, firstOn, sids)
	assertNoLostEvent(rep, published, truncated)

	// Everything above walks the event streams, so a snapshot taken while they were still
	// growing is a snapshot none of them can give a verdict over: the stale-owner event
	// that is missing, or the seq that would close a gap, may simply not have arrived yet.
	// A note does not reach the exit code, and a run that ends VERDE over a partial
	// sequence says it measured an ordering it only half read.
	markUnmeasured(rep.assertions[fromStreams:], stillPublishing)
	assertNoDuplicateEffect(rep, answers, pairs, expired)
	return assertConsumerGroups(ctx, cl, rep, sids, stillWorking)
}

// Invariant 1: one instance owns a session at a time, and losing the lease stops it.
//
// Two halves, because the invariant has two and one of them is invisible to the other.
//
// The first: for one `(sid, epoch)`, every event carries the same publisher. An epoch is a
// generation of ownership, so two instances publishing under one generation are two
// holders of the same lease, whatever the lease key happened to say the moment somebody
// looked at it.
//
// The second is the fence: an instance that lost the lease has to stop publishing at once.
// It does not show in the first half at all -- the old owner keeps publishing under its own
// old epoch, so every `(sid, epoch)` still has exactly one publisher and the check passes
// while two processes write to the same session. What shows it is the order of the stream:
// once an event of a higher epoch is in, an event of a lower one arriving after it came
// from an instance that had already been replaced.
//
// Read per stream and not across them, because "after" only means something inside one
// stream. A session on two streams is a different finding, and `assertOneShard` is the one
// that makes it.
// The fence claim comes in rather than the late count going out, because the two are one
// reading. `late` is counted here and nowhere else, and a caller that took it and forgot
// to pass it on is the same branch that leaves a run green over a fence it never reached
// -- the class the deferred claim in the frozen phase exists to remove. Passed in, the
// only place that can forget it is this function, which a table can disprove.
func assertOneOwner(rep *report, published map[string][]protocol.Event,
	inOrder map[string]map[string][]protocol.Event, counted *census, sids int,
	fence *fenceOutcome,
) {

	pairs, offenders := 0, []string{}
	for sid, events := range published {
		byEpoch := map[uint64]map[string]bool{}
		for _, event := range events {
			if byEpoch[event.Epoch] == nil {
				byEpoch[event.Epoch] = map[string]bool{}
			}
			byEpoch[event.Epoch][event.Inst] = true
		}
		for epoch, holders := range byEpoch {
			pairs++
			if len(holders) > 1 {
				offenders = append(offenders, fmt.Sprintf(
					"%s foi publicado sob o epoch %d por %s, e um epoch e uma geracao de posse: "+
						"dois publicadores nele sao dois donos da mesma lease",
					sid, epoch, strings.Join(sorted(holders), " e ")))
			}
		}
	}

	// One late event from a previous owner is allowed. A SECOND one from the same instance
	// is not, and the difference is the fence.
	//
	// The contract says as much, and names this exact case: a client keeps the highest
	// epoch it has seen and drops every event below it, "which is what stops a late event
	// from a previous owner overwriting the state of the instance running the account now
	// -- a paused process, a socket that outlived its lease, a delivery that sat in a
	// queue". `Session.stillOwned` is the connector's side of that trade: the ownership
	// check happens after the publish as well as before it, because a lease can run out
	// while the write is in flight, and what the connector does then is refuse the
	// acknowledgement so the message comes back on a redelivery.
	//
	// So a run that reported the first late event as a broken invariant was reporting the
	// contract's own guarantee. MEASURED over this bench: a clean tree produces exactly one
	// per (sid, instance) -- the write that was in flight when the freeze landed -- while
	// the mutant that removes the fence from `session.publish` produces 246, and the one
	// that lets two instances hold a lease produces 269. What separates them is not a
	// threshold somebody picked: after the first one the connector tears the session down
	// ("lost a lease; stopping the session"), so a second event from that instance for that
	// session is the fence not having acted.
	fenced, late := 0, 0
	for sid, streams := range inOrder {
		for stream, events := range streams {
			var highest uint64
			var highestBy, highestID string
			// Counted per (instance, epoch) and not per instance, because an epoch is a
			// generation of ownership and the allowance belongs to the generation. An
			// instance can take a session, lose it with a write in flight, take it again
			// under a higher epoch and lose it again the same way: two late events, from
			// one instance, each of them the write the contract allows. Added up under the
			// instance's name they read as the fence having failed, and the fleet that
			// produces them is the ordinary one where the sweep hands a session back to a
			// process that already had it. Within ONE generation there is no second
			// allowance: the connector tears the session down when it detects the first.
			//
			// Distinct ids, because the transport is allowed to hand the same event over
			// twice: a retried XADD whose first answer was lost puts the same late event on
			// the stream again, and counted as two this would report the contract's own
			// at-least-once delivery as the fence having failed. The same reason
			// `assertSeqMonotonic` discounts a repeat before comparing.
			stale := map[string]map[string]bool{}
			for _, event := range events {
				if event.Epoch < highest {
					late++
					generation := fmt.Sprintf("%s@%d", event.Inst, event.Epoch)
					if stale[generation] == nil {
						stale[generation] = map[string]bool{}
					}
					stale[generation][event.ID] = true
					if len(stale[generation]) > 1 {
						offenders = append(offenders, fmt.Sprintf(
							"%s: a instancia %s publicou o evento %s sob o epoch %d em %s DEPOIS de %s ja "+
								"ter publicado o evento %s sob o epoch %d, e essa ja e a %da vez dela SOB "+
								"ESSE MESMO epoch nesta sessao: a primeira e a escrita que estava em voo, "+
								"que o contrato preve e o cliente descarta pelo cursor, mas ao detecta-la o "+
								"conector derruba a sessao, entao a segunda e a cerca nao tendo agido",
							sid, event.Inst, event.ID, event.Epoch, stream, highestBy, highestID, highest,
							len(stale[generation])))
					}
					continue
				}
				if event.Epoch > highest {
					highest, highestBy, highestID = event.Epoch, event.Inst, event.ID
				}
			}
			fenced += len(events)
		}
	}
	if late > 0 {
		rep.note(fmt.Sprintf("%d evento(s) de dono anterior apareceram depois de um epoch mais alto no "+
			"mesmo stream. Isso o contrato preve -- o cliente guarda o epoch mais alto que viu e descarta "+
			"o que vier abaixo -- e e a escrita que estava em voo quando a posse mudou; o que esta "+
			"afirmado e que nenhuma instancia publicou uma SEGUNDA vez depois disso, porque ai a cerca "+
			"nao teria agido", late))
	}

	// The third reading, and the only one that sees two holders who never shared an
	// epoch. Every acquisition runs an `INCR`, so a second holder publishes under a
	// higher epoch and the per-epoch check above stays green while two instances run the
	// same account. What says otherwise is arithmetic: a fleet with N distinct sessions
	// cannot have its instances add up to more than N.
	top, taken := counted.worst()
	above, sustained := counted.over(sids)
	switch {
	case sustained:
		lines := make([]string, 0, len(above)+1)
		lines = append(lines, fmt.Sprintf(
			"a frota inteira disse estar rodando mais sessoes do que existem sids (%d distintos), e a "+
				"sobra se manteve por mais de dois heartbeats, entao nao e o atraso do gauge: alguma "+
				"sessao esta sendo rodada por mais de uma instancia ao mesmo tempo. Os %d censos acima "+
				"do limite foram:", sids, len(above)))
		for _, sample := range above {
			lines = append(lines, "  "+sample.String())
		}
		offenders = append(offenders, strings.Join(lines, "\n"))
	case len(above) > 0:
		// Reported and not asserted on. A gauge written once a heartbeat can show one
		// instance still counting a session the next one already counts, and that is a
		// fact about when the numbers were written rather than about who owns the
		// account.
		rep.note(fmt.Sprintf("%d censo(s) da frota passaram de %d sessoes sem se sustentar por dois "+
			"heartbeats, o que e o atraso esperado de um gauge escrito uma vez por tick e nao posse "+
			"dupla. O mais alto foi %s", len(above), sids, top))
	}

	// The half this claim does not reach, said out loud rather than left to the label.
	//
	// Invariant 1 fences two things when a lease moves: publishing and writing to the
	// store. The streams show the first, because an event carries the instance that wrote
	// it. They cannot show the second: MEASURED, no table of the store carries an instance
	// or an epoch in a column, so a row written by an instance that had already lost the
	// lease is indistinguishable, after the fact, from one the new owner wrote.
	rep.note("metade do store da invariante 1 (escrita fencida depois de perder a lease): NAO MEDIDA. " +
		"Nenhuma tabela do store carrega instancia ou epoch em coluna, entao uma linha escrita por quem " +
		"ja perdeu a lease nao se distingue, depois do fato, de uma escrita pelo dono novo. O que esta " +
		"afirmado abaixo e a metade que publica, que os streams mostram porque o evento carrega o inst.")

	rep.assert(&assertion{
		invariant: "1 (uma instancia dona da sessao por vez, arbitrada pela lease; perder a lease cerca a publicacao na hora; a metade do store fica sem medida, ver notas)",
		claim: "nunca duas instancias com a mesma lease: um epoch, um publicador; nenhuma instancia " +
			"publicando de novo depois de um evento de epoch velho (o primeiro e a escrita em voo, que " +
			"o contrato preve); e a frota somada nunca roda mais sessoes do que existem sids",
		series: fmt.Sprintf("%d pares (sid, epoch) sobre %d sessoes, %d eventos lidos na ordem do stream deles, "+
			"e %d censos da frota (o mais alto somou %d de %d sids)",
			pairs, len(published), fenced, taken, top.total, sids),
		points: pairs + taken,
		held:   len(offenders) == 0,
		detail: strings.Join(offenders, "\n"),
		notWhy: ifEmpty(pairs+taken, "nenhum evento foi publicado e nenhum censo foi tomado"),
	})
	fenceLateEvents(fence.claim, late)
}

// fenceLateEvents puts, on the frozen phase's claim, how many events this run saw
// published under an epoch the stream had already passed.
//
// The phase claims a CONDITION and not an exercise: a peer took sessions from an owner
// that was alive and stopped, which no kill can produce and which is the only state the
// guarded line can run in. It used to claim the exercise on that alone, and an adoption is
// not one -- the thawed owner, whose pending entries the peers claim after
// WAC_CLAIM_MIN_IDLE, usually comes back with nothing left to publish.
//
// The count goes in the series rather than into the verdict, because zero is ambiguous
// from out here: it is what an owner with nothing to say produces AND what a fence that
// works produces, since the session is torn down on the lost lease before a publish gets
// out. MEASURED over this bench: a clean tree produced zero in 83 of 87 runs and one in
// the other four, while the mutant that removes the fence from `session.publish` produced
// 246 in a single run -- without it the old owner keeps every session alive and keeps
// emitting, so what separates the two is a flood and not one in-flight write. The pair is
// therefore sensitive, and what shows it is the mutation battery, not this claim.
func fenceLateEvents(claim *assertion, late int) {
	if claim == nil {
		return
	}
	claim.series += fmt.Sprintf("; %d evento(s) publicado(s) sob epoch ja vencido nesta corrida", late)
}

// Invariant 2: the epoch rises on every ownership change.
//
// Read as: walking one session's events in stream order, the moment the publisher changes
// the epoch has to be higher than every epoch that session has carried. A client drops
// state from a stale epoch, so an epoch that repeats across a change is a client accepting
// the old owner's state as current.
func assertEpochRises(rep *report, published map[string][]protocol.Event) {
	// Read over the events a client would accept, which is not every event in the stream.
	//
	// A client keeps the highest epoch it has seen and drops everything below it, so a late
	// write from a previous owner never reaches its state. Counted here, that same event
	// looks like ownership moving back to the old instance under a lower epoch -- and the
	// run reported "the epoch did not rise" about a change that never happened, on a fleet
	// where the only thing that happened was a write that was in flight when the freeze
	// landed. Whether that late event is allowed at all is `assertOneOwner`'s question,
	// and it is answered there; this one is about the epoch of an actual handover.
	//
	// What this reading cannot decide, and what `assertEpochCounter` is for: a handover to
	// an instance that publishes exactly ONE event under a regressed epoch has, in the
	// stream, the same shape as the in-flight write the contract allows -- one event below
	// the highest, and nothing after it from that publisher. Nothing in the stream tells
	// the two apart, so the counter the epoch is handed out from is read instead.
	changes, offenders, dropped := 0, []string{}, 0
	for sid, events := range published {
		var lastInst string
		var highest uint64
		for _, event := range events {
			if event.Epoch < highest {
				dropped++
				continue
			}
			if lastInst != "" && event.Inst != lastInst {
				changes++
				if event.Epoch <= highest {
					offenders = append(offenders, fmt.Sprintf(
						"%s mudou de %s para %s e o epoch nao subiu: %d, contra %d ja visto",
						sid, lastInst, event.Inst, event.Epoch, highest))
				}
			}
			if event.Epoch > highest {
				highest = event.Epoch
			}
			lastInst = event.Inst
		}
	}
	if dropped > 0 {
		rep.note(fmt.Sprintf("%d evento(s) de epoch inferior ao mais alto ja visto ficaram de fora desta "+
			"leitura, porque e isso que um cliente faz com eles. Contados, cada um pareceria uma troca "+
			"de dono de volta para a instancia velha. Uma posse que comecasse com epoch regredido e "+
			"publicasse um evento so tem essa mesma forma no stream, e quem a separa da escrita em voo "+
			"e a afirmacao do contador de epoch, logo abaixo", dropped))
	}
	rep.assert(&assertion{
		invariant: "2 (todo evento carrega o epoch do dono, e ele sobe em toda troca de posse)",
		claim: "epoch estritamente crescente a cada troca de dono, entre os eventos que um cliente " +
			"aceita (o epoch de onde eles saem e afirmado pelo contador, na linha seguinte)",
		series: fmt.Sprintf("%d trocas de dono observadas sobre %d sessoes", changes, len(published)),
		points: changes,
		held:   len(offenders) == 0,
		detail: strings.Join(offenders, "\n"),
		notWhy: ifEmpty(changes, "nenhuma troca de dono apareceu nos streams, entao nao havia o que "+
			"afirmar: uma asserção de epoch sem troca nao tem como ficar vermelha"),
	})
}

// Invariant 2, at the source: the counter an epoch is handed out from never falls below an
// epoch that session has already published under.
//
// `assertEpochRises` reads the streams the way a client does, and there is one case that
// reading cannot decide (its own comment says which). The epoch is not a number the events
// carry from nowhere: it is `INCR <prefix>lease-epoch:<sid>`, monotonic by construction, so
// the only way it falls is the key being deleted or lost -- `ForgetEpoch` firing on a
// session that is not gone is the reachable one. A counter sitting below an epoch already
// on the wire means the next acquire hands out a generation that has been published under,
// and a client keeping the highest epoch it has seen would drop that new owner's whole
// session. That is invariant 2 failing at the source, before an event exists to read it
// from, and it is red here whatever the streams looked like.
//
// What it does NOT catch, and the claim says so by naming its series: a regression that was
// climbed back over before this read, which needs an acquire that published nothing under
// the reissued generation -- had it published, `assertOneOwner` would have two instances
// under one epoch to show.
func assertEpochCounter(rep *report, published map[string][]protocol.Event,
	counters map[string]uint64, keyShape string,
) {
	offenders, checked := epochCounterOffenders(published, counters)
	rep.assert(&assertion{
		invariant: "2 (todo evento carrega o epoch do dono, e ele sobe em toda troca de posse)",
		claim: "o contador de onde sai o epoch nunca esta abaixo do epoch mais alto que aquela " +
			"sessao ja publicou, lido ao fim da corrida",
		series: fmt.Sprintf("%d sessoes com evento publicado, cada uma contra o seu %s",
			checked, keyShape),
		points: checked,
		held:   len(offenders) == 0,
		detail: strings.Join(offenders, "\n"),
		notWhy: ifEmpty(checked, "nenhuma sessao publicou evento, entao nao ha epoch no stream "+
			"para comparar com contador nenhum"),
	})
}

// epochCounterOffenders is the decision, out of the assertion so that a test reaches it
// without a fleet: the phases are processes and sockets, and a table is what says which
// shapes are offenders.
func epochCounterOffenders(published map[string][]protocol.Event,
	counters map[string]uint64,
) (offenders []string, checked int) {
	sids := make([]string, 0, len(published))
	for sid := range published {
		sids = append(sids, sid)
	}
	sort.Strings(sids)

	for _, sid := range sids {
		var highest uint64
		var by string
		for _, event := range published[sid] {
			if event.Epoch > highest {
				highest, by = event.Epoch, event.Inst
			}
		}
		// An epoch of zero is no epoch: the counter starts at 1 on the first `INCR`, so a
		// session whose events all carry zero never had a generation to compare against,
		// and calling that a regression would be a finding about a session nobody owned.
		if highest == 0 {
			continue
		}
		checked++
		counter, present := counters[sid]
		if !present {
			offenders = append(offenders, fmt.Sprintf(
				"%s publicou sob o epoch %d (%s) e nao tem contador de epoch nenhum: a proxima "+
					"posse comeca de novo no 1 e reemite geracoes que ja sairam no stream",
				sid, highest, by))
			continue
		}
		if counter < highest {
			offenders = append(offenders, fmt.Sprintf(
				"%s tem o contador de epoch em %d, abaixo do epoch %d que %s ja publicou: a "+
					"proxima posse recebe uma geracao que ja saiu no stream, e um cliente "+
					"descarta a sessao inteira do dono novo",
				sid, counter, highest, by))
		}
	}
	return offenders, checked
}

// Invariant 3, first half: `seq` is monotonic per `(sid, epoch)`.
func assertSeqMonotonic(rep *report, published map[string][]protocol.Event) {
	points, withOrder, offenders := 0, 0, []string{}
	for sid, events := range published {
		type key struct {
			epoch uint64
		}
		byEpoch := map[key][]protocol.Event{}
		for _, event := range events {
			byEpoch[key{event.Epoch}] = append(byEpoch[key{event.Epoch}], event)
		}
		for k, run := range byEpoch {
			points += len(run)
			if len(run) > 1 {
				withOrder++
			}
			// The same event id twice is redelivery, and the contract allows it.
			//
			// `contract/PROTOCOL.md`: delivery is at-least-once per event, and `seq` is
			// what lets the consumer drop the copy the transport handed over twice. Redis
			// can produce one without anything being wrong -- an XADD that ran and whose
			// answer was lost comes back on the retry -- so reading a repeated id as a
			// broken invariant would report the contract's own guarantee as a defect.
			//
			// What is NOT allowed is two DIFFERENT events under one seq, and that is the
			// defect this looks for: the id is carried into the comparison rather than
			// dropped with the duplicate.
			seen := map[string]bool{}
			ordered := make([]protocol.Event, 0, len(run))
			for _, event := range run {
				if seen[event.ID] {
					continue
				}
				seen[event.ID] = true
				ordered = append(ordered, event)
			}
			atSeq := map[uint64]string{}
			for _, event := range ordered {
				if other, clash := atSeq[event.Seq]; clash {
					offenders = append(offenders, fmt.Sprintf(
						"%s no epoch %d: dois eventos diferentes com seq %d (%s e %s), e um seq so "+
							"nomeia um evento", sid, k.epoch, event.Seq, other, event.ID))
					continue
				}
				atSeq[event.Seq] = event.ID
			}
			// Strictly less, and not less-or-equal: two different events sharing a seq is
			// the check above, which names both ids. Asking for it here as well would be
			// two lines answering one question, and neither of them provable on its own --
			// removing either leaves the other covering the case, so nothing says which is
			// load-bearing.
			for i := 1; i < len(ordered); i++ {
				if ordered[i].Seq < ordered[i-1].Seq {
					offenders = append(offenders, fmt.Sprintf(
						"%s no epoch %d: seq %d (evento %s) veio depois de seq %d (evento %s)",
						sid, k.epoch, ordered[i].Seq, ordered[i].ID, ordered[i-1].Seq, ordered[i-1].ID))
				}
			}
		}
	}
	rep.assert(&assertion{
		invariant: "3 (seq monotonico por (sid, epoch))",
		claim: "seq estritamente crescente dentro de cada (sid, epoch), com a reentrega do mesmo " +
			"evento descontada porque o contrato a permite",
		series: fmt.Sprintf("%d eventos, dos quais %d pares (sid, epoch) com dois ou mais pontos",
			points, withOrder),
		points: withOrder,
		held:   len(offenders) == 0,
		detail: strings.Join(offenders, "\n"),
		notWhy: ifEmpty(withOrder, "nenhum (sid, epoch) teve mais de um evento, e uma ordem sobre um ponto "+
			"so nao tem como ficar vermelha"),
	})
}

// Invariant 3, second half: a session always lands on the same shard.
func assertOneShard(rep *report, shardOf map[string]map[string]bool,
	firstOn map[string]map[string]string, sids []string) {

	offenders := []string{}
	// A session with no event at all is a finding, and it is invisible to every reading
	// that walks what was published: they all iterate over the sessions that produced
	// something. Lose an entire session's events -- a shard nobody wrote to, a publisher
	// that never ran, a stream deleted under the run -- and the claims below hold over the
	// sessions that survived while saying nothing about the one that did not. The run
	// asked for these sids, connected them and sent to them, so silence from one of them
	// is not a session that had nothing to say.
	for _, sid := range sids {
		if len(shardOf[sid]) == 0 {
			offenders = append(offenders, fmt.Sprintf(
				"%s foi pedida e conectada nesta corrida e nao publicou evento nenhum, entao nenhuma "+
					"afirmacao sobre ordem, epoch ou continuidade a alcancou", sid))
		}
	}
	for sid, streams := range shardOf {
		if len(streams) > 1 {
			// Named with an event id on each stream, because "it appeared on two" is not
			// yet something anybody can go and look at: the ids are what a reader pulls
			// out of Redis to see the two halves of one session for themselves.
			where := make([]string, 0, len(streams))
			for _, stream := range sorted(streams) {
				where = append(where, fmt.Sprintf("%s (a partir do evento %s)", stream, firstOn[sid][stream]))
			}
			offenders = append(offenders, fmt.Sprintf("%s apareceu em %s", sid, strings.Join(where, " e ")))
		}
	}
	rep.assert(&assertion{
		invariant: "3 (uma sessao sempre no mesmo stream de eventos)",
		claim:     "cada sid pedido publicou, e publicou num shard so",
		series:    fmt.Sprintf("%d sessoes com evento publicado, de %d pedidas", len(shardOf), len(sids)),
		points:    len(sids),
		held:      len(offenders) == 0,
		detail:    strings.Join(offenders, "\n"),
	})
}

// No lost event, read as the absence of a hole in `seq` and not as "we read a lot".
func assertNoLostEvent(rep *report, published map[string][]protocol.Event, truncated map[string]bool) {
	runs, offenders, skipped := 0, []string{}, 0
	for sid, events := range published {
		byEpoch := map[uint64][]protocol.Event{}
		for _, event := range events {
			byEpoch[event.Epoch] = append(byEpoch[event.Epoch], event)
		}
		for epoch, run := range byEpoch {
			if truncated[sid] {
				skipped++
				continue
			}
			runs++
			// Distinct and sorted, because a repeated `seq` is not a missing one. Read
			// with duplicates in, 1,1,3 reports "falta seq 2 entre 1 e 1", which names a
			// hole between a number and itself: the repetition is what is wrong there,
			// and `assertSeqMonotonic` is the assertion that says so.
			distinct := map[uint64]bool{}
			for _, event := range run {
				distinct[event.Seq] = true
			}
			seqs := make([]uint64, 0, len(distinct))
			for seq := range distinct {
				seqs = append(seqs, seq)
			}
			sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
			// The first event of an epoch is seq 1, and a series that starts higher is
			// missing what came before it.
			//
			// `Session.publish` numbers from a counter that belongs to the session object,
			// and a new owner builds a new one: the contract says as much -- a higher epoch
			// means the session was re-owned, "so its numbering restarts". This bench reads
			// streams it created itself, from the first entry, so the beginning of every
			// epoch is inside what it read. Comparing only neighbours, a series of 2,3
			// reports nothing: the two are adjacent, and the event that is gone left no
			// gap between survivors, only a hole at the front.
			if len(seqs) > 0 && seqs[0] != 1 {
				offenders = append(offenders, fmt.Sprintf(
					"%s no epoch %d: a serie comeca em seq %d, e todo epoch comeca em 1, entao falta "+
						"o que veio antes; o stream dele nao foi truncado", sid, epoch, seqs[0]))
			}
			for i := 1; i < len(seqs); i++ {
				if seqs[i] != seqs[i-1]+1 {
					offenders = append(offenders, fmt.Sprintf(
						"%s no epoch %d: falta seq %d entre %d e %d, e o stream dele nao foi truncado",
						sid, epoch, seqs[i-1]+1, seqs[i-1], seqs[i]))
				}
			}
		}
	}
	why := ""
	if skipped > 0 {
		why = fmt.Sprintf("%d series ficaram de fora porque o stream delas chegou ao limite de corte "+
			"(%d entradas): um buraco sob stream truncado nao diz nada sobre quem publicou",
			skipped, redisstream.DefaultEventMaxLen)
	}
	// Joined rather than concatenated, so that a run with neither offenders nor trimming
	// has an empty detail and prints no evidence line at all. An `evidencia:` with nothing
	// under it reads as evidence that could not be shown.
	evidence := make([]string, 0, len(offenders)+1)
	evidence = append(evidence, offenders...)
	if why != "" {
		evidence = append(evidence, why)
	}
	rep.assert(&assertion{
		invariant: "3 (nenhum evento perdido entre o publicador e o cliente)",
		claim:     "sem buraco em seq dentro de cada (sid, epoch), em stream nao truncado",
		series:    fmt.Sprintf("%d series (sid, epoch) examinadas, %d de fora por truncamento", runs, skipped),
		points:    runs,
		held:      len(offenders) == 0,
		detail:    strings.Join(evidence, "\n"),
		notWhy:    ifEmpty(runs, "nenhuma serie sobrou depois de tirar os streams truncados"),
	})
}

// No duplicated side effect, read from out here, from the two places it can show.
//
// The first is a redelivery answering twice: one command id with two answers that disagree
// about what the second run produced. The recall path answers twice as well and the two
// are identical, which is the whole difference between remembering a command and doing it
// again.
//
// The second is a message asked for twice under two command ids, which is what
// `exerciseIdempotency` sets up and what a client does when it resends after a timeout.
// It is here because the first source alone does not reach the ledger: after a kill, the
// peer carries the dead owner's unanswered command out once and answers once.
//
// One claim and not two, because they are one claim: the issue asks whether a side effect
// can happen twice, and these are the two doors to it that a client can see from outside.
func assertNoDuplicateEffect(rep *report, answers map[string][]string, pairs []idempotentPair,
	expired string) {
	examined, repeated, retried, offenders := 0, 0, 0, []string{}
	for id, given := range answers {
		if len(given) == 0 {
			continue
		}
		examined++
		if len(given) < 2 {
			continue
		}
		repeated++

		// Only the answers that say the command WORKED, and this is the difference between
		// a duplicated effect and an ordinary retry.
		//
		// MEASURED in the connector: `Session.carryOut` remembers successes and nothing
		// else (internal/session/session.go, "Only a success is remembered. A failure is
		// the caller's to try again"). So a command whose first attempt answered with an
		// error and whose owner died before the XACK is redelivered, runs for real, and
		// answers differently the second time -- by design, because the first attempt left
		// no side effect to duplicate. Comparing whole reply strings calls that a
		// duplicated side effect and reports the connector's retry path as a defect.
		//
		// What cannot happen is two answers that BOTH claim success and disagree: the
		// ledger is what makes the second one a recall of the first, and two different
		// successes mean it ran twice.
		worked := []string{}
		for _, answer := range given {
			var reply protocol.Reply
			// An answer that does not parse is compared as it came: it is not a success
			// this can vouch for, and dropping it would be dropping evidence.
			if err := json.Unmarshal([]byte(answer), &reply); err != nil || reply.OK {
				worked = append(worked, answer)
			}
		}
		if len(worked) < len(given) {
			retried++
		}
		if len(worked) < 2 {
			continue
		}
		first := worked[0]
		for _, other := range worked[1:] {
			if other != first {
				offenders = append(offenders, fmt.Sprintf(
					"o comando %s foi respondido com sucesso %d vezes, com respostas diferentes, entao ele "+
						"rodou de novo em vez de ser lembrado pela idempotencia de message_id:\n  %s\n  %s",
					id, len(worked), first, other))
				break
			}
		}
	}
	widest := time.Duration(0)
	for _, pair := range pairs {
		if pair.gap > widest {
			widest = pair.gap
		}
		if pair.duplicated() {
			offenders = append(offenders, fmt.Sprintf(
				"a mensagem %s de %s foi enviada pelo comando %s e de novo pelo comando %s, %s depois de "+
					"a primeira ter sido respondida, e o resultado mudou, entao ela saiu duas vezes em vez "+
					"de ser lembrada pela chave de idempotencia msg:%s. O observavel e o carimbo de tempo "+
					"que o motor poe em cada envio que ele realmente faz:\n  %s\n  %s",
				pair.messageID, pair.sid, pair.firstID, pair.secondID, pair.gap.Round(time.Millisecond),
				pair.messageID, pair.first, pair.second))
		}
	}
	if expired != "" {
		rep.note(expired)
	}
	rep.assert(&assertion{
		invariant: "5 (comandos idempotentes por message_id: uma reentrega nao duplica efeito)",
		claim: "nenhum efeito colateral duplicado NOS LOTES LIDOS (os comandos em voo na troca de " +
			"dono e os pedidos em par): nem resposta que discorda de si mesma, nem message_id " +
			"repetido que saiu de novo. A carga continua e o lote do dono congelado ficam de fora, " +
			"ver notas",
		series: fmt.Sprintf("%d comandos com resposta, dos quais %d responderam mais de uma vez "+
			"(%d com alguma tentativa que falhou antes, que a idempotencia nao lembra por decisao); "+
			"e %d mensagens pedidas duas vezes, a maior folga entre os dois pedidos sendo %s",
			examined, repeated, retried, len(pairs), widest.Round(time.Millisecond)),
		points: repeated + len(pairs),
		held:   len(offenders) == 0,
		detail: strings.Join(offenders, "\n"),
		notWhy: ifEmpty(repeated+len(pairs), "nenhum comando desta corrida foi respondido duas vezes e "+
			"nenhuma mensagem foi pedida duas vezes, entao nao houve o que duplicar"),
	})
}

// The consumer group ends without a hole: nothing pending and nothing waiting to be read.
//
// What this speaks for, and what it does not.
//
// It is the sixth claim the issue asks for, and it is about command delivery: every
// command this bench put on a session's stream was read, carried out and acknowledged, so
// the group has nothing pending and no lag. A hole here is a command the fleet took and
// never retired, which after an ownership change is a command nobody will run.
//
// It is NOT invariant 4. That one is about an inbound WhatsApp message being acknowledged
// to WhatsApp only after its event is published, and nothing in this run touches it: the
// workload is outbound commands, no inbound message arrives, and Redis is never cut
// mid-publish. A build that acknowledged inbound messages before publishing would pass
// this check untouched. Saying so here rather than leaving the label to imply otherwise is
// the difference between a bench that measures six things and one that claims seven.
func assertConsumerGroups(ctx context.Context, cl *client, rep *report, sids []string,
	stillWorking string) error {
	checked, offenders := 0, []string{}
	streams := append([]string{cl.keys.Control()}, nil...)
	for _, sid := range sids {
		streams = append(streams, cl.keys.Commands(sid))
	}
	for _, stream := range streams {
		groups, err := cl.rdb.XInfoGroups(ctx, stream).Result()
		found, seen, fatal := inspectGroups(stream, groups, err)
		if fatal != nil {
			return fatal
		}
		offenders = append(offenders, found...)
		checked += seen
	}
	rep.assert(consumerGroupVerdict(checked, len(streams), offenders, stillWorking))
	return nil
}

// consumerGroupVerdict is the judgement, apart from the reading that feeds it.
//
// Apart because the reading needs a server and the judgement needs a table: kept together,
// the only instrument that could disprove this is a four-minute run against two real
// servers, and the case that matters most -- a deadline that passed with work still in
// flight -- is exactly the one a healthy bench will not produce on demand.
//
// A drain deadline that passed is NOT a hole. What is left pending after a wait that ended
// on the clock is the work of a busy fleet (a large load, a slow server, a full machine),
// and reading it as lost delivery turns "not enough time" into a defect of the connector,
// which is the confusion the four exit codes exist to avoid. So this gives a verdict only
// when the wait ended on the fact it was waiting for; without that, whatever is pending
// comes out as NAO MEDIDO with the reason written down.
// groupOffender says whether one consumer group still holds work, and names it when it
// does.
//
// Both numbers, and not either one: `Pending` counts entries delivered to a consumer that
// never acknowledged them, and `Lag` counts entries the group has not been handed at all.
// A group can have one without the other -- a redelivered command sitting unacknowledged
// with nothing new behind it is pending 1, lag 0, which is what a run measured after an
// owner was killed -- so a check that wanted both to be non-zero would report an empty
// stream as fine and a stuck consumer as fine too.
func groupOffender(stream string, group redis.XInfoGroup) string {
	if group.Pending == 0 && group.Lag == 0 {
		return ""
	}
	return fmt.Sprintf("%s, grupo %s: %d pendentes e lag %d depois dos acks",
		stream, group.Name, group.Pending, group.Lag)
}

// inspectGroups judges one stream's consumer groups, apart from the call that read them.
//
// A stream of this run that is gone is a finding, not a skip. Every stream handed here was
// written to earlier in the run -- the control stream and one command stream per session,
// each carrying the sends this bench put in flight -- so "it is not there" means it was
// trimmed away, deleted, or never created, and none of those is "nothing to check here".
// Skipped silently, the claim came back AFIRMADO over the groups it did read while saying
// nothing about the ones it did not. A stream that exists with no group at all is the same
// finding wearing another shape: nobody consumed what this run sent through it.
//
// Anything else -- a connection that dropped, a timeout, an ACL -- is a reading that
// failed, and it stops the run instead of being spent as a pass.
func inspectGroups(stream string, groups []redis.XInfoGroup, err error) (
	offenders []string, checked int, fatal error,
) {
	if err != nil {
		if errors.Is(err, redis.Nil) || strings.Contains(err.Error(), "no such key") {
			return []string{fmt.Sprintf("%s: o stream nao existe mais, e esta corrida escreveu nele",
				stream)}, 0, nil
		}
		return nil, 0, fmt.Errorf("%w: read the consumer groups of %s: %w", errSetup, stream, err)
	}
	if len(groups) == 0 {
		return []string{fmt.Sprintf("%s: o stream existe e nao tem grupo consumidor nenhum, entao "+
			"ninguem leu o que esta corrida mandou por ele", stream)}, 0, nil
	}
	for _, group := range groups {
		if offender := groupOffender(stream, group); offender != "" {
			offenders = append(offenders, offender)
		}
	}
	return offenders, len(groups), nil
}

func consumerGroupVerdict(checked, streams int, offenders []string, stillWorking string) *assertion {
	// A deadline that passed explains entries still pending. It does not explain a stream
	// that is gone or a stream nobody consumed: those are not work in progress, and a busy
	// session exhausting the drain deadline while another session's stream was deleted
	// would otherwise file the deletion as "not measured" and exit 2.
	structural := []string{}
	for _, offender := range offenders {
		if strings.Contains(offender, "nao existe mais") || strings.Contains(offender, "grupo consumidor nenhum") {
			structural = append(structural, offender)
		}
	}
	undecided := ""
	if len(offenders) > 0 && len(structural) == 0 && stillWorking != "" {
		undecided = stillWorking
	}
	// The findings count as points, and not only the groups that answered.
	//
	// A run where every expected stream is gone produces offenders and no groups at all,
	// and a series of zero prints as NAO MEDIDO -- so the worst case this claim can find
	// would come out as "not measured" and exit 2, while the report above it listed every
	// missing stream. Something was examined whenever there is something to report.
	points := checked
	if len(offenders) > points {
		points = len(offenders)
	}
	return &assertion{
		invariant: "entrega de comando pelo transporte (NAO e a invariante 4: o ack de mensagem que " +
			"CHEGA, depois da publicacao, nao e exercitado por esta carga e fica sem medida)",
		claim:  "grupo consumidor sem buraco: nada pendente e lag zero ao fim",
		series: fmt.Sprintf("%d grupos consumidores sobre %d streams de comando", checked, streams),
		points: points,
		held:   len(offenders) == 0,
		detail: strings.Join(offenders, "\n"),
		notWhy: undecided,
	}
}

func sorted(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for key := range set {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// ifEmpty turns a series of zero into the reason it is empty, which is what keeps a claim
// with nothing behind it from printing as one that held.
func ifEmpty(points int, why string) string {
	if points == 0 {
		return why
	}
	return ""
}

// markUnmeasured turns claims into unmeasured ones when the reading they ran over could
// not be trusted, and does nothing when `why` is empty.
//
// It leaves a claim that already has a reason alone: the first reason is the specific one
// -- "no (sid, epoch) had more than one event" says more than "the stream was still
// growing" -- and the second would overwrite it with the general case.
func markUnmeasured(claims []*assertion, why string) {
	if why == "" {
		return
	}
	for _, claim := range claims {
		// A violation already observed stays a violation. More events cannot undo a seq
		// that regressed, a session that landed on two shards, or two instances that held
		// one lease: the tail that had not arrived could only have added to the evidence.
		// Overwriting it would turn a defect (exit 1) into an incomplete run (exit 2),
		// which is the one direction this must never move.
		if claim.state() == "QUEBRADO" {
			continue
		}
		if claim.notWhy == "" {
			claim.notWhy = why
		}
	}
}
