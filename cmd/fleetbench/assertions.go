package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/transport/redisstream"
)

// The six claims the issue asks of the third measurement, each with the series it looked
// at and a verdict, or with the reason it was not measured.
//
// Never both and never neither: an item with no series is not an assertion that passed,
// it is an assertion that never ran, and printing it as a pass is how a bench ends up
// saying more than it measured.
func assertInvariants(ctx context.Context, cl *client, rep *report, plan benchPlan,
	inFlight, sids []string, pairs []idempotentPair) error {

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

	assertOneOwner(rep, published, inOrder)
	assertEpochRises(rep, published)
	assertSeqMonotonic(rep, published)
	assertOneShard(rep, shardOf, firstOn, sids)
	assertNoLostEvent(rep, published, truncated)
	if err := assertNoDuplicateEffect(ctx, cl, rep, inFlight, pairs); err != nil {
		return err
	}
	return assertConsumerGroups(ctx, cl, rep, sids)
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
func assertOneOwner(rep *report, published map[string][]protocol.Event,
	inOrder map[string]map[string][]protocol.Event) {

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
					"%s no epoch %d foi publicado por %s ao mesmo tempo", sid, epoch, strings.Join(sorted(holders), " e ")))
			}
		}
	}

	fenced := 0
	for sid, streams := range inOrder {
		for stream, events := range streams {
			var highest uint64
			var highestBy, highestID string
			for _, event := range events {
				if event.Epoch < highest {
					offenders = append(offenders, fmt.Sprintf(
						"%s: a instancia %s publicou o evento %s sob o epoch %d em %s DEPOIS de %s ja ter "+
							"publicado o evento %s sob o epoch %d, entao ela seguiu publicando essa sessao "+
							"depois de perder a lease",
						sid, event.Inst, event.ID, event.Epoch, stream, highestBy, highestID, highest))
					continue
				}
				if event.Epoch > highest {
					highest, highestBy, highestID = event.Epoch, event.Inst, event.ID
				}
			}
			fenced += len(events)
		}
	}

	rep.assert(&assertion{
		invariant: "1 (uma instancia dona da sessao por vez, arbitrada pela lease; perder a lease cerca a sessao na hora)",
		claim: "nunca duas instancias com a mesma lease: um epoch, um publicador, e nenhum evento de " +
			"epoch velho depois de um novo no mesmo stream",
		series: fmt.Sprintf("%d pares (sid, epoch) sobre %d sessoes, e %d eventos lidos na ordem do stream deles",
			pairs, len(published), fenced),
		points: pairs,
		held:   len(offenders) == 0,
		detail: strings.Join(offenders, "\n"),
	})
}

// Invariant 2: the epoch rises on every ownership change.
//
// Read as: walking one session's events in stream order, the moment the publisher changes
// the epoch has to be higher than every epoch that session has carried. A client drops
// state from a stale epoch, so an epoch that repeats across a change is a client accepting
// the old owner's state as current.
func assertEpochRises(rep *report, published map[string][]protocol.Event) {
	changes, offenders := 0, []string{}
	for sid, events := range published {
		var lastInst string
		var highest uint64
		for _, event := range events {
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
	rep.assert(&assertion{
		invariant: "2 (todo evento carrega o epoch do dono, e ele sobe em toda troca de posse)",
		claim:     "epoch estritamente crescente a cada troca de dono",
		series:    fmt.Sprintf("%d trocas de dono observadas sobre %d sessoes", changes, len(published)),
		points:    changes,
		held:      len(offenders) == 0,
		detail:    strings.Join(offenders, "\n"),
		notWhy: ifEmpty(changes, "nenhuma troca de dono apareceu nos streams, entao nao havia o que "+
			"afirmar: uma asserção de epoch sem troca nao tem como ficar vermelha"),
	})
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
			for i := 1; i < len(run); i++ {
				if run[i].Seq <= run[i-1].Seq {
					offenders = append(offenders, fmt.Sprintf(
						"%s no epoch %d: seq %d (evento %s) veio depois de seq %d (evento %s)",
						sid, k.epoch, run[i].Seq, run[i].ID, run[i-1].Seq, run[i-1].ID))
				}
			}
		}
	}
	rep.assert(&assertion{
		invariant: "3 (seq monotonico por (sid, epoch))",
		claim:     "seq estritamente crescente dentro de cada (sid, epoch)",
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
		claim:     "cada sid publicado num shard so",
		series:    fmt.Sprintf("%d sessoes com evento publicado, de %d pedidas", len(shardOf), len(sids)),
		points:    len(shardOf),
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
			seqs := make([]uint64, 0, len(run))
			for _, event := range run {
				seqs = append(seqs, event.Seq)
			}
			sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
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
func assertNoDuplicateEffect(ctx context.Context, cl *client, rep *report, inFlight []string,
	pairs []idempotentPair) error {
	examined, repeated, offenders := 0, 0, []string{}
	for _, id := range inFlight {
		answers, err := cl.repliesTo(ctx, id)
		if err != nil {
			return fmt.Errorf("%w: %w", errSetup, err)
		}
		if len(answers) == 0 {
			continue
		}
		examined++
		if len(answers) < 2 {
			continue
		}
		repeated++
		first := answers[0]
		for _, other := range answers[1:] {
			if other != first {
				offenders = append(offenders, fmt.Sprintf(
					"o comando %s foi respondido %d vezes com respostas diferentes, entao ele rodou de novo "+
						"em vez de ser lembrado pela idempotencia de message_id:\n  %s\n  %s",
					id, len(answers), first, other))
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
	rep.assert(&assertion{
		invariant: "5 (comandos idempotentes por message_id: uma reentrega nao duplica efeito)",
		claim:     "nenhum efeito colateral duplicado: nem resposta que discorda de si mesma, nem message_id repetido que saiu de novo",
		series: fmt.Sprintf("%d comandos com resposta, dos quais %d responderam mais de uma vez; "+
			"e %d mensagens pedidas duas vezes, a maior folga entre os dois pedidos sendo %s",
			examined, repeated, len(pairs), widest.Round(time.Millisecond)),
		points: repeated + len(pairs),
		held:   len(offenders) == 0,
		detail: strings.Join(offenders, "\n"),
		notWhy: ifEmpty(repeated+len(pairs), "nenhum comando desta corrida foi respondido duas vezes e "+
			"nenhuma mensagem foi pedida duas vezes, entao nao houve o que duplicar"),
	})
	return nil
}

// The consumer group ends without a hole: nothing pending and nothing waiting to be read.
func assertConsumerGroups(ctx context.Context, cl *client, rep *report, sids []string) error {
	checked, offenders := 0, []string{}
	streams := append([]string{cl.keys.Control()}, nil...)
	for _, sid := range sids {
		streams = append(streams, cl.keys.Commands(sid))
	}
	for _, stream := range streams {
		groups, err := cl.rdb.XInfoGroups(ctx, stream).Result()
		if err != nil {
			// A stream with no group is a stream nothing consumed, which is not a hole.
			continue
		}
		for _, group := range groups {
			checked++
			if group.Pending != 0 || group.Lag != 0 {
				offenders = append(offenders, fmt.Sprintf(
					"%s, grupo %s: %d pendentes e lag %d depois dos acks", stream, group.Name, group.Pending, group.Lag))
			}
		}
	}
	rep.assert(&assertion{
		invariant: "4 (o ack so acontece depois da publicacao: perder o Redis custa reentrega, nunca mensagem)",
		claim:     "grupo consumidor sem buraco: nada pendente e lag zero ao fim",
		series:    fmt.Sprintf("%d grupos consumidores sobre %d streams de comando", checked, len(streams)),
		points:    checked,
		held:      len(offenders) == 0,
		detail:    strings.Join(offenders, "\n"),
	})
	return nil
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
