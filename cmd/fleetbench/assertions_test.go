package main

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// The assertions are predicates, and a predicate is cheapest to disprove by hand.
//
// The mutation battery in the issue is what proves they fire against real processes, and
// it costs four minutes a run on two servers. That is the wrong instrument for "does this
// still notice a repeated seq" on every change, and it is also the wrong instrument for
// the cases a fleet will not produce on demand: a stream that was trimmed, a session
// published by two instances under one epoch. Both are built here in three lines.
//
// Written against the state each assertion prints rather than against its internals, so a
// rewrite of how a verdict is reached does not rewrite these.

func event(sid, inst string, epoch, seq uint64) protocol.Event {
	return protocol.Event{
		V: protocol.Version, ID: sid + "-" + inst + "-" + itoa(epoch) + "-" + itoa(seq),
		Type: protocol.EventSessionState, SID: sid, Epoch: epoch, Seq: seq,
		TS: time.Now().UnixMilli(), Inst: inst, Payload: json.RawMessage(`{}`),
	}
}

// moment is a fixed instant plus an offset, so a table can say "these two readings are
// five seconds apart" without depending on when the test runs.
func moment(after time.Duration) time.Time {
	return time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC).Add(after)
}

func itoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	var out []byte
	for n > 0 {
		out = append([]byte{byte('0' + n%10)}, out...)
		n /= 10
	}
	return string(out)
}

// eventAs is an event with an id of its own, which is how a table says "two different
// events at the same seq" without depending on which field the assertion reads.
func eventAs(id, sid, inst string, epoch, seq uint64) protocol.Event {
	made := event(sid, inst, epoch, seq)
	made.ID = id
	return made
}

// only returns the single assertion a report holds, failing when it holds anything else:
// a test that read the wrong one would be checking a verdict it did not ask for.
func only(t *testing.T, rep *report) *assertion {
	t.Helper()
	if len(rep.assertions) != 1 {
		t.Fatalf("o relatorio ficou com %d assercoes, e este teste pediu uma", len(rep.assertions))
	}
	return rep.assertions[0]
}

func TestOneOwnerNoticesTwoInstancesUnderOneEpoch(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		events  []protocol.Event
		samples []fleetSample
		state   string
		says    string
	}{
		"um publicador por epoch": {
			events: []protocol.Event{event("s1", "a", 1, 1), event("s1", "a", 1, 2)},
			state:  "AFIRMADO",
		},
		"dois publicadores sob o mesmo epoch": {
			events: []protocol.Event{event("s1", "a", 1, 1), event("s1", "b", 1, 2)},
			state:  "QUEBRADO",
			says:   "dois donos da mesma lease",
		},
		"UM epoch velho depois de um novo e a escrita que estava em voo": {
			// What the contract names and covers: a client keeps the highest epoch it has
			// seen and drops everything below it, "which is what stops a late event from a
			// previous owner overwriting the state of the instance running the account now
			// -- a paused process, a socket that outlived its lease, a delivery that sat in
			// a queue". `Session.stillOwned` is the other side of the same trade: the check
			// happens after the publish too, because a lease can run out mid-write.
			// MEASURED: a clean tree produces exactly one of these per (sid, instance).
			events: []protocol.Event{event("s1", "b", 2, 1), event("s1", "a", 1, 3)},
			state:  "AFIRMADO",
		},
		"o MESMO evento tardio entregue duas vezes continua sendo um": {
			// At-least-once is the transport's guarantee: a retried XADD whose first answer
			// was lost puts the same entry on the stream again. Counted as two late
			// publications, the contract's own delivery would read as the fence failing.
			events: []protocol.Event{
				event("s1", "b", 2, 1), event("s1", "a", 1, 3), event("s1", "a", 1, 3),
			},
			state: "AFIRMADO",
		},
		"o SEGUNDO epoch velho da mesma instancia e a cerca nao tendo agido": {
			// The fence half, and it is invisible to the first reading: every (sid, epoch)
			// here still has exactly one publisher. On the first late event the connector
			// tears the session down, so a second one from that instance means it did not.
			// MEASURED: the mutant that removes the fence from `session.publish` produces
			// 246 of these in one run.
			events: []protocol.Event{
				event("s1", "b", 2, 1), event("s1", "a", 1, 3), event("s1", "a", 1, 4),
			},
			state: "QUEBRADO",
			says:  "a cerca nao tendo agido",
		},
		"a frota somada roda mais sessoes do que existem sids, e isso se sustenta": {
			// The third half, and it is invisible to both of the others: every
			// acquisition bumps the epoch, so two holders never share one and neither
			// publishes out of order. Only the arithmetic sees it.
			events: []protocol.Event{event("s1", "a", 1, 1), event("s1", "b", 2, 1)},
			samples: []fleetSample{
				{at: moment(0), phase: "troca de dono", byInst: map[string]int{"a": 1, "b": 1}, total: 2},
				{at: moment(5 * time.Second), phase: "troca de dono", byInst: map[string]int{"a": 1, "b": 1}, total: 2},
			},
			state: "QUEBRADO",
			says:  "mais de uma instancia ao mesmo tempo",
		},
		"duas sobras isoladas, com leitura sa no meio, sao duas trocas": {
			// Each spike is one ownership change paying its own tick of gauge lag. Read
			// as a single overshoot that lasted the whole interval, ordinary fleet
			// movement becomes a broken invariant.
			events: []protocol.Event{event("s1", "a", 1, 1), event("s1", "b", 2, 1)},
			samples: []fleetSample{
				{at: moment(0), phase: "troca", byInst: map[string]int{"a": 1, "b": 1}, total: 2},
				{at: moment(30 * time.Second), phase: "troca", byInst: map[string]int{"a": 1}, total: 1},
				{at: moment(60 * time.Second), phase: "troca", byInst: map[string]int{"a": 1, "b": 1}, total: 2},
			},
			state: "AFIRMADO",
		},
		"a mesma sobra dentro de um tick nao e posse dupla": {
			// The gauge is written once per heartbeat, so while ownership moves one
			// instance can still be counting a session the next one already counts. Real
			// in the numbers, false about the fleet, and gone by the next tick.
			events: []protocol.Event{event("s1", "a", 1, 1), event("s1", "b", 2, 1)},
			samples: []fleetSample{
				{at: moment(0), phase: "troca de dono", byInst: map[string]int{"a": 1, "b": 1}, total: 2},
				{at: moment(300 * time.Millisecond), phase: "troca de dono", byInst: map[string]int{"a": 1, "b": 1}, total: 2},
			},
			state: "AFIRMADO",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rep := &report{}
			published := map[string][]protocol.Event{"s1": tc.events}
			inOrder := map[string]map[string][]protocol.Event{"s1": {"wa:events:0": tc.events}}
			assertOneOwner(rep, published, inOrder, &census{samples: tc.samples}, 1)

			got := only(t, rep)
			if got.state() != tc.state {
				t.Fatalf("estado %q, queria %q (evidencia: %s)", got.state(), tc.state, got.detail)
			}
			if tc.says != "" && !strings.Contains(got.detail, tc.says) {
				t.Errorf("a evidencia nao diz %q:\n%s", tc.says, got.detail)
			}
		})
	}
}

func TestEpochRisesOnlySpeaksWhenOwnershipMoved(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		events []protocol.Event
		state  string
	}{
		"sem troca de dono": {
			events: []protocol.Event{event("s1", "a", 1, 1), event("s1", "a", 1, 2)},
			state:  "NAO MEDIDO",
		},
		"troca com epoch que sobe": {
			events: []protocol.Event{event("s1", "a", 1, 1), event("s1", "b", 2, 1)},
			state:  "AFIRMADO",
		},
		"troca com epoch congelado": {
			events: []protocol.Event{event("s1", "a", 1, 1), event("s1", "b", 1, 1)},
			state:  "QUEBRADO",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rep := &report{}
			assertEpochRises(rep, map[string][]protocol.Event{"s1": tc.events})
			if got := only(t, rep); got.state() != tc.state {
				t.Fatalf("estado %q, queria %q (serie: %s, evidencia: %s)",
					got.state(), tc.state, got.series, got.detail)
			}
		})
	}
}

func TestSeqMonotonicNeedsMoreThanOnePoint(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		events []protocol.Event
		state  string
	}{
		"um ponto por (sid, epoch)": {
			events: []protocol.Event{event("s1", "a", 1, 1)},
			state:  "NAO MEDIDO",
		},
		"seq subindo": {
			events: []protocol.Event{event("s1", "a", 1, 1), event("s1", "a", 1, 2)},
			state:  "AFIRMADO",
		},
		"o mesmo evento entregue duas vezes": {
			// `contract/PROTOCOL.md`: delivery is at-least-once, and `seq` is what lets the
			// consumer drop the copy. Reporting it would report the contract's own
			// guarantee as a defect. `event` builds its id out of (sid, inst, epoch, seq),
			// so these two are the same event.
			events: []protocol.Event{event("s1", "a", 1, 1), event("s1", "a", 1, 1)},
			state:  "AFIRMADO",
		},
		"dois eventos diferentes com o mesmo seq": {
			events: []protocol.Event{event("s1", "a", 1, 1), eventAs("outro", "s1", "a", 1, 1)},
			state:  "QUEBRADO",
		},
		"seq reiniciando": {
			events: []protocol.Event{event("s1", "a", 1, 2), event("s1", "a", 1, 1)},
			state:  "QUEBRADO",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rep := &report{}
			assertSeqMonotonic(rep, map[string][]protocol.Event{"s1": tc.events})
			if got := only(t, rep); got.state() != tc.state {
				t.Fatalf("estado %q, queria %q (serie: %s, evidencia: %s)",
					got.state(), tc.state, got.series, got.detail)
			}
		})
	}
}

func TestOneShardNamesAnEventOnEachStream(t *testing.T) {
	t.Parallel()

	rep := &report{}
	assertOneShard(rep,
		map[string]map[string]bool{"s1": {"wa:events:0": true, "wa:events:2": true}},
		map[string]map[string]string{"s1": {"wa:events:0": "ev-0", "wa:events:2": "ev-2"}},
		[]string{"s1"})

	got := only(t, rep)
	if got.state() != "QUEBRADO" {
		t.Fatalf("uma sessao em dois streams saiu %q", got.state())
	}
	// The ids are what a reader pulls out of Redis to see it: naming the streams alone
	// leaves them with two haystacks.
	for _, want := range []string{"wa:events:0", "wa:events:2", "ev-0", "ev-2"} {
		if !strings.Contains(got.detail, want) {
			t.Errorf("a evidencia nao traz %q:\n%s", want, got.detail)
		}
	}
}

func TestNoLostEventTellsAHoleFromATruncation(t *testing.T) {
	t.Parallel()

	gapped := map[string][]protocol.Event{"s1": {event("s1", "a", 1, 1), event("s1", "a", 1, 3)}}

	t.Run("buraco em stream inteiro e buraco", func(t *testing.T) {
		t.Parallel()
		rep := &report{}
		assertNoLostEvent(rep, gapped, map[string]bool{})
		got := only(t, rep)
		if got.state() != "QUEBRADO" {
			t.Fatalf("um seq faltando saiu %q", got.state())
		}
		if !strings.Contains(got.detail, "falta seq 2") {
			t.Errorf("a evidencia nao diz qual seq faltou:\n%s", got.detail)
		}
	})

	t.Run("seq repetido nao e buraco", func(t *testing.T) {
		t.Parallel()
		// 1, 1, 2 covers every number from 1 to 2. What is wrong is the repetition, and
		// that belongs to the order assertion: read with duplicates in, this said "falta
		// seq 2 entre 1 e 1", which names a hole between a number and itself.
		rep := &report{}
		assertNoLostEvent(rep, map[string][]protocol.Event{
			"s1": {event("s1", "a", 1, 1), event("s1", "a", 1, 1), event("s1", "a", 1, 2)},
		}, map[string]bool{})
		got := only(t, rep)
		if got.state() == "QUEBRADO" {
			t.Fatalf("um seq repetido foi reportado como evento perdido:\n%s", got.detail)
		}
	})

	t.Run("buraco continua visivel com repetido em volta", func(t *testing.T) {
		t.Parallel()
		rep := &report{}
		assertNoLostEvent(rep, map[string][]protocol.Event{
			"s1": {event("s1", "a", 1, 1), event("s1", "a", 1, 1), event("s1", "a", 1, 3)},
		}, map[string]bool{})
		got := only(t, rep)
		if got.state() != "QUEBRADO" {
			t.Fatalf("um seq faltando entre repetidos saiu %q", got.state())
		}
		if !strings.Contains(got.detail, "falta seq 2 entre 1 e 3") {
			t.Errorf("a evidencia nao nomeia o buraco de verdade:\n%s", got.detail)
		}
	})

	t.Run("serie que comeca depois do 1 perdeu o comeco", func(t *testing.T) {
		t.Parallel()
		// Adjacent to each other, so a check that only compares neighbours sees nothing:
		// what is gone left no gap between survivors, only a hole at the front. Every epoch
		// starts at seq 1, so a series starting at 2 is missing the event before it.
		rep := &report{}
		assertNoLostEvent(rep, map[string][]protocol.Event{
			"s1": {event("s1", "a", 1, 2), event("s1", "a", 1, 3)},
		}, map[string]bool{})
		got := only(t, rep)
		if got.state() != "QUEBRADO" {
			t.Fatalf("uma serie comecando em 2 saiu %q", got.state())
		}
		if !strings.Contains(got.detail, "comeca em seq 2") {
			t.Errorf("a evidencia nao diz onde a serie comecou:\n%s", got.detail)
		}
	})

	t.Run("o mesmo comeco faltando sob stream truncado nao e perda", func(t *testing.T) {
		t.Parallel()
		rep := &report{}
		assertNoLostEvent(rep, map[string][]protocol.Event{
			"s1": {event("s1", "a", 1, 2), event("s1", "a", 1, 3)},
		}, map[string]bool{"s1": true})
		got := only(t, rep)
		if got.state() == "QUEBRADO" {
			t.Fatalf("um comeco cortado pelo limite do stream foi reportado como perda:\n%s", got.detail)
		}
	})

	t.Run("o mesmo buraco sob stream truncado nao e perda", func(t *testing.T) {
		t.Parallel()
		rep := &report{}
		assertNoLostEvent(rep, gapped, map[string]bool{"s1": true})
		got := only(t, rep)
		if got.state() == "QUEBRADO" {
			t.Fatalf("um buraco sob stream truncado foi reportado como evento perdido:\n%s", got.detail)
		}
		if !strings.Contains(got.notWhy, "truncad") {
			t.Errorf("a razao de nao ter medido nao fala em truncamento: %q", got.notWhy)
		}
	})
}

func TestNoDuplicateEffectReadsTheRepeatedMessageID(t *testing.T) {
	t.Parallel()

	same := json.RawMessage(`{"message_id":"m1","timestamp":1000}`)
	other := json.RawMessage(`{"message_id":"m1","timestamp":1050}`)

	tests := map[string]struct {
		pairs []idempotentPair
		state string
	}{
		"a segunda foi lembrada": {
			pairs: []idempotentPair{{
				messageID: "m1", sid: "s1", firstID: "c-a", secondID: "c-b",
				first: same, second: same, gap: 50 * time.Millisecond,
			}},
			state: "AFIRMADO",
		},
		"a segunda saiu de novo": {
			pairs: []idempotentPair{{
				messageID: "m1", sid: "s1", firstID: "c-a", secondID: "c-b",
				first: same, second: other, gap: 50 * time.Millisecond,
			}},
			state: "QUEBRADO",
		},
		"nada foi pedido duas vezes": {
			pairs: nil,
			state: "NAO MEDIDO",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rep := &report{}
			// No answers from the handover: this test is about the second door, and
			// giving it the first as well would let a pass come from either.
			assertNoDuplicateEffect(rep, nil, tc.pairs, "")
			got := only(t, rep)
			if got.state() != tc.state {
				t.Fatalf("estado %q, queria %q (serie: %s, evidencia: %s)",
					got.state(), tc.state, got.series, got.detail)
			}
			if tc.state == "QUEBRADO" && !strings.Contains(got.detail, "msg:m1") {
				t.Errorf("a evidencia nao nomeia a chave de idempotencia:\n%s", got.detail)
			}
		})
	}
}

// A report that holds a broken assertion exits 1, one with a measurement outside its
// range exits 3, and they are different numbers on purpose: a script reads the code.
func TestTheThreeOutcomesAreDistinct(t *testing.T) {
	t.Parallel()

	broken := &report{}
	broken.assert(&assertion{claim: "c", points: 1, held: false})
	broken.measure("f", "m", 1, "u")
	broken.measurements[0].outside = true
	if got := broken.outcome(); got != outcomeInvariant {
		t.Errorf("uma invariante quebrada saiu %d, e ela vem antes de um numero fora da faixa", got)
	}

	outside := &report{}
	outside.assert(&assertion{claim: "c", points: 1, held: true})
	outside.measure("f", "m", 1, "u")
	outside.measurements[0].outside = true
	if got := outside.outcome(); got != outcomeOutside {
		t.Errorf("um numero fora da faixa saiu %d, queria %d", got, outcomeOutside)
	}

	green := &report{}
	green.assert(&assertion{claim: "c", points: 1, held: true})
	if got := green.outcome(); got != outcomeGreen {
		t.Errorf("tudo afirmado saiu %d, queria 0", got)
	}

	seen := map[outcome]string{}
	for _, o := range []outcome{outcomeGreen, outcomeInvariant, outcomeSetup, outcomeOutside} {
		if before, clash := seen[o]; clash {
			t.Errorf("%q e %q saem com o mesmo codigo %d", before, o.label(), int(o))
		}
		seen[o] = o.label()
	}
}

// The counters a run reports are part of what it claims, and a wrong one is not cosmetic:
// "0 comandos responderam mais de uma vez" is what says the idempotency series had nothing
// in it, and a bench that inflates it claims to have measured something it did not.
func TestTheIdempotencySeriesCountsWhatItSays(t *testing.T) {
	t.Parallel()

	ok := func(body string) string { return `{"v":1,"id":"c1","ok":true,"result":{"b":"` + body + `"}}` }
	failed := `{"v":1,"id":"c1","ok":false,"error":{"code":"internal","message":"nao deu"}}`

	tests := map[string]struct {
		answers  map[string][]string
		pairs    []idempotentPair
		state    string
		contains []string
	}{
		"um comando que respondeu uma vez nao e um comando repetido": {
			answers:  map[string][]string{"c1": {ok("a")}},
			state:    "NAO MEDIDO",
			contains: []string{"1 comandos com resposta, dos quais 0 responderam mais de uma vez"},
		},
		"um comando sem resposta nenhuma nao conta como comando com resposta": {
			answers:  map[string][]string{"c1": {}},
			state:    "NAO MEDIDO",
			contains: []string{"0 comandos com resposta"},
		},
		"duas respostas de sucesso iguais sao a idempotencia funcionando": {
			answers:  map[string][]string{"c1": {ok("a"), ok("a")}},
			state:    "AFIRMADO",
			contains: []string{"1 comandos com resposta, dos quais 1 responderam mais de uma vez"},
		},
		"duas respostas de sucesso diferentes sao efeito duplicado": {
			answers:  map[string][]string{"c1": {ok("a"), ok("b")}},
			state:    "QUEBRADO",
			contains: []string{"foi respondido com sucesso 2 vezes"},
		},
		"uma falha e depois um sucesso diferente e reentrega, nao duplicacao": {
			// MEASURED in the connector: the ledger remembers successes and nothing else
			// (internal/session/session.go, "Only a success is remembered"). So the second
			// attempt runs for real and answers differently, by design: the first left no
			// side effect to duplicate.
			answers:  map[string][]string{"c1": {failed, ok("a")}},
			state:    "AFIRMADO",
			contains: []string{"1 com alguma tentativa que falhou antes"},
		},
		"a maior folga e a maior, e nao a primeira": {
			answers: map[string][]string{},
			pairs: []idempotentPair{
				{messageID: "m1", sid: "s1", firstID: "c-a", secondID: "c-b",
					first: json.RawMessage(`{"message_id":"m1"}`), second: json.RawMessage(`{"message_id":"m1"}`),
					gap: 200 * time.Millisecond},
				{messageID: "m2", sid: "s1", firstID: "c-c", secondID: "c-d",
					first: json.RawMessage(`{"message_id":"m2"}`), second: json.RawMessage(`{"message_id":"m2"}`),
					gap: 50 * time.Millisecond},
			},
			state:    "AFIRMADO",
			contains: []string{"a maior folga entre os dois pedidos sendo 200ms"},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rep := &report{}
			assertNoDuplicateEffect(rep, tc.answers, tc.pairs, "")
			got := only(t, rep)
			if got.state() != tc.state {
				t.Fatalf("estado %q, queria %q (serie: %s, evidencia: %s)",
					got.state(), tc.state, got.series, got.detail)
			}
			for _, want := range tc.contains {
				if !strings.Contains(got.series+"\n"+got.detail, want) {
					t.Errorf("nao diz %q:\n  serie: %s\n  evidencia: %s", want, got.series, got.detail)
				}
			}
		})
	}
}

// A deadline that passed and a hole in the delivery look the same from the consumer
// groups, and only one of them is a defect of the connector.
func TestPendingAfterADeadlineIsNotAHole(t *testing.T) {
	t.Parallel()

	busy := "o grupo consumidor nao drenou em 90 s depois da troca de dono"
	tests := map[string]struct {
		offenders    []string
		stillWorking string
		state        string
	}{
		"nada pendente, e a espera terminou pelo dreno": {nil, "", "AFIRMADO"},
		"nada pendente, e a espera terminou pelo prazo": {nil, busy, "AFIRMADO"},
		"pendente depois de um dreno que completou":     {[]string{"cmd:s1, grupo connector: 1 pendentes"}, "", "QUEBRADO"},
		"pendente depois de uma espera que estourou":    {[]string{"cmd:s1, grupo connector: 1 pendentes"}, busy, "NAO MEDIDO"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got := consumerGroupVerdict(5, 5, tc.offenders, tc.stillWorking)
			if got.state() != tc.state {
				t.Fatalf("estado %q, queria %q (razao de nao medir: %q)", got.state(), tc.state, got.notWhy)
			}
			// The reason given is the reason printed, and not a rephrasing of it: the
			// caller is the one that knows which deadline passed and how long it was.
			if tc.state == "NAO MEDIDO" && got.notWhy != tc.stillWorking {
				t.Errorf("a razao de nao ter medido nao e a que a fase deu:\n  saiu:  %q\n  queria: %q",
					got.notWhy, tc.stillWorking)
			}
			if tc.state == "QUEBRADO" && got.notWhy != "" {
				t.Errorf("um veredito de quebrado nao pode vir com razao de nao medir: %q", got.notWhy)
			}
		})
	}
}

// A run where some streams were trimmed and others were not still has to say so: the note
// is what keeps "no hole found" from being read as "every series was examined".
func TestTruncationIsNamedEvenWhenOtherSeriesWereExamined(t *testing.T) {
	t.Parallel()

	rep := &report{}
	assertNoLostEvent(rep, map[string][]protocol.Event{
		"s1": {event("s1", "a", 1, 1), event("s1", "a", 1, 2)},
		"s2": {event("s2", "a", 1, 1), event("s2", "a", 1, 3)},
	}, map[string]bool{"s2": true})
	got := only(t, rep)
	if got.state() != "AFIRMADO" {
		t.Fatalf("a serie intacta nao rendeu veredito: %q (%s)", got.state(), got.detail)
	}
	if !strings.Contains(got.detail, "ficaram de fora porque o stream delas chegou ao limite de corte") {
		t.Errorf("uma serie ficou de fora por truncamento e a corrida nao diz:\n%s", got.detail)
	}
	if !strings.Contains(got.series, "1 de fora por truncamento") {
		t.Errorf("a serie nao conta quantas ficaram de fora: %s", got.series)
	}
}

// The load has to stop on every exit path, so `end` is called twice on the ordinary one:
// once by the phase that reads the streams, once by the deferred call that covers the
// error returns. Calling it twice has to be safe, or the safety net deadlocks the run.
func TestTheLoadStopsTwice(t *testing.T) {
	t.Parallel()

	gen := &load{stop: func() {}}
	first := gen.end()
	done := make(chan int64, 1)
	go func() { done <- gen.end() }()
	select {
	case second := <-done:
		if second != first {
			t.Errorf("a segunda parada contou %d comandos e a primeira %d", second, first)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a segunda chamada de end travou, entao o defer que protege as saidas de erro trava a corrida")
	}
}

// Pending and lag are two ways for a group to still hold work, and a run that wanted both
// at once would call a stuck consumer fine.
func TestAGroupHoldsWorkUnderEitherNumber(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		pending, lag int64
		offender     bool
	}{
		"nada pendente e nada atrasado":             {0, 0, false},
		"entregue e nao confirmado, sem nada atras": {1, 0, true},
		"nada entregue, e entradas esperando":       {0, 3, true},
		"pendente e atrasado ao mesmo tempo":        {2, 5, true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got := groupOffender("wacbench1:cmd:s1", redis.XInfoGroup{
				Name: "connector", Pending: tc.pending, Lag: tc.lag,
			})
			if (got != "") != tc.offender {
				t.Fatalf("pendentes=%d lag=%d rendeu %q", tc.pending, tc.lag, got)
			}
			if tc.offender && !strings.Contains(got, "wacbench1:cmd:s1") {
				t.Errorf("a evidencia nao nomeia o stream: %q", got)
			}
		})
	}
}

// An answer that does not parse is not an answer this can vouch for, and it is also not
// evidence to drop: dropping it is how two disagreeing replies stop being a finding
// because neither could be read.
func TestAnUnreadableAnswerIsStillCompared(t *testing.T) {
	t.Parallel()

	rep := &report{}
	assertNoDuplicateEffect(rep, map[string][]string{
		"c1": {`nao e json`, `tambem nao e json, e e outro texto`},
	}, nil, "")
	got := only(t, rep)
	if got.state() != "QUEBRADO" {
		t.Fatalf("duas respostas ilegiveis e diferentes sairam %q (evidencia: %s)", got.state(), got.detail)
	}
}

// A reply list expires, and the run has to say so instead of reading an expired list as a
// command that answered once.
func TestAnExpiredReadIsSaidOutLoud(t *testing.T) {
	t.Parallel()

	aged := "os comandos em voo foram lidos 3m0s depois de enviados, e a lista de resposta expira em 1m0s"
	rep := &report{}
	assertNoDuplicateEffect(rep, map[string][]string{}, []idempotentPair{{
		messageID: "m1", sid: "s1", firstID: "c-a", secondID: "c-b",
		first: json.RawMessage(`{"message_id":"m1"}`), second: json.RawMessage(`{"message_id":"m1"}`),
		gap: 50 * time.Millisecond,
	}}, aged)
	got := only(t, rep)
	if got.state() != "AFIRMADO" {
		t.Fatalf("a metade dos pares nao rendeu veredito: %q", got.state())
	}
	// The other half did not run, and a run that does not say so claims to have measured
	// a population it never read.
	found := false
	for _, note := range rep.notes {
		if note == aged {
			found = true
		}
	}
	if !found {
		t.Errorf("a corrida nao diz que leu as respostas depois do TTL:\n%v", rep.notes)
	}
}

// A claim that was never measured is not a pass, and a run that ends on one is not green.
//
// The case this comes from is real: when the consumer group never drains, the delivery
// claim comes out NAO MEDIDO on purpose, because a deadline that passed does not tell a
// busy fleet from a hole. Read as green, a connector that stopped reclaiming pending
// commands altogether would pass this bench with the delivery it stopped doing unverified.
func TestAnUnmeasuredClaimIsNotGreen(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		assertions []*assertion
		outside    bool
		want       outcome
	}{
		"tudo afirmado": {
			assertions: []*assertion{{claim: "a", points: 1, held: true}},
			want:       outcomeGreen,
		},
		"uma afirmada e uma sem medida": {
			assertions: []*assertion{
				{claim: "a", points: 1, held: true},
				{claim: "entrega de comando", points: 5, held: true, notWhy: "a frota ainda estava trabalhando"},
			},
			want: outcomeSetup,
		},
		"serie vazia tambem nao e verde": {
			assertions: []*assertion{{claim: "a", points: 0, held: true}},
			want:       outcomeSetup,
		},
		"quebrada vence sem medida": {
			assertions: []*assertion{
				{claim: "a", points: 1, held: false},
				{claim: "b", points: 1, held: true, notWhy: "nao deu"},
			},
			want: outcomeInvariant,
		},
		"sem medida vence numero fora da faixa": {
			// Both are "this run did not answer the question", and the one that says a
			// claim went unchecked is the one a reader has to act on first.
			assertions: []*assertion{{claim: "a", points: 1, held: true, notWhy: "nao deu"}},
			outside:    true,
			want:       outcomeSetup,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rep := &report{}
			for _, a := range tc.assertions {
				rep.assert(a)
			}
			if tc.outside {
				rep.measure("f", "m", 1, "u")
				rep.measurements[0].outside = true
			}
			if got := rep.outcome(); got != tc.want {
				t.Fatalf("saiu %d (%s), queria %d (%s)", got, got.label(), tc.want, tc.want.label())
			}
		})
	}
}

// And the outcome names what went unmeasured, so the reader does not have to walk the
// whole report looking for the line that said so.
func TestTheOutcomeNamesWhatWentUnmeasured(t *testing.T) {
	t.Parallel()

	rep := &report{}
	rep.assert(&assertion{claim: "grupo consumidor sem buraco", points: 5, held: true, notWhy: "ainda trabalhando"})
	var out strings.Builder
	rep.write(&out, rep.outcome(), nil)
	if !strings.Contains(out.String(), "nao foi medido, e por isso esta corrida nao e verde") {
		t.Errorf("o desfecho nao diz que algo ficou sem medida:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "- grupo consumidor sem buraco") {
		t.Errorf("o desfecho nao nomeia a afirmacao que ficou sem medida:\n%s", out.String())
	}
}

// The fence half of invariant 1 is claimed as an assertion with a series, so a run where
// no peer adopted anything cannot end green over a fence it never reached.
func TestTheFenceIsAClaimWithASeries(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		taken int
		state string
	}{
		"um par assumiu com o dono parado": {2, "AFIRMADO"},
		"ninguem assumiu":                  {0, "NAO MEDIDO"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rep := &report{}
			// Through the function the phase itself calls, and not through an assertion
			// built here to agree with it: a copy of the verdict in the test is a test that
			// passes whatever the phase does.
			rep.assert(fenceExercised(tc.taken, "serie", "nenhum par assumiu sessao nenhuma"))
			if got := only(t, rep).state(); got != tc.state {
				t.Fatalf("com %d adocoes o estado saiu %q, queria %q", tc.taken, got, tc.state)
			}
			// And the run's code follows the state, which is the whole point of claiming it
			// instead of noting it.
			want := outcomeGreen
			if tc.taken == 0 {
				want = outcomeSetup
			}
			if got := rep.outcome(); got != want {
				t.Errorf("o desfecho saiu %s, queria %s", got.label(), want.label())
			}
		})
	}
}

// A stream that did not grow since the last look is not a stream that stopped: it can be a
// publisher between two events, and this bench produces bursts on purpose.
func TestStillnessNeedsMoreThanOneQuietReading(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		readings []string
		settled  bool
	}{
		"uma leitura so nao basta":            {[]string{"0:10:5-1|"}, false},
		"duas iguais ainda nao bastam":        {[]string{"0:10:5-1|", "0:10:5-1|"}, false},
		"tres iguais seguidas bastam":         {[]string{"0:10:5-1|", "0:10:5-1|", "0:10:5-1|"}, true},
		"o publicador entre dois eventos":     {[]string{"0:10:5-1|", "0:10:5-1|", "0:12:7-1|", "0:12:7-1|"}, false},
		"a contagem recomeca depois de subir": {[]string{"0:10:5-1|", "0:10:5-1|", "0:12:7-1|", "0:12:7-1|", "0:12:7-1|"}, true},
		"crescendo o tempo todo":              {[]string{"0:1:1-1|", "0:2:2-1|", "0:3:3-1|", "0:4:4-1|"}, false},
		// The case the length alone cannot see: a shard at DefaultEventMaxLen keeps its
		// length while approximate trimming replaces its entries.
		"stream cheio, comprimento parado, id andando": {
			[]string{"0:1000:900-1|", "0:1000:901-1|", "0:1000:902-1|", "0:1000:903-1|"}, false},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var quiet stillness
			settled := false
			for _, mark := range tc.readings {
				settled = quiet.saw(mark)
				if settled {
					break
				}
			}
			if settled != tc.settled {
				t.Fatalf("depois de %v a espera %s, e devia %s", tc.readings,
					map[bool]string{true: "terminou", false: "seguiu"}[settled],
					map[bool]string{true: "terminar", false: "seguir"}[tc.settled])
			}
		})
	}
}

// A snapshot taken while the shards were still growing cannot give a verdict about
// ordering: the event that is missing may not have arrived yet.
func TestAnUnsettledStreamLeavesTheStreamClaimsUnmeasured(t *testing.T) {
	t.Parallel()

	growing := "os shards de evento ainda cresciam quando o prazo acabou"

	// The fence claim comes from the frozen phase and its series is adoptions, so it is
	// not one of the claims a growing stream leaves unmeasured.
	rep := &report{}
	rep.assert(fenceExercised(2, "duas sessoes assumidas", ""))
	fromStreams := len(rep.assertions)
	rep.assert(&assertion{claim: "seq estritamente crescente", points: 4, held: true})
	rep.assert(&assertion{claim: "sem buraco em seq", points: 4, held: true,
		notWhy: "nenhuma serie sobrou depois de tirar os streams truncados"})
	// Through the function the run itself calls, over the same slice it hands it.
	markUnmeasured(rep.assertions[fromStreams:], growing)

	if got := rep.assertions[0].state(); got != "AFIRMADO" {
		t.Errorf("a cerca, que nao le stream, saiu %q", got)
	}
	for _, a := range rep.assertions[1:] {
		if a.state() != "NAO MEDIDO" {
			t.Errorf("%q saiu %q sobre um stream que ainda crescia", a.claim, a.state())
		}
	}
	// The specific reason wins over the general one: "no series survived the truncation"
	// says more than "the stream was still growing", and the second would bury it.
	if got := rep.assertions[2].notWhy; got == growing {
		t.Errorf("a razao especifica foi trocada pela geral: %q", got)
	}
	// And a run whose streams settled changes nothing.
	quiet := &report{}
	quiet.assert(&assertion{claim: "seq", points: 4, held: true})
	markUnmeasured(quiet.assertions, "")
	if got := quiet.assertions[0].state(); got != "AFIRMADO" {
		t.Errorf("com os shards parados a afirmacao saiu %q", got)
	}
	if got := rep.outcome(); got != outcomeSetup {
		t.Errorf("o desfecho saiu %s, e uma corrida que leu sequencia parcial nao e verde", got.label())
	}
}

// More events cannot undo a violation that was already observed, so an unsettled stream
// must not turn a defect into an incomplete run.
func TestAProvenViolationSurvivesAnUnsettledStream(t *testing.T) {
	t.Parallel()

	rep := &report{}
	rep.assert(&assertion{claim: "seq nao diminui", points: 4, held: false,
		detail: "s1 no epoch 2: seq 3 veio depois de seq 7"})
	rep.assert(&assertion{claim: "cada sid num shard so", points: 4, held: true})
	markUnmeasured(rep.assertions, "os shards ainda cresciam quando o prazo acabou")

	if got := rep.assertions[0].state(); got != "QUEBRADO" {
		t.Fatalf("a violacao ja observada virou %q, e evento que faltava chegar so somaria evidencia", got)
	}
	if got := rep.assertions[1].state(); got != "NAO MEDIDO" {
		t.Errorf("a afirmacao que valia sobre leitura parcial saiu %q", got)
	}
	if got := rep.outcome(); got != outcomeInvariant {
		t.Errorf("o desfecho saiu %s, e um defeito nao pode virar corrida incompleta", got.label())
	}
}

// Two readings are compared by every shard's length AND the id of its last entry, because
// a full shard keeps its length while approximate trimming replaces its entries.
func TestTheStreamMarkCarriesTheLastEntryID(t *testing.T) {
	t.Parallel()

	full := []shardRead{
		{stream: "wacbench1:events:0", length: 1000, firstID: "900-1", lastID: "1900-1"},
		{stream: "wacbench1:events:1", length: 1000, firstID: "880-1", lastID: "1880-1"},
	}
	trimmed := []shardRead{
		{stream: "wacbench1:events:0", length: 1000, firstID: "901-1", lastID: "1901-1"},
		{stream: "wacbench1:events:1", length: 1000, firstID: "880-1", lastID: "1880-1"},
	}
	if streamMark(full) == streamMark(trimmed) {
		t.Errorf("dois instantes de um shard cheio que seguiu publicando deram a mesma marca:\n  %s",
			streamMark(full))
	}
	// Stable across two readings that found the same thing, which is what lets three
	// equal marks in a row mean the shards held still.
	again := []shardRead{
		{stream: "wacbench1:events:0", length: 1000, firstID: "900-1", lastID: "1900-1"},
		{stream: "wacbench1:events:1", length: 1000, firstID: "880-1", lastID: "1880-1"},
	}
	if streamMark(full) != streamMark(again) {
		t.Errorf("duas leituras iguais deram marcas diferentes:\n  %s\n  %s",
			streamMark(full), streamMark(again))
	}
}

// A command stream of this run that is gone, or that nobody consumed, is a finding: it was
// written to earlier in the same run.
func TestAMissingStreamIsAFindingAndNotASkip(t *testing.T) {
	t.Parallel()

	full := []redis.XInfoGroup{{Name: "connector", Pending: 0, Lag: 0}}
	tests := map[string]struct {
		groups    []redis.XInfoGroup
		err       error
		offenders int
		checked   int
		fatal     bool
	}{
		"grupo vazio e o caso bom":       {full, nil, 0, 1, false},
		"grupo com trabalho parado":      {[]redis.XInfoGroup{{Name: "connector", Pending: 2}}, nil, 1, 1, false},
		"o stream sumiu":                 {nil, redis.Nil, 1, 0, false},
		"o stream nao tem grupo nenhum":  {nil, nil, 1, 0, false},
		"a leitura falhou por outra via": {nil, errors.New("connection refused"), 0, 0, true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			offenders, checked, fatal := inspectGroups("wacbench1:cmd:s1", tc.groups, tc.err)
			if (fatal != nil) != tc.fatal {
				t.Fatalf("erro fatal = %v, queria %v", fatal, tc.fatal)
			}
			if len(offenders) != tc.offenders {
				t.Errorf("%d achados, queria %d: %v", len(offenders), tc.offenders, offenders)
			}
			if checked != tc.checked {
				t.Errorf("%d grupos contados, queria %d", checked, tc.checked)
			}
			if tc.offenders > 0 && !strings.Contains(offenders[0], "wacbench1:cmd:s1") {
				t.Errorf("o achado nao nomeia o stream: %q", offenders[0])
			}
		})
	}
}

// A violation already in the report outranks a reading that failed after it, and a run
// that stopped because of one exits on the defect rather than on "the machine".
func TestTheStoppedOutcomePrefersWhatWasAlreadyFound(t *testing.T) {
	t.Parallel()

	withBroken := func() *report {
		rep := &report{}
		rep.assert(&assertion{claim: "duas instancias com uma lease", points: 4, held: false,
			detail: "s1: epoch 2 e epoch 4 publicando juntos"})
		return rep
	}
	clean := func() *report {
		rep := &report{}
		rep.assert(&assertion{claim: "c", points: 4, held: true})
		return rep
	}

	tests := map[string]struct {
		rep  *report
		err  error
		want outcome
	}{
		"parou por invariante quebrada":         {withBroken(), errInvariantBroken, outcomeInvariant},
		"leitura final falhou depois da quebra": {withBroken(), errSetup, outcomeInvariant},
		"parou por setup, sem quebra nenhuma":   {clean(), errSetup, outcomeSetup},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := stoppedOutcome(tc.rep, tc.err); got != tc.want {
				t.Fatalf("saiu %s, queria %s", got.label(), tc.want.label())
			}
		})
	}
}

// The worst case this claim can find is every expected stream gone, and that one must not
// print as "not measured": there were findings, so something was examined.
func TestFindingsWithoutGroupsStillCountAsMeasured(t *testing.T) {
	t.Parallel()

	gone := []string{
		"wacbench1:cmd:s1: o stream nao existe mais, e esta corrida escreveu nele",
		"wacbench1:cmd:s2: o stream nao existe mais, e esta corrida escreveu nele",
	}
	got := consumerGroupVerdict(0, 3, gone, "")
	if got.state() != "QUEBRADO" {
		t.Fatalf("com %d achados e nenhum grupo contado o estado saiu %q", len(gone), got.state())
	}
	rep := &report{}
	rep.assert(got)
	if out := rep.outcome(); out != outcomeInvariant {
		t.Errorf("o desfecho saiu %s, e streams desta corrida que sumiram sao defeito, nao maquina", out.label())
	}
}

// Work interrupted and load still arriving are different facts, and their sum answers
// neither.
func TestTheKillAftermathKeepsTheTwoHalvesApart(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		left        backlog
		cut, unread float64
		says        string
	}{
		"o dono morreu no meio do trabalho": {
			left: backlog{pending: 10, lag: 1}, cut: 10, unread: 1,
			says: "10 entradas seguiam ENTREGUES",
		},
		"o dono confirmou tudo antes de morrer": {
			// The case the sum got wrong: a fast owner with the whole batch acknowledged,
			// and a load that kept arriving. Summed, this reported five commands of
			// interrupted work with an empty pending list.
			left: backlog{pending: 0, lag: 5}, cut: 0, unread: 5,
			says: "NAO interrompeu trabalho",
		},
		"nada pendente e nada atrasado": {
			left: backlog{}, cut: 0, unread: 0,
			says: "NAO interrompeu trabalho",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cut, unread, note := killAftermath("bench-1-a", 16, tc.left)
			if cut != tc.cut || unread != tc.unread {
				t.Fatalf("cortado=%v nao lido=%v, queria %v e %v", cut, unread, tc.cut, tc.unread)
			}
			if !strings.Contains(note, tc.says) {
				t.Errorf("a nota nao diz %q:\n%s", tc.says, note)
			}
		})
	}
}

// A late write from a previous owner is not ownership moving back to it.
func TestEpochRisesReadsWhatAClientWouldAccept(t *testing.T) {
	t.Parallel()

	// b took the session under epoch 2, then a's in-flight write from epoch 1 landed. A
	// client drops that one on the cursor; counted as a handover, it reads as ownership
	// returning to a under a lower epoch, and the run called that a broken invariant.
	rep := &report{}
	assertEpochRises(rep, map[string][]protocol.Event{
		"s1": {event("s1", "a", 1, 1), event("s1", "b", 2, 1), event("s1", "a", 1, 2)},
	})
	got := only(t, rep)
	if got.state() == "QUEBRADO" {
		t.Fatalf("a escrita atrasada do dono anterior foi lida como troca de posse:\n%s", got.detail)
	}
	if got.series != "1 trocas de dono observadas sobre 1 sessoes" {
		t.Errorf("a serie contou o evento atrasado como troca: %s", got.series)
	}

	// And a real handover that does not raise the epoch is still a finding.
	broken := &report{}
	assertEpochRises(broken, map[string][]protocol.Event{
		"s1": {event("s1", "a", 2, 1), event("s1", "b", 2, 2)},
	})
	if state := only(t, broken).state(); state != "QUEBRADO" {
		t.Errorf("uma troca de dono sob o mesmo epoch saiu %q", state)
	}
}

// The claim has to say what it checks, and it stopped checking one of the things it used
// to say.
//
// It read "nenhum evento de epoch velho depois de um novo no mesmo stream" while the run
// had started allowing exactly one of those -- the write in flight, which the contract
// names and the client drops on the cursor. A claim printed over a run says what that run
// proved, and this one would have said more.
func TestTheOneOwnerClaimSaysWhatItChecks(t *testing.T) {
	t.Parallel()

	rep := &report{}
	assertOneOwner(rep, map[string][]protocol.Event{"s1": {event("s1", "a", 1, 1)}},
		map[string]map[string][]protocol.Event{"s1": {"wa:events:0": {event("s1", "a", 1, 1)}}},
		&census{}, 1)
	claim := only(t, rep).claim

	if strings.Contains(claim, "nenhum evento de epoch velho") {
		t.Errorf("a afirmacao ainda promete que nenhum evento de epoch velho aparece, e um aparece "+
			"por desenho:\n%s", claim)
	}
	for _, half := range []string{"um epoch, um publicador", "publicando de novo", "mais sessoes do que existem sids"} {
		if !strings.Contains(claim, half) {
			t.Errorf("a afirmacao nao menciona %q, que e uma das tres leituras que ela faz:\n%s", half, claim)
		}
	}
}

// A session that published nothing is a finding, and every other reading is blind to it:
// they all walk what was published.
func TestASilentSessionIsAFinding(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		shardOf map[string]map[string]bool
		sids    []string
		state   string
		says    string
	}{
		"as duas publicaram, cada uma num shard": {
			shardOf: map[string]map[string]bool{
				"s1": {"wa:events:0": true}, "s2": {"wa:events:1": true},
			},
			sids:  []string{"s1", "s2"},
			state: "AFIRMADO",
		},
		"uma delas nao publicou nada": {
			shardOf: map[string]map[string]bool{"s1": {"wa:events:0": true}},
			sids:    []string{"s1", "s2"},
			state:   "QUEBRADO",
			says:    "nao publicou evento nenhum",
		},
		"uma delas apareceu em dois shards": {
			shardOf: map[string]map[string]bool{
				"s1": {"wa:events:0": true, "wa:events:1": true}, "s2": {"wa:events:1": true},
			},
			sids:  []string{"s1", "s2"},
			state: "QUEBRADO",
			says:  "apareceu em",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rep := &report{}
			firstOn := map[string]map[string]string{}
			for sid, streams := range tc.shardOf {
				firstOn[sid] = map[string]string{}
				for stream := range streams {
					firstOn[sid][stream] = "1-0"
				}
			}
			assertOneShard(rep, tc.shardOf, firstOn, tc.sids)
			got := only(t, rep)
			if got.state() != tc.state {
				t.Fatalf("estado %q, queria %q (evidencia: %s)", got.state(), tc.state, got.detail)
			}
			if tc.says != "" && !strings.Contains(got.detail, tc.says) {
				t.Errorf("a evidencia nao diz %q:\n%s", tc.says, got.detail)
			}
		})
	}
}

// A drain deadline explains entries still pending. It does not explain a stream that is
// gone, and downgrading that to "not measured" turns a deletion into exit 2.
func TestAMissingStreamSurvivesADrainTimeout(t *testing.T) {
	t.Parallel()

	busy := "o grupo consumidor nao drenou em 90 s"
	pending := []string{"wacbench1:cmd:s1, grupo connector: 2 pendentes e lag 0 depois dos acks"}
	gone := []string{"wacbench1:cmd:s2: o stream nao existe mais, e esta corrida escreveu nele"}

	if got := consumerGroupVerdict(5, 5, pending, busy).state(); got != "NAO MEDIDO" {
		t.Errorf("pendencia sob prazo estourado saiu %q", got)
	}
	if got := consumerGroupVerdict(5, 5, gone, busy).state(); got != "QUEBRADO" {
		t.Errorf("um stream apagado sob prazo estourado saiu %q, e ele nao e trabalho em curso", got)
	}
	if got := consumerGroupVerdict(5, 5, append(append([]string{}, pending...), gone...), busy).state(); got != "QUEBRADO" {
		t.Errorf("com pendencia E stream apagado o veredito saiu %q", got)
	}
}

// The case the stream reading cannot decide, decided by the counter the epoch comes from.
//
// A handover to an instance that publishes exactly ONE event under a regressed epoch has
// the same shape in the stream as the in-flight write the contract allows, and both
// assertions that walk the streams tolerate it: `assertEpochRises` drops it the way a
// client would, and `assertOneOwner` counts one late event per instance as the write that
// was in flight. So the epoch is checked where it is handed out instead.
func TestTheEpochCounterIsReadWhereTheEpochComesFrom(t *testing.T) {
	t.Parallel()

	// `says` is checked and not only the count, because the two offenders are two different
	// diagnoses and one of them collapses into the other on its own: with the missing-key
	// branch gone, a session with no counter reads as a counter of zero and still comes out
	// as one offender, under the wrong sentence. The number alone cannot see that.
	cases := []struct {
		name      string
		published map[string][]protocol.Event
		counters  map[string]uint64
		checked   int
		offenders int
		says      string
	}{
		{
			name:      "contador acima do publicado e a corrida saudavel",
			published: map[string][]protocol.Event{"s1": {event("s1", "a", 1, 1), event("s1", "b", 2, 1)}},
			counters:  map[string]uint64{"s1": 2},
			checked:   1,
		},
		{
			name:      "contador abaixo do epoch ja publicado e a geracao sendo reemitida",
			published: map[string][]protocol.Event{"s1": {event("s1", "a", 3, 1)}},
			counters:  map[string]uint64{"s1": 1},
			checked:   1,
			offenders: 1,
			says:      "contador de epoch em 1, abaixo do epoch 3",
		},
		{
			name:      "sessao que publicou e nao tem contador nenhum recomeca no 1",
			published: map[string][]protocol.Event{"s1": {event("s1", "a", 4, 1)}},
			counters:  map[string]uint64{},
			checked:   1,
			offenders: 1,
			says:      "nao tem contador de epoch nenhum",
		},
		{
			// Zero is not a generation: the counter only exists from the first INCR, so a
			// session whose events all carry zero never had one to fall below.
			name:      "epoch zero nao e geracao, entao nao ha o que comparar",
			published: map[string][]protocol.Event{"s1": {event("s1", "a", 0, 1)}},
			counters:  map[string]uint64{},
		},
		{
			name: "uma sessao regredida entre sessoes sas e achado so dela",
			published: map[string][]protocol.Event{
				"s1": {event("s1", "a", 2, 1)},
				"s2": {event("s2", "b", 5, 1)},
				"s3": {event("s3", "c", 1, 1)},
			},
			counters: map[string]uint64{"s1": 2, "s2": 4, "s3": 1},
			checked:  3,
			// only s2, whose counter reads below the epoch it already published under
			offenders: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			offenders, checked := epochCounterOffenders(tc.published, tc.counters)
			if checked != tc.checked {
				t.Errorf("conferiu %d sessoes, esperado %d", checked, tc.checked)
			}
			if len(offenders) != tc.offenders {
				t.Errorf("achou %d infratores, esperado %d:\n%s",
					len(offenders), tc.offenders, strings.Join(offenders, "\n"))
			}
			if tc.says != "" && !strings.Contains(strings.Join(offenders, "\n"), tc.says) {
				t.Errorf("o achado nao diz %q, entao ele nomeia o diagnostico errado:\n%s",
					tc.says, strings.Join(offenders, "\n"))
			}
		})
	}
}

// A regression the streams tolerate has to come out red somewhere, and this is where.
func TestTheRegressedEpochIsRedAtTheCounterAndNotInTheStream(t *testing.T) {
	t.Parallel()

	// a/1, b/3, c/2: c took the session under a generation below one already published,
	// and it published exactly one event, which is the shape of an allowed in-flight write.
	published := map[string][]protocol.Event{
		"s1": {event("s1", "a", 1, 1), event("s1", "b", 3, 1), event("s1", "c", 2, 1)},
	}

	stream := &report{}
	assertEpochRises(stream, published)
	if state := only(t, stream).state(); state == "QUEBRADO" {
		t.Errorf("a leitura de stream deu veredito sobre um caso que ela nao distingue: %s", state)
	}

	counter := &report{}
	assertEpochCounter(counter, published, map[string]uint64{"s1": 2}, "wa:lease-epoch:<sid>")
	got := only(t, counter)
	if got.state() != "QUEBRADO" {
		t.Fatalf("o contador em 2, abaixo do epoch 3 ja publicado, saiu %q", got.state())
	}
	if !strings.Contains(got.detail, "s1") {
		t.Errorf("o detalhe nao nomeia a sessao:\n%s", got.detail)
	}
	if !strings.Contains(got.series, "wa:lease-epoch:<sid>") {
		t.Errorf("a serie nao diz contra o que comparou:\n%s", got.series)
	}
}

// Nothing published means nothing to compare, and that is NAO MEDIDO and not a pass.
func TestTheEpochCounterClaimIsNotGreenWithoutEvents(t *testing.T) {
	t.Parallel()

	rep := &report{}
	assertEpochCounter(rep, map[string][]protocol.Event{}, map[string]uint64{"s1": 7}, "wa:lease-epoch:<sid>")
	if state := only(t, rep).state(); state != "NAO MEDIDO" {
		t.Errorf("uma corrida sem evento nenhum afirmou o contador de epoch: %q", state)
	}
}

// The stream claim used to promise the whole of invariant 2, and it stopped checking part
// of it on purpose. A claim printed over a run says what that run proved.
func TestTheEpochRisesClaimSaysItReadsWhatAClientAccepts(t *testing.T) {
	t.Parallel()

	rep := &report{}
	assertEpochRises(rep, map[string][]protocol.Event{
		"s1": {event("s1", "a", 1, 1), event("s1", "b", 2, 1)},
	})
	claim := only(t, rep).claim
	for _, half := range []string{"eventos que um cliente", "contador"} {
		if !strings.Contains(claim, half) {
			t.Errorf("a afirmacao nao diz %q, entao ela promete a invariante 2 inteira sobre uma "+
				"leitura que nao a decide inteira:\n%s", half, claim)
		}
	}
}
