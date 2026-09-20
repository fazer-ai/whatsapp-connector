package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

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
		"epoch velho depois de um novo, no mesmo stream": {
			// The fence half, and it is invisible to the first: every (sid, epoch) here
			// still has exactly one publisher. What says it is the order.
			events: []protocol.Event{event("s1", "b", 2, 1), event("s1", "a", 1, 3)},
			state:  "QUEBRADO",
			says:   "depois de perder a lease",
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
		"seq repetido": {
			events: []protocol.Event{event("s1", "a", 1, 1), event("s1", "a", 1, 1)},
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
			assertNoDuplicateEffect(rep, nil, tc.pairs)
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
