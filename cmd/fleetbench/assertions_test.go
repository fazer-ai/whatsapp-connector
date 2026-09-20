package main

import (
	"encoding/json"
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
			assertNoDuplicateEffect(rep, tc.answers, tc.pairs)
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
	}, nil)
	got := only(t, rep)
	if got.state() != "QUEBRADO" {
		t.Fatalf("duas respostas ilegiveis e diferentes sairam %q (evidencia: %s)", got.state(), got.detail)
	}
}
