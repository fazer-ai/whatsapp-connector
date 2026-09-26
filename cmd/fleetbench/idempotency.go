package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// idempotentPair is one message asked for twice, under two command ids.
//
// It exists because the handover on its own does not reach invariant 5. A redelivery
// after a kill is a command the dead owner never answered: the peer carries it out once
// and answers once, so the ledger is never consulted and the claim comes out NAO MEDIDO
// with eight commands behind it. What reaches the ledger is a second command naming a
// message that already went out, which is the same thing the client does when it resends
// after a timeout, and the only shape of it this bench can produce on purpose.
type idempotentPair struct {
	messageID string
	sid       string
	firstID   string
	secondID  string
	first     json.RawMessage
	second    json.RawMessage
	gap       time.Duration
}

// duplicated reports whether the second command did the work again instead of being
// remembered.
//
// Byte equality of the two results, and the gap is what makes it mean something: the fake
// engine stamps every send it carries out with `time.Now().UnixMilli()`, so a second
// execution separated from the first by more than a millisecond cannot produce the same
// bytes. Equal results after a gap of tens of milliseconds are the record answering.
func (p *idempotentPair) duplicated() bool { return !bytes.Equal(p.first, p.second) }

// exerciseIdempotency sends each message twice and collects what came back.
//
// The gap is deliberate and it is the assertion's whole footing rather than a wait for
// the fleet to settle: the first command is answered before the second is sent, so the
// ledger entry is already written, and the clock has moved far enough that a second
// execution could not coincide with the first.
func exerciseIdempotency(ctx context.Context, active *run, cl *client, sids []string) ([]idempotentPair, error) {
	const gap = 50 * time.Millisecond

	pairs := make([]idempotentPair, 0, len(sids))
	for _, sid := range sids {
		messageID := fmt.Sprintf("dup-%s-%s", active.id, shortSID(sid))
		payload := fmt.Sprintf(`{"message_id":%q,"to":{"kind":"phone","id":"5511999990002"},`+
			`"content":{"type":"text","body":"a mesma mensagem, pedida duas vezes"}}`, messageID)

		first := messageID + "-cmd-a"
		if err := cl.send(ctx, cl.keys.Commands(sid), &protocol.Command{
			V: protocol.Version, ID: first, Type: protocol.CommandMessageSend, SID: sid,
			TS: time.Now().UnixMilli(), ReplyTo: cl.keys.Reply(first), Payload: json.RawMessage(payload),
		}); err != nil {
			return nil, fmt.Errorf("%w: send %s for %s: %w", errSetup, messageID, sid, err)
		}
		firstReply, err := cl.await(ctx, first, 60*time.Second)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", errSetup, err)
		}
		if !firstReply.OK {
			return nil, fmt.Errorf("%w: the fleet refused the first send of %s: %w", errSetup, messageID, firstReply.Error)
		}

		sentAt := time.Now()
		timer := time.NewTimer(gap)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}

		second := messageID + "-cmd-b"
		if err := cl.send(ctx, cl.keys.Commands(sid), &protocol.Command{
			V: protocol.Version, ID: second, Type: protocol.CommandMessageSend, SID: sid,
			TS: time.Now().UnixMilli(), ReplyTo: cl.keys.Reply(second), Payload: json.RawMessage(payload),
		}); err != nil {
			return nil, fmt.Errorf("%w: resend %s for %s: %w", errSetup, messageID, sid, err)
		}
		secondReply, err := cl.await(ctx, second, 60*time.Second)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", errSetup, err)
		}
		if !secondReply.OK {
			return nil, fmt.Errorf("%w: the fleet refused the resend of %s: %w", errSetup, messageID, secondReply.Error)
		}

		pairs = append(pairs, idempotentPair{
			messageID: messageID, sid: sid, firstID: first, secondID: second,
			first: firstReply.Result, second: secondReply.Result, gap: time.Since(sentAt),
		})
	}
	return pairs, nil
}
