package whatsmeow

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	wm "go.mau.fi/whatsmeow"
	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

func inviteCommand(t *testing.T, payload string) *protocol.Command {
	t.Helper()
	return &protocol.Command{
		V: protocol.Version, ID: "c1", Type: protocol.CommandGroupInviteGet,
		SID: "s1", Payload: json.RawMessage(payload),
	}
}

const anInvitedGroup = `{"group":{"kind":"group","id":"120363000000000001"}}`

func TestAnInviteRequestRefusesAPayloadThatNamesNoGroup(t *testing.T) {
	t.Parallel()

	for _, refused := range []struct {
		name    string
		payload string
	}{
		{name: "no payload at all", payload: `{}`},
		{name: "a group with no id", payload: `{"group":{"kind":"group","id":""}}`},
		// A person has no invite link. Sending it on would have WhatsApp refuse a group
		// IQ addressed to somebody, which reaches the client as an opaque `wa_error`.
		{name: "a chat that is not a group", payload: `{"group":{"kind":"phone","id":"5511999990002"}}`},
	} {
		t.Run(refused.name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			session.inviteLink = func(context.Context, *wm.Client, waTypes.JID, bool) (string, error) {
				t.Error("a payload that names no group was asked about anyway")
				return "", nil
			}

			_, err := session.Execute(t.Context(), inviteCommand(t, refused.payload))
			assertCode(t, err, protocol.ErrorInvalidPayload)
		})
	}
}

// Both halves of the answer, because they are two things a client does two things with:
// the code is what it stores and compares, the URL is what an operator sends somebody.
func TestAnInviteRequestAnswersTheCodeAndTheLink(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.inviteLink = func(_ context.Context, _ *wm.Client, group waTypes.JID, revoke bool) (string, error) {
		if group.Server != waTypes.GroupServer || group.User != "120363000000000001" {
			t.Errorf("the request was addressed to %s, want the group that was asked", group)
		}
		if revoke {
			t.Error("a request that did not ask to revoke revoked the group's link anyway")
		}
		return wm.InviteLinkPrefix + "ABCdef123", nil
	}

	result, err := session.Execute(t.Context(), inviteCommand(t, anInvitedGroup))
	if err != nil {
		t.Fatalf("group.invite.get: %v", err)
	}
	var invite groupInvite
	if err := json.Unmarshal(result, &invite); err != nil {
		t.Fatalf("unmarshal the answer: %v", err)
	}
	if invite.Code != "ABCdef123" {
		t.Errorf("the code came back as %q, want it without the link prefix", invite.Code)
	}
	if invite.URL != wm.InviteLinkPrefix+"ABCdef123" {
		t.Errorf("the url came back as %q, want the whole link", invite.URL)
	}
}

// Revoking is not a read: whoever holds the previous link can no longer join, and there
// is no undo. A request that did not ask for it must never reach WhatsApp as one that
// did, which is what this pins.
func TestAnInviteRequestRevokesOnlyWhenItWasAsked(t *testing.T) {
	t.Parallel()

	for _, asked := range []struct {
		name    string
		payload string
		want    bool
	}{
		{name: "asked to revoke", payload: `{"group":{"kind":"group","id":"120363000000000001"},"revoke":true}`, want: true},
		{name: "asked not to", payload: `{"group":{"kind":"group","id":"120363000000000001"},"revoke":false}`, want: false},
		{name: "said nothing about it", payload: anInvitedGroup, want: false},
	} {
		t.Run(asked.name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			session.inviteLink = func(_ context.Context, _ *wm.Client, _ waTypes.JID, revoke bool) (string, error) {
				if revoke != asked.want {
					t.Errorf("WhatsApp was asked to revoke=%v, want %v", revoke, asked.want)
				}
				return wm.InviteLinkPrefix + "ABCdef123", nil
			}

			if _, err := session.Execute(t.Context(), inviteCommand(t, asked.payload)); err != nil {
				t.Fatalf("group.invite.get: %v", err)
			}
		})
	}
}

// A link this build cannot take apart is refused rather than passed on whole. A client
// that stored the URL as the code would compare it against codes forever after, and the
// prefix is whatsmeow's own constant, so this is it having changed under us.
func TestAnInviteRequestRefusesALinkItCannotReadACodeOutOf(t *testing.T) {
	t.Parallel()

	for _, unreadable := range []struct {
		name string
		link string
	}{
		{name: "a link with another prefix", link: "https://wa.me/join/ABCdef123"},
		{name: "the prefix and nothing after it", link: wm.InviteLinkPrefix},
		{name: "nothing at all", link: ""},
	} {
		t.Run(unreadable.name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			session.inviteLink = func(context.Context, *wm.Client, waTypes.JID, bool) (string, error) {
				return unreadable.link, nil
			}

			_, err := session.Execute(t.Context(), inviteCommand(t, anInvitedGroup))
			assertCode(t, err, protocol.ErrorInternal)
		})
	}
}

// Three refusals whatsmeow separates and the contract has one code for, so the message is
// what tells them apart -- and they send an operator somewhere different: ask an admin,
// check the group, or rejoin it.
func TestAnInviteRequestSaysWhichRefusalItGot(t *testing.T) {
	t.Parallel()

	for _, refused := range []struct {
		name string
		err  error
		want protocol.ErrorCode
		says string
	}{
		{name: "not an admin", err: wm.ErrGroupInviteLinkUnauthorized, want: protocol.ErrorWaError, says: "administrator"},
		{name: "no such group", err: wm.ErrGroupNotFound, want: protocol.ErrorWaError, says: "does not know this group"},
		{name: "not in the group", err: wm.ErrNotInGroup, want: protocol.ErrorWaError, says: "not in the group"},
		{name: "the connection went", err: wm.ErrNotConnected, want: protocol.ErrorNotConnected, says: ""},
		{name: "rate limited", err: &wm.IQError{Code: 429}, want: protocol.ErrorRateLimited, says: ""},
	} {
		t.Run(refused.name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			session.inviteLink = func(context.Context, *wm.Client, waTypes.JID, bool) (string, error) {
				return "", refused.err
			}

			_, err := session.Execute(t.Context(), inviteCommand(t, anInvitedGroup))
			assertCode(t, err, refused.want)
			if errors.Is(err, refused.err) {
				t.Error("whatsmeow's own error reached the client instead of a code from the contract")
			}
			if refused.says != "" && !strings.Contains(err.Error(), refused.says) {
				t.Errorf("the refusal reads %q, want it to say %q", err, refused.says)
			}
		})
	}
}

// Asked before the socket: a session with no connection has no group to read a link from.
func TestAnInviteRequestNeedsAConnection(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.inviteLink = func(context.Context, *wm.Client, waTypes.JID, bool) (string, error) {
		t.Error("a disconnected session asked WhatsApp for an invite link anyway")
		return "", nil
	}

	_, err := session.Execute(t.Context(), inviteCommand(t, anInvitedGroup))
	assertCode(t, err, protocol.ErrorNotConnected)
}
