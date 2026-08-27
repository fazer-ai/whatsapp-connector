package whatsmeow

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	wm "go.mau.fi/whatsmeow"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waStore "go.mau.fi/whatsmeow/store"
	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// A reaction names the message it is on with a key, and a key that resolves to no message
// is accepted by WhatsApp all the same: the send answers with a timestamp and nobody ever
// sees the reaction. So the sender the key is built around is decided here, and what
// cannot be decided is refused rather than guessed.
func TestWhoAReactionIsSaidToBeOn(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		payload string
		chat    string
		want    string
		refused bool
	}{
		{"the account's own message", `"target_from_me":true`, "5511999990002@s.whatsapp.net", "", false},
		// A chat with one other person names its sender on its own: everything in it the
		// account did not send came from whoever it is with.
		{"the contact's message in a direct chat", ``, "5511999990002@s.whatsapp.net",
			"5511999990002@s.whatsapp.net", false},
		{"the contact's, in a chat addressed by LID", ``, "182736451928374@lid",
			"182736451928374@lid", false},
		// A group has many, and the key needs the one.
		{"somebody's message in a group, said whose",
			`"target_participant":{"kind":"phone","id":"5511999990003"}`, "120363000000000000@g.us",
			"5511999990003@s.whatsapp.net", false},
		{"somebody's message in a group, not said whose", ``, "120363000000000000@g.us", "", true},
		// Both, and they cannot both be true.
		{"the account's own and somebody else's at once",
			`"target_from_me":true,"target_participant":{"kind":"phone","id":"5511999990003"}`,
			"5511999990002@s.whatsapp.net", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var req reactRequest
			body := `{"target_id":"3EB0","emoji":"👍"`
			if tc.payload != "" {
				body += "," + tc.payload
			}
			if err := json.Unmarshal([]byte(body+"}"), &req); err != nil {
				t.Fatalf("unmarshal %s: %v", body, err)
			}
			chat, err := waTypes.ParseJID(tc.chat)
			if err != nil {
				t.Fatalf("ParseJID(%q): %v", tc.chat, err)
			}

			got, err := whoSentTheTarget(&req, chat)
			switch {
			case tc.refused && err == nil:
				t.Fatalf("that was built around %s instead of being refused", got)
			case tc.refused:
				assertCode(t, err, protocol.ErrorInvalidPayload)
				return
			case err != nil:
				t.Fatalf("that was refused: %v", err)
			}
			// An empty JID is how whatsmeow's BuildMessageKey is told the target is the
			// account's own, and it renders as the empty string.
			if got.String() != tc.want {
				t.Fatalf("the reaction was built around %q, want %q", got, tc.want)
			}
		})
	}
}

// The sender above is only half of it: what reaches WhatsApp is the key whatsmeow builds
// from it, so the two are checked together. The questions it answers are whose message
// this is and, in a chat with more than one other person, which of them sent it. A key
// that says `from_me` about the contact's message points at a message the account never
// sent, and the reaction lands on nothing.
func TestTheKeyAReactionEndsUpCarrying(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, payload, chat string
		fromMe              bool
		participant         string
	}{
		{"on the account's own message", `"target_from_me":true`,
			"5511999990002@s.whatsapp.net", true, ""},
		// One other person, and the chat already names them: a participant here is a
		// field WhatsApp does not expect in a direct chat.
		{"on the contact's, in a direct chat", ``,
			"5511999990002@s.whatsapp.net", false, ""},
		{"on somebody's, in a group", `"target_participant":{"kind":"phone","id":"5511999990003"}`,
			"120363000000000000@g.us", false, "5511999990003@s.whatsapp.net"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _, _ := outboundSession(t)
			var req reactRequest
			body := `{"target_id":"3EB0","emoji":"👍"`
			if tc.payload != "" {
				body += "," + tc.payload
			}
			if err := json.Unmarshal([]byte(body+"}"), &req); err != nil {
				t.Fatalf("unmarshal %s: %v", body, err)
			}
			chat, err := waTypes.ParseJID(tc.chat)
			if err != nil {
				t.Fatalf("ParseJID(%q): %v", tc.chat, err)
			}
			sender, err := whoSentTheTarget(&req, chat)
			if err != nil {
				t.Fatalf("whoSentTheTarget: %v", err)
			}

			key := session.current().
				BuildReaction(chat, sender, req.TargetID, *req.Emoji).
				GetReactionMessage().GetKey()
			if key.GetFromMe() != tc.fromMe {
				t.Fatalf("the key says from_me=%v, want %v", key.GetFromMe(), tc.fromMe)
			}
			if got := key.GetParticipant(); got != tc.participant {
				t.Fatalf("the key names %q as the sender, want %q", got, tc.participant)
			}
			if got, want := key.GetRemoteJID(), chat.String(); got != want {
				t.Fatalf("the key names %q as the chat, want %q", got, want)
			}
		})
	}
}

// An empty emoji is how the contract says to take a reaction off, and no emoji at all is
// a caller that did not say what to react with. Read into a plain string the two arrive
// the same, and every malformed reaction would quietly remove one instead.
func TestAReactionWithNoEmojiIsNotAReactionThatRemovesOne(t *testing.T) {
	t.Parallel()

	session, _, _ := outboundSession(t)
	_, err := session.react(t.Context(), &protocol.Command{
		Type: protocol.CommandMessageReact,
		Payload: json.RawMessage(`{"to":{"kind":"phone","id":"5511999990002"},
			"target_id":"3EB0A1B2C3D4E5F60718"}`),
	})
	assertCode(t, err, protocol.ErrorInvalidPayload)

	// And the empty one is not refused with it: it is what removing looks like.
	var req reactRequest
	if err := json.Unmarshal([]byte(`{"target_id":"3EB0","emoji":""}`), &req); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if req.Emoji == nil || *req.Emoji != "" {
		t.Fatalf("an empty emoji decoded as %v", req.Emoji)
	}
}

// Every one of the three names a message that already exists, and a command that does not
// name one has nothing to act on. Refused on the payload rather than sent: WhatsApp
// accepts a key built around an empty id and answers success.
func TestACommandThatActsOnNothingIsRefused(t *testing.T) {
	t.Parallel()

	const chat = `"to":{"kind":"phone","id":"5511999990002"}`
	for _, tc := range []struct {
		name    string
		run     func(*Session, string) error
		payload string
	}{
		{"an edit", func(s *Session, p string) error {
			_, err := s.edit(t.Context(), &protocol.Command{
				Type: protocol.CommandMessageEdit, Payload: json.RawMessage(p)})
			return err
		}, `{` + chat + `,"content":{"type":"text","body":"corrigido"}}`},
		{"a revoke", func(s *Session, p string) error {
			_, err := s.revoke(t.Context(), &protocol.Command{
				Type: protocol.CommandMessageRevoke, Payload: json.RawMessage(p)})
			return err
		}, `{` + chat + `}`},
		{"a reaction", func(s *Session, p string) error {
			_, err := s.react(t.Context(), &protocol.Command{
				Type: protocol.CommandMessageReact, Payload: json.RawMessage(p)})
			return err
		}, `{` + chat + `,"emoji":"👍"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _, _ := outboundSession(t)
			assertCode(t, tc.run(session, tc.payload), protocol.ErrorInvalidPayload)
		})
	}
}

// A correction is the whole corrected message, so a caption edit needs the file's upload
// coordinates again and nothing here keeps them once a send is done. Refused with the
// reason rather than sent with coordinates that resolve to nothing, which would replace a
// caption with a broken attachment and report success. See #32.
func TestOnlyATextBodyCanBeCorrected(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, content string
		code          protocol.ErrorCode
	}{
		{"a media caption", `{"type":"media","kind":"image","caption":"outra legenda"}`,
			protocol.ErrorUnsupported},
		{"a location", `{"type":"location","latitude":-25.4,"longitude":-49.2}`,
			protocol.ErrorUnsupported},
		{"a body that does not say what it is", `{"body":"corrigido"}`, protocol.ErrorInvalidPayload},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := editedBody(json.RawMessage(tc.content))
			assertCode(t, err, tc.code)
		})
	}

	// And the one that can.
	corrected, err := editedBody(json.RawMessage(`{"type":"text","body":"corrigido"}`))
	if err != nil {
		t.Fatalf("editedBody: %v", err)
	}
	if got := corrected.GetConversation(); got != "corrigido" {
		t.Fatalf("the correction reads %q", got)
	}
}

// Invariant 5 says a redelivered command must not duplicate a side effect, and for a
// message the last mile of that is the stanza id: the receiving client is what discards
// the second copy, and it discards on the id. The session layer answers a redelivery from
// its record, but the record is written after the send, so a crash between the two hands
// the same command to this code twice -- and an id made up on the spot is different each
// time, which lands a second edit and a second reaction.
func TestAnIdTheCallerLeftOutIsTheSameOnTheSecondTry(t *testing.T) {
	t.Parallel()

	session, _, _ := outboundSession(t)
	for _, tc := range []struct {
		name    string
		command *protocol.Command
	}{
		{"a command identified by its own id", &protocol.Command{
			Type: protocol.CommandMessageReact, ID: "cmd_000012"}},
		{"one identified by the caller's key", &protocol.Command{
			Type: protocol.CommandMessageReact, ID: "cmd_000012", IdempotencyKey: "react-once"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			first := session.orDerived(tc.command, "")
			if again := session.orDerived(tc.command, ""); again != first {
				t.Fatalf("the same command went out as %s and then as %s", first, again)
			}
			if !strings.HasPrefix(first, wm.WebMessageIDPrefix) || len(first) != len(wm.WebMessageIDPrefix)+18 {
				t.Fatalf("%q is not the shape whatsmeow generates", first)
			}
		})
	}

	// A different command is a different message, and two of them sharing a stanza id
	// would have the recipient discard the second as a duplicate of the first.
	react := session.orDerived(&protocol.Command{Type: protocol.CommandMessageReact, ID: "cmd_a"}, "")
	for _, other := range []*protocol.Command{
		{Type: protocol.CommandMessageReact, ID: "cmd_b"},
		{Type: protocol.CommandMessageEdit, ID: "cmd_a"},
		{Type: protocol.CommandMessageReact, ID: "cmd_a", IdempotencyKey: "somebody's key"},
	} {
		if got := session.orDerived(other, ""); got == react {
			t.Fatalf("%s/%s took the same stanza id as react/cmd_a", other.Type, other.ID)
		}
	}

	// And the caller's own id still wins, which is the ordinary case.
	if got := session.orDerived(&protocol.Command{Type: protocol.CommandMessageReact, ID: "cmd_a"}, "3EB0CAFE"); got != "3EB0CAFE" {
		t.Fatalf("the caller named %q and the message went out as %q", "3EB0CAFE", got)
	}
}

// A reaction to somebody's status is a message to that person, and the ordinary send path
// cannot address one: `status@broadcast` is where an account publishes its own status, so
// the stanza is encrypted to every contact in that account's status audience. The author
// gets it only if they happen to be in that list, and everybody else in it gets an
// envelope about a status they may never have seen. See #36.
//
// A revoke is not refused with it: deleting one's own status is exactly a message to that
// audience, and a test that refused both would pin the wrong rule.
func TestOnlyAReactionIsRefusedOnAStatus(t *testing.T) {
	t.Parallel()

	session, _, _ := outboundSession(t)
	const status = `"to":{"kind":"status","id":"status"}`

	_, err := session.react(t.Context(), &protocol.Command{
		Type: protocol.CommandMessageReact,
		Payload: json.RawMessage(`{` + status + `,"target_id":"3EB0A1B2C3D4E5F60718","emoji":"👍",
			"target_participant":{"kind":"phone","id":"5511999990003"}}`),
	})
	assertCode(t, err, protocol.ErrorUnsupported)

	_, revoked := session.revoke(t.Context(), &protocol.Command{
		Type:    protocol.CommandMessageRevoke,
		Payload: json.RawMessage(`{` + status + `,"target_id":"3EB0A1B2C3D4E5F60718"}`),
	})
	var coded *protocol.Error
	if errors.As(revoked, &coded) && coded.Code == protocol.ErrorUnsupported {
		t.Fatalf("deleting one's own status was refused as unsupported: %v", revoked)
	}
}

// A channel names the post a reaction is on with a server id, not with a message key, and
// carries it on a node of its own. Sent the ordinary way it goes out naming a key the
// channel cannot resolve, WhatsApp accepts it, and nobody sees a reaction. See #34.
//
// An edit and a revoke are not in the same position: whatsmeow recognises both on the
// newsletter path and rewrites the stanza id to the target's, so they are not refused and
// a test that refused all three would pin the wrong rule.
func TestOnlyAReactionIsRefusedOnAChannel(t *testing.T) {
	t.Parallel()

	session, _, _ := outboundSession(t)
	const channel = `"to":{"kind":"newsletter","id":"120363000000000000"}`

	_, err := session.react(t.Context(), &protocol.Command{
		Type:    protocol.CommandMessageReact,
		Payload: json.RawMessage(`{` + channel + `,"target_id":"3EB0A1B2C3D4E5F60718","emoji":"👍"}`),
	})
	assertCode(t, err, protocol.ErrorUnsupported)

	// The other two reach the wire, where an unconnected session is what stops them --
	// which is a different answer from `unsupported`, and the point.
	for _, tc := range []struct {
		name string
		run  func() error
	}{
		{"an edit of a channel post", func() error {
			_, err := session.edit(t.Context(), &protocol.Command{
				Type: protocol.CommandMessageEdit,
				Payload: json.RawMessage(`{` + channel + `,"target_id":"3EB0A1B2C3D4E5F60718",
					"content":{"type":"text","body":"corrigido"}}`)})
			return err
		}},
		{"a revoke of one", func() error {
			_, err := session.revoke(t.Context(), &protocol.Command{
				Type:    protocol.CommandMessageRevoke,
				Payload: json.RawMessage(`{` + channel + `,"target_id":"3EB0A1B2C3D4E5F60718"}`)})
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.run()
			var coded *protocol.Error
			if errors.As(err, &coded) && coded.Code == protocol.ErrorUnsupported {
				t.Fatalf("%s was refused as unsupported, and whatsmeow carries it: %v", tc.name, err)
			}
		})
	}
}

// wire records what a command actually handed to WhatsApp, which is the only place the
// stanza id and the built body can be looked at: everything before it is a decision, and
// a test of the decision alone passes whether or not the command uses it.
type wire struct {
	to      waTypes.JID
	id      string
	message *waE2E.Message
	err     error
}

func (w *wire) hand(
	_ context.Context, to waTypes.JID, id string, message *waE2E.Message,
) (wm.SendResponse, error) {
	w.to, w.id, w.message = to, id, message
	if w.err != nil {
		return wm.SendResponse{}, w.err
	}
	return wm.SendResponse{ID: id, Timestamp: time.Unix(1755440000, 0)}, nil
}

func actingSession(t *testing.T) (*Session, *wire) {
	t.Helper()

	session, _, _ := outboundSession(t)
	sent := &wire{}
	session.handOver = sent.hand
	return session, sent
}

// All three go out under an id nothing else will take, and a redelivery has to take the
// same one: the record that answers it is written after the send, so a crash between them
// hands the command to this code again, and the receiving client deduplicates on the
// stanza id. Asserted on what reached the wire rather than on the helper that decides it,
// because a command that computed the right id and then sent another passes the second
// and fails the first.
func TestEachOfTheThreeGoesOutUnderAnIdARetryWillRepeat(t *testing.T) {
	t.Parallel()

	const chat = `"to":{"kind":"phone","id":"5511999990002"}`
	for _, tc := range []struct {
		name    string
		kind    protocol.CommandType
		payload string
		run     func(*Session, *protocol.Command) error
	}{
		{"an edit", protocol.CommandMessageEdit,
			`{` + chat + `,"target_id":"3EB0TARGET","content":{"type":"text","body":"corrigido"}}`,
			func(s *Session, c *protocol.Command) error { _, err := s.edit(t.Context(), c); return err }},
		{"a revoke", protocol.CommandMessageRevoke,
			`{` + chat + `,"target_id":"3EB0TARGET"}`,
			func(s *Session, c *protocol.Command) error { _, err := s.revoke(t.Context(), c); return err }},
		{"a reaction", protocol.CommandMessageReact,
			`{` + chat + `,"target_id":"3EB0TARGET","emoji":"👍"}`,
			func(s *Session, c *protocol.Command) error { _, err := s.react(t.Context(), c); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			command := &protocol.Command{Type: tc.kind, ID: "cmd_000042", Payload: json.RawMessage(tc.payload)}
			session, sent := actingSession(t)
			if err := tc.run(session, command); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			first := sent.id
			if first == "" {
				t.Fatal("that went out with no stanza id at all")
			}

			// The same command again, on a session that never saw the first: this is the
			// redelivery, not a second call on warmed-up state.
			second, sentAgain := actingSession(t)
			if err := tc.run(second, command); err != nil {
				t.Fatalf("the redelivery of %s: %v", tc.name, err)
			}
			if sentAgain.id != first {
				t.Fatalf("the redelivery went out as %s and the first as %s, so the recipient "+
					"sees two", sentAgain.id, first)
			}
		})
	}
}

// A group is addressed by phone number or by LID, and a message key in it names its
// sender in whichever the group is on. The client cannot know which -- this connector
// publishes a sender as both, and Chatwoot hands back the LID whenever it has one
// (message_sender.rb, target_participant via Address.for_contact) -- so a key built from
// what it sends can name a participant no message in that group was ever sent by.
// WhatsApp accepts it, stamps it, and shows nothing.
func TestAParticipantIsPutInTheGroupsOwnNamespace(t *testing.T) {
	t.Parallel()

	const (
		group = "120363000000000000@g.us"
		pn    = "5511999990003@s.whatsapp.net"
		lid   = "182736451928374@lid"
	)
	for _, tc := range []struct {
		name, participant, want string
		mode                    waTypes.AddressingMode
		modeErr                 bool
		refused                 bool
	}{
		{name: "a LID where the group is on phone numbers", participant: lid, want: pn,
			mode: waTypes.AddressingModePN},
		{name: "a phone number where the group is on LIDs", participant: pn, want: lid,
			mode: waTypes.AddressingModeLID},
		// Already right, so nothing is looked up and nothing changes.
		{name: "a LID where the group is on LIDs", participant: lid, want: lid,
			mode: waTypes.AddressingModeLID},
		{name: "a phone number where the group is on phone numbers", participant: pn, want: pn,
			mode: waTypes.AddressingModePN},
		// The group could not be read. Left as it came, which is what this did before and
		// is no worse: refusing over a metadata query having a bad second would break a
		// reaction that mostly works.
		{name: "a group that could not be read", participant: lid, want: lid, modeErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _, _ := outboundSession(t)
			session.groupMode = func(context.Context, waTypes.JID) (waTypes.AddressingMode, error) {
				if tc.modeErr {
					return "", errors.New("no route to WhatsApp")
				}
				return tc.mode, nil
			}
			mustMap(t, session, pn, lid)

			got, err := session.asTheGroupAddresses(t.Context(), mustJID(t, group), mustJID(t, tc.participant))
			if err != nil {
				t.Fatalf("asTheGroupAddresses: %v", err)
			}
			if got.String() != tc.want {
				t.Fatalf("the key names %s, want %s", got, tc.want)
			}
		})
	}

	// A direct chat's key carries no participant at all, so there is nothing to place and
	// nothing to look up: a round trip here would be spent on every reaction in every
	// one-to-one chat.
	t.Run("a direct chat is left alone", func(t *testing.T) {
		t.Parallel()

		session, _, _ := outboundSession(t)
		session.groupMode = func(context.Context, waTypes.JID) (waTypes.AddressingMode, error) {
			t.Error("a direct chat asked WhatsApp how its group addresses members")
			return "", nil
		}
		direct := mustJID(t, "5511999990002@s.whatsapp.net")
		got, err := session.asTheGroupAddresses(t.Context(), direct, direct)
		if err != nil {
			t.Fatalf("asTheGroupAddresses: %v", err)
		}
		if got != direct {
			t.Fatalf("a direct chat's sender came back as %s", got)
		}
	})

	// And the failures once the group has been read, which are three different things and
	// have to answer as three. A client told its address is wrong stops sending it, so a
	// store having a bad second must never come back as that.
	t.Run("the mapping cannot be looked up", func(t *testing.T) {
		t.Parallel()

		for _, tc := range []struct {
			name        string
			participant string
			ctx         func(*testing.T) context.Context
			want        protocol.ErrorCode
		}{
			// Asked and answered: no mapping exists, so no key naming this participant
			// would resolve, and the next attempt says the same.
			{"nobody by that name is in the group", "999999999999999@lid",
				func(t *testing.T) context.Context { return t.Context() }, protocol.ErrorInvalidPayload},
			// The caller's own budget running out, which is not their address being wrong.
			{"the command ran out of time", "999999999999999@lid", func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				return ctx
			}, protocol.ErrorTimeout},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				session, _, _ := outboundSession(t)
				session.groupMode = func(context.Context, waTypes.JID) (waTypes.AddressingMode, error) {
					return waTypes.AddressingModePN, nil
				}
				_, err := session.asTheGroupAddresses(tc.ctx(t), mustJID(t, group), mustJID(t, tc.participant))
				assertCode(t, err, tc.want)
			})
		}
	})

	// An address that crosses the wire is `{kind, id}` and never a raw JID, and a refusal
	// carrying one is still crossing: a message goes into the reply, and from there into a
	// client's UI and whatever it ships its telemetry to. The server names in a JID say
	// nothing to whoever reads them and something about this connector's insides to
	// whoever does not.
	t.Run("the refusal names the participant the way the caller did", func(t *testing.T) {
		t.Parallel()

		session, _, _ := outboundSession(t)
		session.groupMode = func(context.Context, waTypes.JID) (waTypes.AddressingMode, error) {
			return waTypes.AddressingModePN, nil
		}
		_, err := session.asTheGroupAddresses(t.Context(),
			mustJID(t, group), mustJID(t, "999999999999999@lid"))
		assertCode(t, err, protocol.ErrorInvalidPayload)

		for _, raw := range []string{"@", "s.whatsapp.net", "@lid"} {
			if strings.Contains(err.Error(), raw) {
				t.Fatalf("the refusal carries %q, which is a JID and not an address: %v", raw, err)
			}
		}
		// And it still says which one, or the caller cannot act on it.
		if !strings.Contains(err.Error(), "999999999999999") {
			t.Fatalf("the refusal does not say which participant it is about: %v", err)
		}
	})

	// The store failing for its own reasons, which is neither of those, and whose words
	// stop here: a reply crosses into a client's UI, where a driver's own text is noise to
	// whoever reads it and a description of this deployment's insides to whoever does not.
	t.Run("the store could not answer", func(t *testing.T) {
		t.Parallel()

		const secret = "pq: relation \"whatsmeow_lid_map\" does not exist"
		session, _, _ := outboundSession(t)
		session.groupMode = func(context.Context, waTypes.JID) (waTypes.AddressingMode, error) {
			return waTypes.AddressingModePN, nil
		}
		session.current().Store.LIDs = brokenLIDs{err: errors.New(secret)}

		_, err := session.asTheGroupAddresses(t.Context(), mustJID(t, group), mustJID(t, lid))
		assertCode(t, err, protocol.ErrorInternal)
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("the driver's own words went to the client: %v", err)
		}
	})
}

// brokenLIDs is a mapping store that only ever fails, which is the one way the real one
// cannot be made to behave from a test.
type brokenLIDs struct{ err error }

func (b brokenLIDs) PutManyLIDMappings(context.Context, []waStore.LIDMapping) error { return b.err }
func (b brokenLIDs) PutLIDMapping(context.Context, waTypes.JID, waTypes.JID) error  { return b.err }
func (b brokenLIDs) GetPNForLID(context.Context, waTypes.JID) (waTypes.JID, error) {
	return waTypes.EmptyJID, b.err
}

func (b brokenLIDs) GetLIDForPN(context.Context, waTypes.JID) (waTypes.JID, error) {
	return waTypes.EmptyJID, b.err
}

func (b brokenLIDs) GetManyLIDsForPNs(
	context.Context, []waTypes.JID,
) (map[waTypes.JID]waTypes.JID, error) {
	return nil, b.err
}

func mustJID(t *testing.T, raw string) waTypes.JID {
	t.Helper()

	jid, err := waTypes.ParseJID(raw)
	if err != nil {
		t.Fatalf("ParseJID(%q): %v", raw, err)
	}
	return jid
}

func mustMap(t *testing.T, session *Session, pn, lid string) {
	t.Helper()

	if err := session.current().Store.LIDs.PutManyLIDMappings(t.Context(), []waStore.LIDMapping{
		{LID: mustJID(t, lid), PN: mustJID(t, pn)},
	}); err != nil {
		t.Fatalf("PutManyLIDMappings: %v", err)
	}
}

// And both commands that name a participant have to use it. Asserted on the key that
// reached the wire, because a command that resolves the participant and then builds the
// key from the unresolved one passes every test of the resolving and still names somebody
// the group has never sent a message from.
func TestBothCommandsPutTheParticipantOnTheWireResolved(t *testing.T) {
	t.Parallel()

	const (
		group = `"to":{"kind":"group","id":"120363000000000000"}`
		pn    = "5511999990003@s.whatsapp.net"
		lid   = "182736451928374@lid"
	)
	for _, tc := range []struct {
		name        string
		run         func(*Session, *protocol.Command) error
		kind        protocol.CommandType
		payload     string
		participant func(*waE2E.Message) string
	}{
		{"a reaction", func(s *Session, c *protocol.Command) error { _, err := s.react(t.Context(), c); return err },
			protocol.CommandMessageReact,
			`{` + group + `,"target_id":"3EB0TARGET","emoji":"👍",
				"target_participant":{"kind":"lid","id":"182736451928374"}}`,
			func(m *waE2E.Message) string { return m.GetReactionMessage().GetKey().GetParticipant() }},
		{"an admin revoke", func(s *Session, c *protocol.Command) error { _, err := s.revoke(t.Context(), c); return err },
			protocol.CommandMessageRevoke,
			`{` + group + `,"target_id":"3EB0TARGET",
				"participant":{"kind":"lid","id":"182736451928374"}}`,
			func(m *waE2E.Message) string { return m.GetProtocolMessage().GetKey().GetParticipant() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, sent := actingSession(t)
			// A group still on phone numbers, which is where the LID the client sends
			// names nobody.
			session.groupMode = func(context.Context, waTypes.JID) (waTypes.AddressingMode, error) {
				return waTypes.AddressingModePN, nil
			}
			mustMap(t, session, pn, lid)

			if err := tc.run(session, &protocol.Command{
				Type: tc.kind, ID: "cmd_000042", Payload: json.RawMessage(tc.payload),
			}); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if got := tc.participant(sent.message); got != pn {
				t.Fatalf("the key names %q, want %q", got, pn)
			}
		})
	}
}
