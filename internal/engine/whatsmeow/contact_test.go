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

func checkCommand(t *testing.T, payload string) *protocol.Command {
	t.Helper()
	return &protocol.Command{
		V: protocol.Version, ID: "c1", Type: protocol.CommandContactCheck,
		SID: "s1", Payload: json.RawMessage(payload),
	}
}

func TestAContactCheckRefusesAPayloadThatAsksNothing(t *testing.T) {
	t.Parallel()

	for _, refused := range []struct {
		name    string
		payload string
	}{
		{name: "no phones at all", payload: `{}`},
		{name: "an empty list", payload: `{"phones":[]}`},
		// Each of these reaches WhatsApp as a query it does not recognise, and comes back
		// as simply not registered -- the same answer a real number that is not on
		// WhatsApp gets. A caller told a customer has no WhatsApp stops writing to them,
		// so the two cannot be allowed to look alike.
		{name: "a number written with a plus", payload: `{"phones":["+5511999990002"]}`},
		{name: "a number written with punctuation", payload: `{"phones":["55 11 99999-0002"]}`},
		{name: "one good number and one that is not", payload: `{"phones":["5511999990002","not a number"]}`},
		{name: "an empty string", payload: `{"phones":[""]}`},
	} {
		t.Run(refused.name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			session.onWhatsApp = func(context.Context, *wm.Client, []string) ([]waTypes.IsOnWhatsAppResponse, error) {
				t.Error("a payload that names no number to ask about was asked about anyway")
				return nil, nil
			}

			_, err := session.Execute(t.Context(), checkCommand(t, refused.payload))
			assertCode(t, err, protocol.ErrorInvalidPayload)
		})
	}
}

// The ordering is the whole of this command's contract, and the reason it is a row per
// number asked rather than a row per answer: the query is one IQ, the server answers only
// what it recognises, and it does not promise the order it answers in. A caller lining the
// two lists up by position against a shorter or reordered answer reads one number's result
// under another's name -- and there is no way for it to notice.
func TestAContactCheckAnswersOneRowPerNumberAskedInTheOrderAsked(t *testing.T) {
	t.Parallel()

	session, container := newTestSession(t, "5511999990001")
	session.setConnected(true)
	_ = container

	asked := []string{"5511999990002", "5511999990003", "5511999990004"}
	session.onWhatsApp = func(_ context.Context, _ *wm.Client, phones []string) ([]waTypes.IsOnWhatsAppResponse, error) {
		if len(phones) != len(asked) {
			t.Errorf("whatsmeow was asked about %d numbers, want %d", len(phones), len(asked))
		}
		// Reordered, and the middle one left out: the shapes the server is free to
		// answer in and this command has to survive.
		return []waTypes.IsOnWhatsAppResponse{
			{Query: "5511999990004", IsIn: false, JID: waTypes.NewJID("5511999990004", waTypes.DefaultUserServer)},
			{Query: "5511999990002", IsIn: true, JID: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)},
		}, nil
	}

	result, err := session.Execute(t.Context(), checkCommand(t, `{"phones":["5511999990002","5511999990003","5511999990004"]}`))
	if err != nil {
		t.Fatalf("contact.check: %v", err)
	}
	var rows []checked
	if err := json.Unmarshal(result, &rows); err != nil {
		t.Fatalf("unmarshal the answer: %v", err)
	}
	if len(rows) != len(asked) {
		t.Fatalf("the answer has %d rows, want one per number asked (%d)", len(rows), len(asked))
	}
	for i, phone := range asked {
		if rows[i].Phone != phone {
			t.Errorf("row %d is about %q, want %q", i, rows[i].Phone, phone)
		}
	}
	if !rows[0].Exists {
		t.Error("the number WhatsApp said is registered came back as not registered")
	}
	if rows[0].Address == nil || rows[0].Address.ID != "5511999990002" {
		t.Errorf("the registered number came back with address %+v, want one naming it", rows[0].Address)
	}
	// The one the server never mentioned. Reported as not registered rather than left
	// out, because a row missing from the answer is a row the caller cannot ask about
	// again without guessing which one it was.
	if rows[1].Exists || rows[1].Address != nil {
		t.Errorf("a number the server did not answer for came back as %+v, want not registered", rows[1])
	}
	if rows[2].Exists {
		t.Error("the number WhatsApp said is not registered came back as registered")
	}
	if rows[2].Address != nil {
		t.Errorf("a number that is not on WhatsApp came back with an address (%+v)", rows[2].Address)
	}
}

// whatsmeow's IsOnWhatsApp takes numbers "in international format, including the `+`
// prefix", and the contract carries `digits` without one. Sending the contract's form
// straight through is not a formatting slip: WhatsApp does not recognise the query, and
// an unrecognised query comes back as a number that is not registered -- the same answer
// a real number with no WhatsApp gets. Every check would quietly report every customer as
// unreachable.
func TestAContactCheckAsksWhatsAppInTheFormItRequires(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.onWhatsApp = func(_ context.Context, _ *wm.Client, phones []string) ([]waTypes.IsOnWhatsAppResponse, error) {
		for _, phone := range phones {
			if !strings.HasPrefix(phone, "+") {
				t.Errorf("whatsmeow was asked about %q, want it in international format with a leading +", phone)
			}
		}
		// Echoed the way the server echoes it: the query as it was sent, `+` and all.
		return []waTypes.IsOnWhatsAppResponse{
			{Query: "+5511999990002", IsIn: true, JID: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)},
		}, nil
	}

	result, err := session.Execute(t.Context(), checkCommand(t, `{"phones":["5511999990002"]}`))
	if err != nil {
		t.Fatalf("contact.check: %v", err)
	}
	var rows []checked
	if err := json.Unmarshal(result, &rows); err != nil {
		t.Fatalf("unmarshal the answer: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("the answer has %d rows, want 1", len(rows))
	}
	// The row is matched back to what the caller wrote, so the `+` has to come off again.
	// Left on, the lookup misses and a registered number reads as not registered.
	if rows[0].Phone != "5511999990002" {
		t.Errorf("the row is about %q, want the number as the caller wrote it", rows[0].Phone)
	}
	if !rows[0].Exists {
		t.Error("a registered number came back as not registered, so the echoed query was not matched")
	}
}

// A number can be registered under a different one from the one asked about, and the
// Brazilian ninth digit is the everyday case: a mobile asked about with the 9 comes back
// under the form the account actually has. Sending to the other one fails silently, so a
// caller that keeps what it typed writes into nothing.
//
// `address` is not enough on its own, and that is what makes this the row's own problem
// rather than the caller's: this connector publishes a LID whenever it has one, and the
// consumer builds a phone JID out of that field -- a LID read as digits is a number
// nobody can be reached at, and it lands in a contact record. So the resolved number
// travels in `phone`, and correlation is the row's position.
func TestAContactCheckCarriesTheNumberWhatsAppResolved(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.onWhatsApp = func(context.Context, *wm.Client, []string) ([]waTypes.IsOnWhatsAppResponse, error) {
		return []waTypes.IsOnWhatsAppResponse{{
			Query: "+5511987654321",
			IsIn:  true,
			// Registered without the ninth digit, which is not the number that was asked.
			JID:         waTypes.NewJID("551187654321", waTypes.DefaultUserServer),
			PhoneNumber: waTypes.NewJID("551187654321", waTypes.DefaultUserServer),
		}}, nil
	}

	result, err := session.Execute(t.Context(), checkCommand(t, `{"phones":["5511987654321"]}`))
	if err != nil {
		t.Fatalf("contact.check: %v", err)
	}
	var rows []checked
	if err := json.Unmarshal(result, &rows); err != nil {
		t.Fatalf("unmarshal the answer: %v", err)
	}
	if rows[0].Phone != "551187654321" {
		t.Errorf("the row carries %q, want the number WhatsApp registered it under", rows[0].Phone)
	}
	if rows[0].Address == nil || rows[0].Address.ID != "551187654321" {
		t.Errorf("the address is %+v, want one naming the resolved number", rows[0].Address)
	}
}

// What a query failed with decides where an operator looks and what a client does next,
// so it is read off the error rather than defaulted. `wa_error` claims WhatsApp refused;
// `internal` says to come and read this connector's own logs. Getting that backwards on
// the IsOnWhatsApp path is not hypothetical -- it returns an error from writing the LID
// mappings it just learned, which is a local store and not WhatsApp at all.
func TestAContactQueryIsAnsweredWithWhicheverSideFailed(t *testing.T) {
	t.Parallel()

	for _, failure := range []struct {
		name string
		err  error
		want protocol.ErrorCode
	}{
		{name: "whatsapp refused the query", err: wm.ErrIQNotAuthorized, want: protocol.ErrorWaError},
		{name: "whatsapp is rate limiting", err: wm.ErrIQRateOverLimit, want: protocol.ErrorRateLimited},
		{name: "whatsapp asked for less", err: wm.ErrIQResourceLimit, want: protocol.ErrorRateLimited},
		{name: "the socket went mid-query", err: wm.ErrIQDisconnected, want: protocol.ErrorNotConnected},
		{name: "the session is not connected", err: wm.ErrNotConnected, want: protocol.ErrorNotConnected},
		{name: "the query timed out", err: wm.ErrIQTimedOut, want: protocol.ErrorTimeout},
		// The LID mapping write, which is this connector's own store.
		{name: "a local store failed", err: errors.New("failed to store LID mappings: disk"), want: protocol.ErrorInternal},
	} {
		t.Run(failure.name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			session.onWhatsApp = func(context.Context, *wm.Client, []string) ([]waTypes.IsOnWhatsAppResponse, error) {
				return nil, failure.err
			}

			_, err := session.Execute(t.Context(), checkCommand(t, `{"phones":["5511999990002"]}`))
			assertCode(t, err, failure.want)

			// The same classification on the other query, so the two cannot drift.
			session.profilePicture = func(
				context.Context, *wm.Client, waTypes.JID, *wm.GetProfilePictureParams,
			) (*waTypes.ProfilePictureInfo, error) {
				return nil, failure.err
			}
			_, err = session.Execute(t.Context(), pictureCommand(t, `{"party":{"kind":"phone","id":"5511999990002"}}`))
			assertCode(t, err, failure.want)
		})
	}
}

func pictureCommand(t *testing.T, payload string) *protocol.Command {
	t.Helper()
	return &protocol.Command{
		V: protocol.Version, ID: "c2", Type: protocol.CommandContactProfilePicture,
		SID: "s1", Payload: json.RawMessage(payload),
	}
}

// Both refusals mean the same thing to a client -- there is no picture it may show -- and
// the contract carries a URL or nothing, with no field to tell them apart in. Answering
// an error for either would put a failure in front of an agent where the truthful answer
// is that there is nothing to display.
func TestAProfilePictureThatCannotBeShownIsNullRatherThanAFailure(t *testing.T) {
	t.Parallel()

	for _, refusal := range []struct {
		name string
		err  error
	}{
		{name: "the party set no picture", err: wm.ErrProfilePictureNotSet},
		{name: "the party hid it from this account", err: wm.ErrProfilePictureUnauthorized},
	} {
		t.Run(refusal.name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			session.profilePicture = func(
				context.Context, *wm.Client, waTypes.JID, *wm.GetProfilePictureParams,
			) (*waTypes.ProfilePictureInfo, error) {
				return nil, refusal.err
			}

			result, err := session.Execute(t.Context(), pictureCommand(t, `{"party":{"kind":"phone","id":"5511999990002"}}`))
			if err != nil {
				t.Fatalf("contact.profile_picture: %v", err)
			}
			var answer struct {
				URL *string `json:"url"`
			}
			if err := json.Unmarshal(result, &answer); err != nil {
				t.Fatalf("unmarshal the answer: %v", err)
			}
			if answer.URL != nil {
				t.Errorf("a picture that cannot be shown came back as %q, want null", *answer.URL)
			}
		})
	}
}

func TestAProfilePictureCarriesTheURLAndAsksForThePreviewWhenToldTo(t *testing.T) {
	t.Parallel()

	for _, want := range []struct {
		name    string
		payload string
		preview bool
	}{
		{name: "the full picture by default", payload: `{"party":{"kind":"phone","id":"5511999990002"}}`},
		{name: "the preview when asked for", payload: `{"party":{"kind":"phone","id":"5511999990002"},"preview":true}`, preview: true},
	} {
		t.Run(want.name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			session.profilePicture = func(
				_ context.Context, _ *wm.Client, party waTypes.JID, params *wm.GetProfilePictureParams,
			) (*waTypes.ProfilePictureInfo, error) {
				if party.User != "5511999990002" {
					t.Errorf("the query names %q, want the party the caller asked about", party.User)
				}
				if params == nil || params.Preview != want.preview {
					t.Errorf("the query asks for preview=%v, want %v", params != nil && params.Preview, want.preview)
				}
				return &waTypes.ProfilePictureInfo{URL: "https://example.invalid/pp.jpg"}, nil
			}

			result, err := session.Execute(t.Context(), pictureCommand(t, want.payload))
			if err != nil {
				t.Fatalf("contact.profile_picture: %v", err)
			}
			var answer struct {
				URL *string `json:"url"`
			}
			if err := json.Unmarshal(result, &answer); err != nil {
				t.Fatalf("unmarshal the answer: %v", err)
			}
			if answer.URL == nil || *answer.URL != "https://example.invalid/pp.jpg" {
				t.Errorf("the answer carries %v, want the URL whatsmeow gave", answer.URL)
			}
		})
	}
}

// whatsmeow answers nil with no error when a picture has not changed since an id the
// caller passed. Nothing here passes one, so this is a shape the query should not come
// back in -- and reading `.URL` off it would take the session's executor down with a nil
// dereference, which costs every command queued behind it.
func TestAProfilePictureThatComesBackEmptyWithoutAReasonIsAFailureRatherThanAPanic(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.profilePicture = func(
		context.Context, *wm.Client, waTypes.JID, *wm.GetProfilePictureParams,
	) (*waTypes.ProfilePictureInfo, error) {
		return nil, nil
	}

	_, err := session.Execute(t.Context(), pictureCommand(t, `{"party":{"kind":"phone","id":"5511999990002"}}`))
	assertCode(t, err, protocol.ErrorInternal)
}

func TestAProfilePictureRefusesAPayloadThatNamesNobody(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)

	_, err := session.Execute(t.Context(), pictureCommand(t, `{}`))
	assertCode(t, err, protocol.ErrorInvalidPayload)
}

// A LID is not a number anybody can be reached at, and reading one as the resolved phone
// puts it in a contact record. whatsmeow answers with the LID in `JID` on an account
// addressing that way, so the fallback has to look at the server and not just take it.
func TestAContactCheckNeverReportsALIDAsTheResolvedNumber(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.onWhatsApp = func(context.Context, *wm.Client, []string) ([]waTypes.IsOnWhatsAppResponse, error) {
		return []waTypes.IsOnWhatsAppResponse{{
			Query: "+5511999990002", IsIn: true,
			JID: waTypes.NewJID("123456789012345", waTypes.HiddenUserServer),
		}}, nil
	}

	result, err := session.Execute(t.Context(), checkCommand(t, `{"phones":["5511999990002"]}`))
	if err != nil {
		t.Fatalf("contact.check: %v", err)
	}
	var rows []checked
	if err := json.Unmarshal(result, &rows); err != nil {
		t.Fatalf("unmarshal the answer: %v", err)
	}
	if rows[0].Phone == "123456789012345" {
		t.Error("a LID was reported as the number this contact can be reached at")
	}
	if rows[0].Phone != "5511999990002" {
		t.Errorf("the row carries %q, want the number asked about when WhatsApp resolved none", rows[0].Phone)
	}
}
