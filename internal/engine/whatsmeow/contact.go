package whatsmeow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	wm "go.mau.fi/whatsmeow"
	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// checkRequest is `contact.check`: which of these numbers are on WhatsApp.
type checkRequest struct {
	Phones []string `json:"phones"`
}

// checked is one row of the answer, `{phone, exists, address}`.
//
// On a registered row `phone` is the number WhatsApp resolved, which is what a caller
// has to write to and store; on any other row it is the number that was asked, because
// nothing was resolved. Lining a row up against what was asked is the row's position and
// not its contents -- there is one row per number asked, in order -- which leaves `phone`
// free to answer the question its name asks.
type checked struct {
	Phone   string            `json:"phone"`
	Exists  bool              `json:"exists"`
	Address *protocol.Address `json:"address"`
}

// checkContacts answers which of a list of numbers have WhatsApp.
//
// One row per number asked about, in the order they were asked, whether or not WhatsApp
// mentioned it. The query is a single IQ and the server answers only what it recognises,
// so a caller matching by position on a shorter list would read one number's answer under
// another's name -- and a caller that sent one number and got an empty array back cannot
// tell "not on WhatsApp" from "the query went nowhere".
func (s *Session) checkContacts(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	var req checkRequest
	if err := json.Unmarshal(command.Payload, &req); err != nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a contact check has to name the numbers it is asking about")
	}
	if len(req.Phones) == 0 {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a contact check with no numbers in it asks nothing")
	}
	for _, phone := range req.Phones {
		if !onlyDigits(phone) {
			return nil, protocol.NewError(protocol.ErrorInvalidPayload,
				fmt.Sprintf("%q is not a phone number: a contact check names each one as digits, with no punctuation and no plus", phone))
		}
	}
	if err := s.readyToSend(); err != nil {
		return nil, err
	}

	// Asked with the `+` whatsmeow's own documentation requires, and the contract carries
	// numbers without one: `digits` is bare, deliberately, because a `+` is a way of
	// writing a number rather than part of it. The translation belongs here, at the
	// boundary between the wire and the library, and it is not cosmetic -- a query in the
	// wrong form is one WhatsApp does not recognise, and an unrecognised query comes back
	// as a number that is not registered, which is the answer that costs a customer.
	asked := make([]string, len(req.Phones))
	for i, phone := range req.Phones {
		asked[i] = "+" + phone
	}

	found, err := s.onWhatsApp(ctx, s.current(), asked)
	if err != nil {
		return nil, contactFailure(err, "contact check")
	}

	// Keyed by the query WhatsApp echoes rather than by the JID it resolved to, because
	// that is the only field that ties a row back to what was asked. Two callers'
	// spellings of one number would resolve to the same JID and have to share a row.
	//
	// The `+` comes back off, because the server echoes the query as it was sent and the
	// rows are matched against what the caller wrote.
	byQuery := make(map[string]waTypes.IsOnWhatsAppResponse, len(found))
	for _, one := range found {
		byQuery[strings.TrimPrefix(one.Query, "+")] = one
	}

	rows := make([]checked, 0, len(req.Phones))
	for _, phone := range req.Phones {
		row := checked{Phone: phone}
		if one, answered := byQuery[phone]; answered && one.IsIn {
			row.Exists = true
			// The number WhatsApp registered it under, which is not always the one that
			// was asked: a Brazilian mobile asked about with the ninth digit comes back
			// under the form the account actually has. A caller that keeps the number it
			// typed writes to a number that silently goes nowhere, so a registered row
			// carries the resolved one and correlation is the row's position, which this
			// loop guarantees by building one row per number asked, in order.
			if resolved := resolvedNumber(&one); resolved != "" {
				row.Phone = resolved
			}
			// The same address the rest of this connector would publish a conversation
			// with this person under -- a LID when there is one -- so a caller can use
			// what comes back as an address and not have to resolve it again.
			//
			// This is also where the number WhatsApp resolved to reaches the caller, and
			// the two are not always the same one: a Brazilian number asked about with
			// the ninth digit comes back registered under the form WhatsApp knows it by.
			// `phone` above echoes the query, because it is what lines a row up against
			// what was asked; the resolved identity travels here.
			if address, named := s.address(ctx, one.JID, one.PhoneNumber); named {
				row.Address = &address
			} else {
				// Registered, and nothing this connector can name it by. A caller with no
				// address falls back to the number it asked about, which is the one form
				// known not to be what WhatsApp resolved -- so say so where an operator
				// can see it rather than let it pass as an ordinary row.
				s.log.Warn().Str("jid", one.JID.String()).
					Msg("a number is on WhatsApp and could not be named as an address")
			}
		}
		rows = append(rows, row)
	}
	return json.Marshal(rows)
}

// resolvedNumber is the phone number WhatsApp answered with, or nothing when it did not
// answer with one.
//
// `PhoneNumber` first, because that is the field whose whole job is to carry it. The
// canonical JID is the fallback and only when it is a phone: on an account addressing by
// LID it holds the LID instead, and a LID read as a number is a number nobody can be
// reached at -- worse than the one that was asked, which at least came from a person.
func resolvedNumber(one *waTypes.IsOnWhatsAppResponse) string {
	if one.PhoneNumber.Server == waTypes.DefaultUserServer && one.PhoneNumber.User != "" {
		return one.PhoneNumber.User
	}
	if one.JID.Server == waTypes.DefaultUserServer {
		return one.JID.User
	}
	return ""
}

// contactFailure maps what a contact query can fail with onto the contract's codes.
//
// The shared half is `commandFailure`. What is left is asked rather than assumed: an IQ
// WhatsApp answered with an error is an `*IQError`, so `wa_error` is a fact about the
// reply and not a guess about which side broke. Everything else is this connector's --
// `IsOnWhatsApp` returns an error from writing the LID mappings it learned, which is a
// local store having a bad second and nothing to do with WhatsApp -- and saying
// `wa_error` there sends an operator to the wrong logs and a client into retrying against
// a server that never refused anything.
//
// Two of the IQ codes are their own answer. A rate limit is worth waiting out rather than
// retrying at once, and the contract has a code that says exactly that; a disconnect
// mid-query is the socket rather than a refusal, and reads as `not_connected` like every
// other command's would.
func contactFailure(err error, subject string) error {
	if named, coded := commandFailure(err, subject); named {
		return coded
	}
	if errors.Is(err, wm.ErrIQDisconnected) {
		return protocol.NewError(protocol.ErrorNotConnected,
			"the connection went while the "+subject+" was in flight")
	}
	var refused *wm.IQError
	if !errors.As(err, &refused) {
		// Not an answer from WhatsApp at all. The text is not passed on: it describes
		// this deployment's insides to whoever reads a reply, which is the same reason
		// every other path here answers out of a closed vocabulary.
		return protocol.NewError(protocol.ErrorInternal, "the "+subject+" could not be carried out")
	}
	switch refused.Code {
	case 419, 429:
		return protocol.NewError(protocol.ErrorRateLimited,
			"WhatsApp is rate limiting this account's "+subject+"s")
	}
	return protocol.NewError(protocol.ErrorWaError, "WhatsApp refused the "+subject)
}

// onlyDigits is what the contract's `digits` means: a bare number, no `+`, no spaces and
// no punctuation. Checked here rather than left to WhatsApp because a number it does not
// recognise comes back as simply not registered, which is the same answer a typo gets --
// and a caller told a customer is not on WhatsApp stops writing to them.
func onlyDigits(phone string) bool {
	if phone == "" {
		return false
	}
	for _, digit := range phone {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	return true
}

// pictureRequest is `contact.profile_picture`.
type pictureRequest struct {
	Party   protocol.Address `json:"party"`
	Preview bool             `json:"preview"`
}

// contactPicture answers where a party's profile picture can be fetched from, or that
// there is none to fetch.
//
// Both of whatsmeow's refusals become `null` rather than an error, and they are not the
// same thing: one is a party with no picture set, the other a party who has hidden theirs
// from this account. The contract carries a URL or nothing, so there is no field to tell
// them apart in, and inventing an error for the second would have a client show a failure
// where the truthful answer is that there is no picture it may show. Which of the two it
// was is logged, because it is the difference between a contact who never set one and a
// privacy setting an operator may be asked about.
func (s *Session) contactPicture(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	var req pictureRequest
	if err := json.Unmarshal(command.Payload, &req); err != nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a profile picture request has to name whose picture it wants")
	}
	party, err := jidOf(req.Party)
	if err != nil {
		return nil, err
	}
	if err := s.readyToSend(); err != nil {
		return nil, err
	}

	picture, err := s.profilePicture(ctx, s.current(), party, &wm.GetProfilePictureParams{Preview: req.Preview})
	switch {
	case errors.Is(err, wm.ErrProfilePictureNotSet):
		s.log.Debug().Str("kind", string(req.Party.Kind)).Msg("that party has no profile picture set")
		return json.Marshal(map[string]any{"url": nil})
	case errors.Is(err, wm.ErrProfilePictureUnauthorized):
		s.log.Debug().Str("kind", string(req.Party.Kind)).Msg("that party has hidden their profile picture from this account")
		return json.Marshal(map[string]any{"url": nil})
	case err != nil:
		return nil, contactFailure(err, "profile picture query")
	case picture == nil:
		// whatsmeow answers nil without an error when the picture has not changed since
		// an id the caller passed. Nothing here passes one, so this is a shape the query
		// should not come back in -- and a nil dereference below would take the session's
		// executor with it.
		return nil, protocol.NewError(protocol.ErrorInternal,
			"the profile picture query came back empty without saying why")
	}
	return json.Marshal(map[string]any{"url": picture.URL})
}

// targetRequest is the payload `contact.resolve` names a person with.
type targetRequest struct {
	Party protocol.Address `json:"party"`
}

// resolveContact carries out `contact.resolve`: both of WhatsApp's names for one person,
// plus whatever display names this session has already learned for them.
//
// A phone number and a LID are the same person under two namespaces, and which one an
// event carries depends on the path it arrived on. A client that stored a contact by
// number and then meets a LID-only party has no way to see they are the same, and this is
// the command that answers it.
//
// Local by construction: whatsmeow keeps the mapping in the device store, so this costs a
// read rather than a round trip. `contact.info` is the one that asks WhatsApp, and the
// division is the whole reason the contract has two commands with one payload.
//
// Paired is all it asks for. Every other command here wants a connection because it puts
// something on the wire; this one reads a table that is there whether the socket is up or
// not, and refusing a client reconciling its contacts during a reconnect would be a
// refusal this connector does not need to make.
func (s *Session) resolveContact(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	var req targetRequest
	if err := json.Unmarshal(command.Payload, &req); err != nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a resolve has to name the party to resolve")
	}
	jid, err := personOf(req.Party, "resolve")
	if err != nil {
		return nil, err
	}
	phone, lid := s.identity()
	if phone == "" || s.isRevoked() {
		// Revoked is an account WhatsApp has taken away, on credentials this session is
		// still holding: the identity is copied out at pairing, and the cleanup that
		// clears it is a store round trip behind the unlink. Every other command here is
		// kept out by needing a connection; this one asks for no connection on purpose, so
		// it has to ask the question the connection was answering.
		return nil, protocol.NewError(protocol.ErrorNotPaired,
			"this session has no WhatsApp account to resolve against")
	}

	// Bounded, and not by the caller's deadline alone. A command may carry none at all,
	// and this runs on the session's executor: a database call that wedges with the
	// session's own context behind it holds every later command for this session for as
	// long as it lasts. The same bound every event handler reads the store under.
	reading, done := context.WithTimeout(ctx, s.storeLimit)
	defer done()

	// The mapping read directly rather than through `party`, because a command whose
	// whole answer is the mapping has to tell a store that did not answer from a pairing
	// nobody has learned. `party` reports both as an absence, which is right where losing
	// the mapping costs less than losing the event, and wrong here: a client told the
	// other namespace does not exist stops asking, and the one told to retry retries.
	var named protocol.Party
	naming(&named, jid)
	if named.Phone == phone || (lid != "" && named.LID == lid) {
		// The account asking about itself. Both of its names were copied out of the device
		// at pairing, and the mapping table is a separate write that whatsmeow logs rather
		// than fails on -- so the account can be the one party the table cannot answer
		// for, which would be an absurd thing for this command to be unable to resolve.
		//
		// The LID half is as good as what the session has been told, which is the device
		// it was built on plus what the connection brought: a resumed device learns its
		// LID there rather than through a `PairSuccess`. A session that has never
		// connected can still answer with the number alone.
		if lid != "" {
			named.LID = lid
		}
		named.Phone = phone
		// The table first, and the session for what it does not hold. Its own names are
		// not in there to begin with -- the table is the people this account has met, and
		// it is not one of them -- so the session, which took them off the device where an
		// ordering exists, is what usually answers.
		//
		// The exception is why the order is this way round: an account that renames its
		// business has the new name written to the table and not to the device record, so
		// after a restart the device is the stale copy of the two.
		s.nameFromStore(reading, &named)
		pushName, verifiedName := s.names()
		if named.PushName == "" {
			named.PushName = pushName
		}
		if named.VerifiedName == "" {
			named.VerifiedName = verifiedName
		}
		return json.Marshal(named)
	}
	alt, found, err := s.aliases.lookup(reading, s, jid)
	if err != nil {
		return nil, s.storeFailure(err, "the address mapping")
	}
	if found {
		// `whatsmeow_lid_map` is keyed by `(lid, pn)` and by nothing else: every account
		// on this deployment writes into one table, so a mapping in it may have been
		// learned by a different one. Enriching an event with it is one thing -- the event
		// is about somebody this account is already talking to -- and answering a question
		// about an arbitrary address is another, which is a client asking this connector
		// for a number another operator's account was shown.
		//
		// The contact table is keyed by `our_jid`, so it is the one thing here that
		// answers "has this account met them". A party it has not is answered with the
		// half the caller already had. Issue #137 is the mapping table itself.
		met, err := s.hasMet(reading, jid, alt)
		switch {
		case err != nil:
			return nil, s.storeFailure(err, "the contact record")
		case met:
			naming(&named, alt)
		default:
			s.log.Debug().Str("kind", string(req.Party.Kind)).
				Msg("withholding a mapping this account has no record of having learned")
		}
	}
	if named.Phone == "" && named.LID == "" {
		// jidOf built this JID out of an address kind personOf just accepted, so the
		// party cannot come back empty unless the addressing layer stopped naming one of
		// the two namespaces it is built on.
		return nil, protocol.NewError(protocol.ErrorInternal, "the party resolved to no address at all")
	}
	s.nameFromStore(reading, &named)
	return json.Marshal(named)
}

// personOf is jidOf for the commands that act on somebody rather than on a conversation.
//
// A group, a channel or the status feed parses as an address and is not a person: asked
// to resolve one, this would hand back a party naming a group as though it were somebody,
// and a client would file a conversation under a contact that does not exist.
func personOf(address protocol.Address, subject string) (waTypes.JID, error) {
	switch address.Kind {
	case protocol.AddressPhone, protocol.AddressLID:
		if !onlyDigits(address.ID) {
			// The contract lets an `address` carry any non-empty id -- a group's is not a
			// number -- while a `party` is digits in both namespaces. A person whose id is
			// not one would answer with a party the contract refuses, and a client
			// validating what it reads drops the reply rather than the id inside it.
			return waTypes.EmptyJID, protocol.NewError(protocol.ErrorInvalidPayload,
				"a person is named by digits, and this address carries something else")
		}
		jid, err := jidOf(address)
		if err != nil {
			return waTypes.EmptyJID, err
		}
		if jid.IsBot() {
			// Meta's own assistants answer on the ordinary phone server under a reserved
			// range. The addressing layer refuses to name one as a party -- a client
			// handed the number would open a conversation with something that is not a
			// person -- so this is the caller's payload rather than a failure here.
			return waTypes.EmptyJID, protocol.NewError(protocol.ErrorInvalidPayload,
				"that number belongs to a bot, and a bot is not somebody this contract names")
		}
		return jid, nil
	default:
		return waTypes.EmptyJID, protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("only a person can be the subject of a %s, and %q is not one", subject, address.Kind))
	}
}

// nameFromStore fills in the display names the device already holds for a party.
//
// Both namespaces are asked, in the order the party names them, because a push name is
// learned from whichever address the message carrying it arrived under -- so a party
// resolved from a LID may have its name filed under the phone, and the other way round.
//
// Names are an annotation. A store that will not answer leaves the party as it is rather
// than failing the command: the addresses are what the caller asked for, and they are
// already in hand.
func (s *Session) nameFromStore(ctx context.Context, named *protocol.Party) {
	client := s.current()
	if client == nil || client.Store == nil || client.Store.Contacts == nil {
		return
	}
	for _, address := range []protocol.Address{
		{Kind: protocol.AddressPhone, ID: named.Phone},
		{Kind: protocol.AddressLID, ID: named.LID},
	} {
		if named.PushName != "" && named.VerifiedName != "" {
			return
		}
		if address.ID == "" {
			continue
		}
		jid, err := jidOf(address)
		if err != nil {
			continue
		}
		contact, err := client.Store.Contacts.GetContact(ctx, jid)
		if err != nil {
			s.log.Debug().Err(err).Str("kind", string(address.Kind)).
				Msg("could not read the stored names for a party")
			continue
		}
		// Field by field, and a row that was found is not the end of the search: the two
		// namespaces are written by different paths -- an app-state contact sync files one,
		// a message's push name the other -- so the row for the address that was asked
		// about can exist and hold neither name.
		if named.PushName == "" {
			named.PushName = contact.PushName
		}
		if named.VerifiedName == "" {
			named.VerifiedName = contact.BusinessName
		}
	}
}

// hasMet reports whether this account has a record of either of a party's two addresses.
//
// The contact table is keyed by `our_jid`, which makes it the only per-account record in
// the device store: a row exists once a message, a group listing or an address-book sync
// has put one there. `whatsmeow_lid_map` has no such key -- it is `(lid, pn)` and nothing
// else -- so it is shared by every account on the deployment, and a mapping read out of it
// may be one another operator's account was shown.
//
// Either address counts, because the row can be filed under the namespace the caller did
// not ask about, which is the same asymmetry the name lookup handles.
// A read that fails is not a party this account has not met: withholding on it would
// answer the same one-sided party an unknown mapping answers, and a client told the other
// namespace is unknown stops asking. The error goes back for the same reason the mapping's
// does.
func (s *Session) hasMet(ctx context.Context, addresses ...waTypes.JID) (bool, error) {
	client := s.current()
	if client == nil || client.Store == nil || client.Store.Contacts == nil {
		return false, nil
	}
	for _, jid := range addresses {
		if jid.IsEmpty() {
			continue
		}
		contact, err := client.Store.Contacts.GetContact(ctx, jid)
		if err != nil {
			return false, err
		}
		if contact.Found {
			return true, nil
		}
	}
	return false, nil
}

// storeFailure is what a device store that would not answer comes back as. A cancelled or
// expired context is the caller's deadline rather than a fault here, and the two send a
// client down different roads: one waits and asks again, the other is a line in this
// connector's log.
func (s *Session) storeFailure(err error, subject string) error {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return protocol.NewError(protocol.ErrorTimeout, subject+" did not answer in time")
	}
	// Logged before it is degraded. `internal` is documented as meaning this connector's
	// own logs are where to look, and the wire carries a closed vocabulary rather than a
	// database's text -- so if the error does not reach the log here, it reaches nothing.
	s.log.Error().Err(err).Msg("the device store refused a read " + subject + " needed")
	return protocol.NewError(protocol.ErrorInternal, subject+" could not be read")
}
