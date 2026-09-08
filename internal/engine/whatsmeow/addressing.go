package whatsmeow

import (
	"context"
	"sync"

	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// One conversation reached a client under two addresses depending on which path produced
// the event: a typing indicator under a LID, a receipt for the client's own send under
// another LID, and the send itself addressed by phone. Nothing here mapped one to the
// other, so a client had to reconcile three shapes for one person -- and a contact it
// knew only by number was not found from a LID-only party at all.
//
// The mapping exists, and locally: whatsmeow keeps `whatsmeow_lid_map` in the device
// store and answers in both directions off it. What was missing is a single place that
// asks, used by every path that publishes an address.
//
// LID is what the addresses agree on, and that is not a coin toss. The client's own
// contact key is `lid.presence || phone` and its consolidation merges a phone-keyed
// contact into its LID, so canonicalising the other way would fight the machinery on the
// other side; WhatsApp is moving the same direction, and a number is the half that can go
// missing on a privacy setting.

// alias resolves one namespace to the other, and remembers what it learns.
//
// Only what it found is remembered. A LID and a phone that name one person name each
// other for good, so a hit needs no expiry and no invalidation -- which is what lets this
// be a map and not a cache with a policy. A miss is the opposite: the mapping is learned
// later, from a message or an app-state sync, so remembering its absence would pin the
// wrong answer for as long as the session ran. A pair nobody has learned yet is looked up
// again each time, which is the price of never being stale.
type alias struct {
	mu sync.RWMutex
	// seen is what has been learned, and generation is which account learned it. A
	// lookup that started before the account changed must not write its answer into the
	// map that replaced it: the store read is not under the lock, so a rebuild can land
	// in the middle of one.
	seen       map[string]waTypes.JID
	generation uint64
}

func newAlias() *alias { return &alias{seen: make(map[string]waTypes.JID)} }

// of answers the other namespace's JID for one, and whether there is one to have.
//
// A store that will not answer is logged and left. The address still goes out with the
// half the event carried, which is what happened before this existed at all, and the next
// event for the same party asks again -- on the event path, losing the mapping is worth
// less than losing the event. A caller that is asking for the mapping itself wants the
// difference, and lookup is where it is kept.
func (a *alias) of(ctx context.Context, s *Session, jid waTypes.JID) (waTypes.JID, bool) {
	alt, found, err := a.lookup(ctx, s, jid)
	if err != nil {
		s.log.Debug().Err(err).Str("jid", jid.String()).
			Msg("could not read the other namespace for a party")
		return waTypes.EmptyJID, false
	}
	return alt, found
}

// lookup is of, with the failure kept apart from the absence.
//
// The two are not the same answer and a command whose whole result is the mapping cannot
// treat them as one: "nobody has learned this pairing yet" is a result, and "the store did
// not answer" is a refusal the caller can retry.
func (a *alias) lookup(ctx context.Context, s *Session, jid waTypes.JID) (waTypes.JID, bool, error) {
	if !pairable(jid) {
		return waTypes.EmptyJID, false, nil
	}
	key := jid.ToNonAD().String()

	a.mu.RLock()
	known, remembered := a.seen[key]
	learning := a.generation
	a.mu.RUnlock()
	if remembered {
		return known, true, nil
	}

	client := s.current()
	if client == nil || client.Store == nil {
		return waTypes.EmptyJID, false, nil
	}
	alt, err := client.Store.GetAltJID(ctx, jid)
	switch {
	case err != nil:
		return waTypes.EmptyJID, false, err
	case alt.IsEmpty():
		return waTypes.EmptyJID, false, nil
	}

	a.remember(key, alt, learning)
	return alt, true, nil
}

// learning is which account's mapping is being learned right now.
func (a *alias) learning() uint64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.generation
}

// remember keeps what a lookup found, unless the account changed while it was being read.
//
// The store read is not under the lock -- it is a database round trip, and holding the map
// across one would serialise every path that names a party -- so a rebuild can land in the
// middle of one. The answer still goes back to the caller that asked for it, because the
// command was accepted under the account that could see it; what must not happen is the
// previous account's mapping being written back into a map that was emptied precisely to
// lose it.
func (a *alias) remember(key string, alt waTypes.JID, learning uint64) {
	a.mu.Lock()
	if a.generation == learning {
		a.seen[key] = alt
	}
	a.mu.Unlock()
}

// forget empties the mapping this session has learned.
//
// The cache mirrors a table in the device store, so it lives as long as that device and
// not as long as the session: a logout deletes the device, and the account paired after it
// may be a different one. A pairing between a LID and a number is what one account was
// shown, not a fact about the world, so answering the next account out of it would hand
// over a number nobody gave it.
func (a *alias) forget() {
	a.mu.Lock()
	a.seen = make(map[string]waTypes.JID)
	a.generation++
	a.mu.Unlock()
}

// pairable reports whether a JID is one of the two namespaces that name a person. A
// group, a newsletter and a broadcast list have no counterpart to look up.
func pairable(jid waTypes.JID) bool {
	switch jid.Server {
	case waTypes.DefaultUserServer, waTypes.LegacyUserServer, waTypes.HostedServer,
		waTypes.HiddenUserServer, waTypes.HostedLIDServer:
		return !jid.IsBot() && jid.User != ""
	default:
		return false
	}
}

// looking is the budget one address resolution gets. The mapping is a local read on the
// device store, which shares its one connection with everything the session writes, so a
// handler that waits on it indefinitely waits on whatever else is mid-write.
func (s *Session) looking() (context.Context, context.CancelFunc) {
	return context.WithTimeout(s.ctx, s.storeLimit)
}

// party names somebody by both of the addresses WhatsApp knows them by, filling in from
// the mapping whatever the event did not carry.
func (s *Session) party(ctx context.Context, jids ...waTypes.JID) protocol.Party {
	var named protocol.Party
	naming(&named, jids...)
	if named.Phone != "" && named.LID != "" {
		return named
	}
	for _, jid := range jids {
		alt, ok := s.aliases.of(ctx, s, jid)
		if !ok {
			continue
		}
		naming(&named, alt)
		if named.Phone != "" && named.LID != "" {
			break
		}
	}
	return named
}

// address is the one address a conversation is published under, whichever namespace the
// event named it in.
//
// A LID when there is one, so every path agrees and the client's own key -- which is its
// LID when it has one -- names the same conversation whether it arrived through a
// message, a receipt or a typing indicator.
func (s *Session) address(ctx context.Context, jids ...waTypes.JID) (protocol.Address, bool) {
	// What the event carried first, and the mapping only for what it did not. An event
	// that names both namespaces has already answered the question, and asking the store
	// anyway would be a read per event for an answer in hand.
	var named protocol.Address
	for _, jid := range jids {
		switch address, ok := addressOf(jid); {
		case !ok:
		case address.Kind == protocol.AddressLID:
			return address, true
		case named.ID == "":
			named = address
		}
	}
	if named.ID == "" {
		return protocol.Address{}, false
	}
	if named.Kind != protocol.AddressPhone {
		// A group, a newsletter or a broadcast list. There is no other namespace naming
		// it, so there is nothing to resolve towards.
		return named, true
	}
	for _, jid := range jids {
		alt, found := s.aliases.of(ctx, s, jid)
		if !found {
			continue
		}
		if canonical, addressable := addressOf(alt); addressable && canonical.Kind == protocol.AddressLID {
			return canonical, true
		}
	}
	return named, true
}
