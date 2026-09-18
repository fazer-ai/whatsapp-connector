package whatsmeow

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	wm "go.mau.fi/whatsmeow"
	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/store"
)

// A creation waiting on WhatsApp to say which group it made is refused with a word of its
// own, and not with the one the contract keeps for this connector having a bug.
//
// The refusal itself is #213 and is not what this asserts: two requests for a group of the
// same name, both open, make a group that is evidence for either and proof for neither, so
// answering one of them with it would hand that request the other's conversation. What a
// client could not tell apart was the answer. `internal` reads as "this connector is
// broken", and the truth is that the outcome exists, the connector will know which one it
// is as soon as WhatsApp's notification lands, and asking again in a moment settles it. A
// client that could tell the two apart retries this one and pages a human for the other.
//
// Asserted against the string on the wire and not against the constant, because a test
// that compares the constant to itself passes on any spelling, and this value belongs to
// the client rather than to us. It also proves the catalogue has it without saying so:
// `protocol.NewError` degrades a code it does not know to `internal`, so a value missing
// from `AllErrorCodes` arrives here as `internal` and fails.
func TestACreationWaitingOnWhatsAppsNameIsRefusedWithItsOwnWord(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	impatient(session)
	self := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)

	began := time.Now().Add(-time.Minute)
	if _, _, err := session.store.BeginGroupCreate(
		t.Context(), "idem:once", "WACOPEN", "Obras", began); err != nil {
		t.Fatalf("begin the attempt that crashed: %v", err)
	}

	session.createTheGroup = func(context.Context, *wm.Client, keyedCreate) (*waTypes.GroupInfo, error) {
		return aMadeGroup("120363099999999999", "Obras", self), nil
	}

	_, err := session.Execute(t.Context(),
		namedCreate("c1", "once", `{"subject":"Obras","participants":[]}`))
	if err == nil {
		t.Fatal("the redelivery answered a group over a creation WhatsApp had said nothing about")
	}
	if got := string(codeOf(err)); got != "not_settled" {
		t.Fatalf("a creation whose group is not named yet answered %q, want \"not_settled\". "+
			"%q tells a client this connector has a bug, so the operator is paged for a case that "+
			"settles itself in seconds and the retry that would collect the group is never made",
			got, got)
	}
}

// And the word does not spread to the failure next to it. A creation that could not even
// write down its intent is this connector failing, which is what `internal` is for, and a
// client retrying that one gets the same failure for as long as the store is down.
//
// Green on the base as well, on purpose: a change whose only red was the new word would
// have fenced nothing about the answer it was carved out of.
func TestACreationThatCouldNotRecordItsIntentIsStillInternal(t *testing.T) {
	t.Parallel()

	session, container := newTestSession(t, "5511999990001")
	session.setConnected(true)

	session.createTheGroup = func(context.Context, *wm.Client, keyedCreate) (*waTypes.GroupInfo, error) {
		return aMadeGroup("120363041234567890", "Obras",
			waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)), nil
	}
	if err := container.Close(); err != nil {
		t.Fatalf("Close the store: %v", err)
	}

	_, err := session.Execute(t.Context(),
		namedCreate("c1", "once", `{"subject":"Obras","participants":[]}`))
	if err == nil {
		t.Fatal("a group was made with nothing written down to stop a redelivery making another")
	}
	if got := string(codeOf(err)); got == "not_settled" {
		t.Fatalf("a creation this connector could not record answered %q: that word promises the "+
			"outcome settles itself, and this one settles when somebody fixes the store", got)
	}
}

// The two failures that share the wait keep their own words, and the table is the point:
// the three cells differ in exactly one thing, which is what makes the answer attributable
// to the wait's outcome rather than to how each case was set up.
//
// Without them, "the wait answers `not_settled`" is a claim nothing distinguishes from "the
// wait answers `not_settled` whatever went wrong in it", and the second is a code that
// tells a client to retry against a store that is down until somebody notices.
func TestTheOtherWaysTheSameWaitEndsKeepTheirOwnWords(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		// arrange is the one thing that differs: everything else below is shared.
		arrange func(t *testing.T, session *Session, container *store.Container) context.Context
		want    protocol.ErrorCode
		why     string
	}{
		{
			name: "WhatsApp has not named the group yet",
			arrange: func(t *testing.T, session *Session, _ *store.Container) context.Context {
				impatient(session)
				return t.Context()
			},
			want: protocol.ErrorNotSettled,
			why:  "the outcome exists and asking again in a moment settles it",
		},
		{
			name: "the store stopped answering inside the wait",
			arrange: func(t *testing.T, session *Session, container *store.Container) context.Context {
				session.createWait = time.Minute
				session.lookingForNotice = func() { _ = container.Close() }
				return t.Context()
			},
			want: protocol.ErrorInternal,
			why:  "this connector could not carry the command out, and it settles when somebody fixes the store",
		},
		{
			name: "the caller ran out of its own time inside the wait",
			arrange: func(t *testing.T, session *Session, _ *store.Container) context.Context {
				session.createWait = time.Minute
				bound, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
				t.Cleanup(cancel)
				return bound
			},
			want: protocol.ErrorTimeout,
			why:  "it is the caller's clock that ran out and not this wait",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, container := newTestSession(t, "5511999990001")
			session.setConnected(true)
			self := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)
			if _, _, err := session.store.BeginGroupCreate(
				t.Context(), "idem:once", "WACOPEN", "Obras", time.Now().Add(-time.Minute)); err != nil {
				t.Fatalf("begin the attempt that crashed: %v", err)
			}
			session.createTheGroup = func(context.Context, *wm.Client, keyedCreate) (*waTypes.GroupInfo, error) {
				t.Error("WhatsApp was asked for a group while the earlier attempt was unresolved")
				return aMadeGroup("120363099999999999", "Obras", self), nil
			}
			ctx := tc.arrange(t, session, container)

			_, err := session.Execute(ctx, namedCreate("c1", "once", `{"subject":"Obras","participants":[]}`))
			if err == nil {
				t.Fatal("the redelivery answered a group over a creation nothing had settled")
			}
			if got := codeOf(err); got != tc.want {
				t.Fatalf("answered %q, want %q: %s", got, tc.want, tc.why)
			}
		})
	}
}

// The value has exactly one producer, and it is the one the round is about.
//
// A code is a promise about when it is sent, and this one promises an outcome that settles
// itself in seconds. A second site that answered it for something else -- a store that is
// down, a lease that moved -- would have clients retrying against a case that never
// settles, and nothing in the repository would say so: the producer fence only asks
// whether a code is named somewhere outside `internal/protocol`, never how many times or
// where.
//
// The directory is parsed, not a file chosen by hand, and not searched as text: a grep
// matches the word in a comment and in a test, and this is a claim about production code.
func TestNotSettledHasOneProducerAndItIsTheGroupCreation(t *testing.T) {
	t.Parallel()

	sites := selectionsOf(t, "ErrorNotSettled", filepath.Join("..", "..", ".."))
	if len(sites) != 1 {
		t.Fatalf("protocol.ErrorNotSettled is answered from %v, and the round names one producer. "+
			"A site answering it for something that does not settle on its own has clients retrying for good",
			sites)
	}
	want := filepath.Join("internal", "engine", "whatsmeow", "group.go")
	if sites[0] != want {
		t.Errorf("the only producer is in %s, and the round names %s", sites[0], want)
	}
}

// selectionsOf walks every production file under root and answers the repository-relative
// paths where `protocol.<name>` is selected, outside `internal/protocol` itself.
func selectionsOf(t *testing.T, name, root string) []string {
	t.Helper()

	var found []string
	seen := map[string]bool{}
	owner := filepath.Join(root, "internal", "protocol")
	for _, dir := range []string{filepath.Join(root, "internal"), filepath.Join(root, "cmd")} {
		err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				if path == owner {
					return fs.SkipDir // the package that declares it names it by definition
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				return fmt.Errorf("parse %s: %w", path, err)
			}
			ast.Inspect(file, func(node ast.Node) bool {
				selector, ok := node.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != name {
					return true
				}
				pkg, ok := selector.X.(*ast.Ident)
				if !ok || pkg.Name != "protocol" {
					return true
				}
				relative, relErr := filepath.Rel(root, path)
				if relErr != nil {
					relative = path
				}
				if !seen[relative] {
					seen[relative] = true
					found = append(found, relative)
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}
	sort.Strings(found)
	return found
}
