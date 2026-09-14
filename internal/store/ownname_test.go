package store_test

import (
	"testing"

	"github.com/fazer-ai/whatsapp-connector/internal/store"
)

// What is kept is the name and what the row was holding instead, because the second is
// what makes the first answerable later: a row that still holds it has not moved, and a
// name is only newer than a row nobody has touched since.
func TestAnUnfiledNameKeepsWhatTheRowWasHolding(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()
	pair(t, container, "sid-1", "5511999990001")
	scoped := container.For("sid-1")

	if err := scoped.PutUnfiledName(ctx, store.UnfiledPushName, "Atendimento", "Antigo"); err != nil {
		t.Fatalf("PutUnfiledName: %v", err)
	}
	kept, found, err := scoped.UnfiledName(ctx, store.UnfiledPushName)
	if err != nil {
		t.Fatalf("UnfiledName: %v", err)
	}
	if !found {
		t.Fatal("nothing was kept for a name that was written down")
	}
	if kept.Name != "Atendimento" || kept.Held != "Antigo" {
		t.Errorf("what was kept reads %+v, want the new name beside what the rows held", kept)
	}
}

// The two names change on their own paths and one being behind says nothing about the
// other, so neither reads or clears the other.
func TestTheTwoKindsOfNameAreKeptApart(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()
	pair(t, container, "sid-1", "5511999990001")
	scoped := container.For("sid-1")

	if err := scoped.PutUnfiledName(ctx, store.UnfiledPushName, "Atendimento", "Antigo"); err != nil {
		t.Fatalf("PutUnfiledName: %v", err)
	}
	if err := scoped.PutUnfiledName(ctx, store.UnfiledVerifiedName, "Loja LTDA", "Loja"); err != nil {
		t.Fatalf("PutUnfiledName: %v", err)
	}
	if err := scoped.DropUnfiledName(ctx, store.UnfiledPushName); err != nil {
		t.Fatalf("DropUnfiledName: %v", err)
	}

	if _, found, err := scoped.UnfiledName(ctx, store.UnfiledPushName); err != nil {
		t.Fatalf("UnfiledName: %v", err)
	} else if found {
		t.Error("the push name is still kept after being dropped")
	}
	kept, found, err := scoped.UnfiledName(ctx, store.UnfiledVerifiedName)
	if err != nil {
		t.Fatalf("UnfiledName: %v", err)
	}
	if !found || kept.Name != "Loja LTDA" {
		t.Errorf("the verified name reads %+v (kept %v), want the one nothing dropped", kept, found)
	}
}

// A second failure is about a later name, and what it refused is a later row. Keeping the
// first would have a rebuilt session comparing against a row that moved two writes ago.
func TestAKeptNameIsReplacedByTheNextOne(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()
	pair(t, container, "sid-1", "5511999990001")
	scoped := container.For("sid-1")

	if err := scoped.PutUnfiledName(ctx, store.UnfiledPushName, "Atendimento", "Antigo"); err != nil {
		t.Fatalf("PutUnfiledName: %v", err)
	}
	if err := scoped.PutUnfiledName(ctx, store.UnfiledPushName, "Recepcao", "Atendimento"); err != nil {
		t.Fatalf("PutUnfiledName: %v", err)
	}

	kept, _, err := scoped.UnfiledName(ctx, store.UnfiledPushName)
	if err != nil {
		t.Fatalf("UnfiledName: %v", err)
	}
	if kept.Name != "Recepcao" || kept.Held != "Atendimento" {
		t.Errorf("what was kept reads %+v, want the latest failure", kept)
	}
}

// One session's name is its own. A database is shared by a whole fleet, so a read that
// crossed sessions would answer one account with another account's name.
func TestAKeptNameIsNotReadByAnotherSession(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()
	pair(t, container, "sid-1", "5511999990001")
	pair(t, container, "sid-2", "5511999990002")

	if err := container.For("sid-1").PutUnfiledName(ctx, store.UnfiledPushName, "Atendimento", "Antigo"); err != nil {
		t.Fatalf("PutUnfiledName: %v", err)
	}
	if _, found, err := container.For("sid-2").UnfiledName(ctx, store.UnfiledPushName); err != nil {
		t.Fatalf("UnfiledName: %v", err)
	} else if found {
		t.Error("one session reads what another session kept")
	}
}

// The name belongs to the pairing. A row that outlived it would be answering for an
// account this device is no longer, which is what the foreign key is there to prevent.
func TestForgettingASessionTakesTheNameItKept(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()
	pair(t, container, "sid-1", "5511999990001")
	scoped := container.For("sid-1")

	if err := scoped.PutUnfiledName(ctx, store.UnfiledPushName, "Atendimento", "Antigo"); err != nil {
		t.Fatalf("PutUnfiledName: %v", err)
	}
	if err := scoped.Forget(ctx); err != nil {
		t.Fatalf("Forget: %v", err)
	}
	if _, found, err := container.For("sid-1").UnfiledName(ctx, store.UnfiledPushName); err != nil {
		t.Fatalf("UnfiledName: %v", err)
	} else if found {
		t.Error("a forgotten session still has a name kept for it")
	}
}

// Invariant 1: a lost lease fences the writes. A durable record written from outside the
// fence is a dead instance writing over what the live one has just written.
func TestALostLeaseKeepsNoName(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()
	pair(t, container, "sid-1", "5511999990001")
	scoped := container.For("sid-1")
	scoped.Drop()

	if err := scoped.PutUnfiledName(ctx, store.UnfiledPushName, "Atendimento", "Antigo"); err == nil {
		t.Error("a session that lost its lease was allowed to keep a name")
	}
	if err := scoped.DropUnfiledName(ctx, store.UnfiledPushName); err == nil {
		t.Error("a session that lost its lease was allowed to drop a name")
	}
	if _, found, err := container.For("sid-1").UnfiledName(ctx, store.UnfiledPushName); err != nil {
		t.Fatalf("UnfiledName: %v", err)
	} else if found {
		t.Error("a name was kept by a session that had lost its lease")
	}
}

// A record of a name has to have a name in it. An empty one would come back as a name the
// account calls itself and answer with nothing at all.
func TestAnEmptyNameIsNotKept(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()
	pair(t, container, "sid-1", "5511999990001")
	scoped := container.For("sid-1")

	if err := scoped.PutUnfiledName(ctx, store.UnfiledPushName, "", "Antigo"); err == nil {
		t.Error("a record with no name in it was kept")
	}
	if _, found, err := scoped.UnfiledName(ctx, store.UnfiledPushName); err != nil {
		t.Fatalf("UnfiledName: %v", err)
	} else if found {
		t.Error("a record with no name in it is on file")
	}
}

// A name kept here is one account's own, held against rows filed under that account's
// addresses. Pairing the same session id to another account updates the bond rather than
// deleting it, so the cascade does not fire: without clearing it by hand, a name would wait
// on the new pairing for a row whose value could match by being empty.
func TestRepairingASessionDropsTheNameTheLastAccountKept(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()
	pair(t, container, "sid-1", "5511999990001")
	scoped := container.For("sid-1")

	if err := scoped.PutUnfiledName(ctx, store.UnfiledPushName, "Atendimento", "Antigo"); err != nil {
		t.Fatalf("PutUnfiledName: %v", err)
	}
	// The same session, another account.
	pair(t, container, "sid-1", "5511999990002")

	if _, found, err := container.For("sid-1").UnfiledName(ctx, store.UnfiledPushName); err != nil {
		t.Fatalf("UnfiledName: %v", err)
	} else if found {
		t.Error("a name kept for one account is still kept after the session paired another")
	}
}

// And the other way: pairing the same account again is the same account, so what it was
// holding is still about the rows it is still filed under.
func TestPairingTheSameAccountAgainKeepsItsName(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()
	jid := pair(t, container, "sid-1", "5511999990001")
	scoped := container.For("sid-1")

	if err := scoped.PutUnfiledName(ctx, store.UnfiledPushName, "Atendimento", "Antigo"); err != nil {
		t.Fatalf("PutUnfiledName: %v", err)
	}
	if err := container.For("sid-1").Bind(ctx, jid); err != nil {
		t.Fatalf("Bind: %v", err)
	}

	kept, found, err := container.For("sid-1").UnfiledName(ctx, store.UnfiledPushName)
	if err != nil {
		t.Fatalf("UnfiledName: %v", err)
	}
	if !found || kept.Name != "Atendimento" {
		t.Errorf("the name reads %+v (kept %v), want the one the same pairing was still holding", kept, found)
	}
}
