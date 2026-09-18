package app_test

import (
	"io"
	"path/filepath"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow/proto/waAdv"
	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/store"
)

// The wiring end of the resume sweep, which is the half no unit test can reach.
//
// `internal/app/resume_internal_test.go` proves the sweep itself, and it proves it on the
// fake engine -- but it builds the Connector by hand and hands it a store. That is why
// the suite was green while a deployment configured the same way had no sweep at all:
// `newEngine` returns a nil container for the fake engine, `resumeWanted` closes its
// channel and returns on a nil store, and nothing between LoadConfig and Run says so.
//
// So this test goes through the door a deployment goes through: environment, LoadConfig,
// app.New, Run. What it asserts is the promise the sweep exists for, in the state every
// restart reaches -- the account is paired, the client asked for it to be connected, and
// no instance is running it.
//
// Seeded through the store rather than through a connect, and that is the point rather
// than a shortcut: a session this connector adopted itself would be running because it
// was adopted, and the assertion would pass with the sweep still dead. The account has to
// be in the database *before* the instance starts, so that the only thing that can bring
// it up is the pass that reads it.
func TestAFakeEngineDeploymentSweepsBackAnAccountNobodyIsRunning(t *testing.T) {
	server := miniredis.RunT(t)
	dsn := "sqlite:" + filepath.Join(t.TempDir(), "wa.db")

	const sid = "2f1c6f0e-0000-4000-8000-000000000265"
	seedWantedConnected(t, dsn, sid, "5511999990265")

	connector := start(t, server.Addr(), "inst-a", map[string]string{"WAC_DATABASE_URL": dsn})

	waitFor(t, "the account nobody was running to be brought back", func() bool {
		return connector.Sessions() == 1
	})
}

// A database this deployment did not configure still means no store, which is what the
// fake engine is for in every test that does not want one. The negative control of the
// test above: it is what stops the fix from being "open a store whatever happens", which
// would have every `WAC_ENGINE=fake` test in this package writing a file it never asked
// for.
func TestAFakeEngineWithNoDatabaseStillRunsWithoutOne(t *testing.T) {
	server := miniredis.RunT(t)

	connector := start(t, server.Addr(), "inst-b", map[string]string{"WAC_DATABASE_URL": ""})

	if err := connector.Ready(t.Context()); err != nil {
		t.Fatalf("a fake-engine instance with no database is not ready: %v", err)
	}
}

// seedWantedConnected puts an account in the database in the state a restart leaves it:
// paired, bound to a session id, and wanted connected.
func seedWantedConnected(t *testing.T, dsn, sid, phone string) {
	t.Helper()

	container, err := store.Open(t.Context(), dsn, store.AlwaysOwned, zerolog.New(io.Discard))
	if err != nil {
		t.Fatalf("open the store the connector will be pointed at: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := container.Close(); closeErr != nil {
			t.Errorf("close the seeding store: %v", closeErr)
		}
	})

	jid, err := waTypes.ParseJID(phone + ":12@" + waTypes.DefaultUserServer)
	if err != nil {
		t.Fatalf("ParseJID: %v", err)
	}
	device := container.Devices().NewDevice()
	device.ID = &jid
	device.Account = &waAdv.ADVSignedDeviceIdentity{
		Details: make([]byte, 32), AccountSignature: make([]byte, 64),
		AccountSignatureKey: make([]byte, 32), DeviceSignature: make([]byte, 64),
	}
	if err := container.Devices().PutDevice(t.Context(), device); err != nil {
		t.Fatalf("PutDevice: %v", err)
	}
	if err := container.For(sid).Bind(t.Context(), jid); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := container.For(sid).PutDesiredConnected(t.Context(), store.Wants{}); err != nil {
		t.Fatalf("PutDesiredConnected: %v", err)
	}
}
