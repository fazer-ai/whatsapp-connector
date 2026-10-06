package whatsmeow

import (
	"bytes"
	"compress/zlib"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waHistorySync "go.mau.fi/whatsmeow/proto/waHistorySync"
	waStore "go.mau.fi/whatsmeow/store"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/fazer-ai/whatsapp-connector/internal/store"
)

// errSecretsDown is the database going away under one store, for as long as a test says.
var errSecretsDown = errors.New("message secrets: the database is not answering")

// breakableSecrets stands where the database does, under the fence: whatsmeow's own
// message-secret store, made to fail on demand, counting what it was asked to keep.
type breakableSecrets struct {
	waStore.MsgSecretStore

	mu      sync.Mutex
	broken  bool
	failed  int
	stored  int
	entered chan<- struct{} // told once a write has arrived, when somebody listens
	hold    <-chan struct{} // a write waits on it before answering, when one is set
}

func (b *breakableSecrets) setBroken(broken bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.broken = broken
}

func (b *breakableSecrets) counts() (failed, stored int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.failed, b.stored
}

func (b *breakableSecrets) PutMessageSecrets(ctx context.Context, inserts []waStore.MessageSecretInsert) error {
	b.mu.Lock()
	entered, hold, broken := b.entered, b.hold, b.broken
	b.mu.Unlock()
	if entered != nil {
		entered <- struct{}{}
	}
	if hold != nil {
		<-hold
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if broken {
		b.failed++
		return errSecretsDown
	}
	b.stored++
	return b.MsgSecretStore.PutMessageSecrets(ctx, inserts) //nolint:wrapcheck // stands in for the store
}

// breakSecrets puts a breakable store under the session's fence. The fence is put back
// over it whole, which leaves the device's other stores behind two fences: harmless, both
// answer to an owner that never lets go, and it is the only way past the unexported
// decorator to the store it decorates.
func breakSecrets(t *testing.T, session *Session) *breakableSecrets {
	t.Helper()
	device := session.current().Store
	breakable := &breakableSecrets{MsgSecretStore: device.MsgSecrets}
	device.MsgSecrets = breakable
	store.Fenced(device, store.NewFence(func() bool { return true }))
	return breakable
}

// inlineDump is a history notification that carries its dump inline, which is how the
// phone sends a small one: whatsmeow's own DownloadHistorySync decodes it and stores what
// it keeps without touching the network.
func inlineDump(t *testing.T, id string, dump *waHistorySync.HistorySync) *waEvents.Message {
	t.Helper()
	raw, err := proto.Marshal(dump)
	if err != nil {
		t.Fatalf("marshal the dump: %v", err)
	}
	var packed bytes.Buffer
	writer := zlib.NewWriter(&packed)
	if _, err := writer.Write(raw); err != nil {
		t.Fatalf("compress the dump: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("compress the dump: %v", err)
	}
	event := historyNotification(id, waE2E.HistorySyncType_RECENT)
	event.Message.GetProtocolMessage().GetHistorySyncNotification().InitialHistBootstrapInlinePayload = packed.Bytes()
	return event
}

const secretChat = "5511999990002@s.whatsapp.net"

// secretDump is a dump with one message that carries the secret a later sealed reaction or
// correction to it is opened with, or none when secret is nil.
func secretDump(id string, secret []byte) *waHistorySync.HistorySync {
	past := pastText(secretChat, id, 1754000000, "antes")
	past.Message.MessageSecret = secret
	return dumpOf(waHistorySync.HistorySync_RECENT, &waHistorySync.Conversation{
		ID:       proto.String(secretChat),
		Messages: []*waHistorySync.HistorySyncMsg{past},
	})
}

// secretSession is a paired session whose dumps go through whatsmeow's real download and
// storage, with the bench standing in for the socket only.
func secretSession(t *testing.T, phone string, history bool) (*Session, *historyBench, *breakableSecrets) {
	t.Helper()
	session, _ := newTestSession(t, phone)
	session.setHistory(history)
	// A dump that did not finish is retried by a timer; the tests run the retry themselves.
	session.historyRetry = time.Hour
	bench := &historyBench{}
	bench.install(session)
	session.downloadHistory = downloadHistoryOverClient
	return session, bench, breakSecrets(t, session)
}

// settled makes one attempt at a dump while reading what it publishes, the way a client
// would, and reports what the attempt answered.
func settled(t *testing.T, session *Session, attempt func() bool) bool {
	t.Helper()
	done := make(chan bool, 1)
	go func() { done <- attempt() }()
	_, got := slicesUntil(t, session, done)
	return got
}

func receiptsOf(bench *historyBench, id string) int {
	count := 0
	for _, receipted := range bench.receipted() {
		if receipted == id {
			count++
		}
	}
	return count
}

func isHeld(t *testing.T, session *Session, id string) bool {
	t.Helper()
	for _, pending := range heldDumps(t, session) {
		if pending == id {
			return true
		}
	}
	return false
}

// A dump whose message secrets could not be stored is not receipted, because the phone
// never sends those secrets again and a receipt tells it the dump arrived whole. Kept for
// the retry, which stores them once the database answers, and receipts it then.
func TestADumpWhoseSecretsCouldNotBeStoredIsNotReceipted(t *testing.T) {
	t.Parallel()

	secret := bytes.Repeat([]byte{0x35}, 32)
	session, bench, secrets := secretSession(t, "5511988880000", true)
	secrets.setBroken(true)

	if !settled(t, session, func() bool { return session.receive(inlineDump(t, "HIST350-N1", secretDump("3EB0HIST350A", secret))) }) {
		t.Fatal("the notification was not acknowledged")
	}
	if failed, _ := secrets.counts(); failed == 0 {
		t.Fatal("the secrets were never written, so the dump proves nothing")
	}
	if got := receiptsOf(bench, "HIST350-N1"); got != 0 {
		t.Fatalf("a dump whose secrets were lost was receipted %d times", got)
	}
	if !isHeld(t, session, "HIST350-N1") {
		t.Fatal("a dump whose secrets were lost was forgotten, and nothing announces it again")
	}

	secrets.setBroken(false)
	if !settled(t, session, session.replayOnce) {
		t.Fatal("the retry did not finish a dump whose secrets now store")
	}
	if got := receiptsOf(bench, "HIST350-N1"); got != 1 {
		t.Fatalf("the retried dump was receipted %d times, want once", got)
	}
	if isHeld(t, session, "HIST350-N1") {
		t.Fatal("the retried dump is still held")
	}
	chat := waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)
	kept, _, err := session.current().Store.MsgSecrets.GetMessageSecret(t.Context(), chat, chat, "3EB0HIST350A")
	if err != nil || !bytes.Equal(kept, secret) {
		t.Fatalf("the retry kept the secret %x (%v), want %x", kept, err, secret)
	}

	if !settled(t, session, session.replayOnce) || receiptsOf(bench, "HIST350-N1") != 1 {
		t.Fatalf("a third attempt receipted the dump again: %v", bench.receipted())
	}
}

// What holds a dump back is a write the dump made, not the state of the store: a dump with
// nothing to keep is receipted while that store is down, and a failure before the dump is
// not the dump's.
func TestOnlyTheDumpsOwnWritesHoldItBack(t *testing.T) {
	t.Parallel()

	t.Run("nothing to keep", func(t *testing.T) {
		t.Parallel()
		session, bench, secrets := secretSession(t, "5511988880000", true)
		secrets.setBroken(true)
		settled(t, session, func() bool { return session.receive(inlineDump(t, "HIST350-N2", secretDump("3EB0HIST350B", nil))) })
		if failed, stored := secrets.counts(); failed+stored != 0 {
			t.Fatalf("a dump without secrets wrote %d of them", failed+stored)
		}
		if receiptsOf(bench, "HIST350-N2") != 1 || isHeld(t, session, "HIST350-N2") {
			t.Fatalf("a dump with nothing to keep was receipted %v and held %v",
				bench.receipted(), heldDumps(t, session))
		}
	})

	t.Run("a failure before it", func(t *testing.T) {
		t.Parallel()
		session, bench, secrets := secretSession(t, "5511988880000", true)
		secrets.setBroken(true)
		chat := waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)
		if err := session.current().Store.MsgSecrets.PutMessageSecrets(t.Context(), []waStore.MessageSecretInsert{
			{Chat: chat, Sender: chat, ID: "3EB0LIVE", Secret: make([]byte, 32)},
		}); !errors.Is(err, errSecretsDown) {
			t.Fatalf("the live write answered %v", err)
		}
		secrets.setBroken(false)
		settled(t, session, func() bool {
			return session.receive(inlineDump(t, "HIST350-N3", secretDump("3EB0HIST350A", make([]byte, 32))))
		})
		if _, stored := secrets.counts(); stored != 1 {
			t.Fatalf("the dump stored its secrets %d times", stored)
		}
		if receiptsOf(bench, "HIST350-N3") != 1 || isHeld(t, session, "HIST350-N3") {
			t.Fatalf("a dump was held for a failure before it: receipted %v, held %v",
				bench.receipted(), heldDumps(t, session))
		}
	})

	t.Run("history not asked for", func(t *testing.T) {
		t.Parallel()
		// The secrets are what a live sealed reaction is opened with, whether or not the
		// conversations were published.
		session, bench, secrets := secretSession(t, "5511988880000", false)
		secrets.setBroken(true)
		session.receive(inlineDump(t, "HIST350-N4", secretDump("3EB0HIST350A", make([]byte, 32))))
		if failed, _ := secrets.counts(); failed == 0 {
			t.Fatal("the secrets were never written")
		}
		if receiptsOf(bench, "HIST350-N4") != 0 || !isHeld(t, session, "HIST350-N4") {
			t.Fatalf("a dump nobody asked to publish lost its secrets and was receipted %v, held %v",
				bench.receipted(), heldDumps(t, session))
		}
	})
}

// The failure is the dump's own and nobody else's: two sessions storing at the same moment,
// one against a store that fails, each answer for their own write.
func TestTwoDumpsAtOnceAnswerForTheirOwnWrites(t *testing.T) {
	t.Parallel()

	for _, failing := range []int{0, 1} {
		t.Run([]string{"first fails", "second fails"}[failing], func(t *testing.T) {
			t.Parallel()
			sessions := [2]*Session{}
			benches := [2]*historyBench{}
			stores := [2]*breakableSecrets{}
			entered := make(chan struct{}, 2)
			release := make(chan struct{})
			for i, phone := range []string{"5511988880000", "5511977770000"} {
				// Not published, so neither attempt waits on a reader: what is under test is the
				// write, and the secrets are kept whether or not history is.
				sessions[i], benches[i], stores[i] = secretSession(t, phone, false)
				stores[i].entered, stores[i].hold = entered, release
			}
			stores[failing].setBroken(true)

			ids := [2]string{"HIST350-A", "HIST350-B"}
			var done sync.WaitGroup
			for i := range sessions {
				done.Go(func() { sessions[i].receive(inlineDump(t, ids[i], secretDump("3EB0HIST350"+ids[i], make([]byte, 32)))) })
			}
			// Both writes are in flight before either answers.
			<-entered
			<-entered
			close(release)
			done.Wait()

			for i := range sessions {
				want := 1
				if i == failing {
					want = 0
				}
				if got := receiptsOf(benches[i], ids[i]); got != want {
					t.Errorf("%s was receipted %d times, want %d", ids[i], got, want)
				}
				if isHeld(t, sessions[i], ids[i]) != (i == failing) {
					t.Errorf("%s held %v, want %v", ids[i], isHeld(t, sessions[i], ids[i]), i == failing)
				}
			}
		})
	}
}
