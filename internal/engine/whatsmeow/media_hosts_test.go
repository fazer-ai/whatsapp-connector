package whatsmeow

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	wm "go.mau.fi/whatsmeow"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waEvents "go.mau.fi/whatsmeow/types/events"
	"go.mau.fi/whatsmeow/util/cbcutil"
	"go.mau.fi/whatsmeow/util/hkdfutil"
	"google.golang.org/protobuf/proto"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/media"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/testwait"
)

// sealedImage is a JPEG-ish plaintext encrypted the way a sender's phone encrypts it, and
// the message fields that describe it.
type sealedImage struct {
	plain, body []byte
	message     *waE2E.ImageMessage
}

func sealImage(t *testing.T, plain []byte) sealedImage {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	expanded := hkdfutil.SHA256(key, nil, []byte(wm.MediaImage), 112)
	iv, cipherKey, macKey := expanded[:16], expanded[16:48], expanded[48:80]
	ciphertext, err := cbcutil.Encrypt(cipherKey, iv, plain)
	if err != nil {
		t.Fatal(err)
	}
	mac := hmac.New(sha256.New, macKey)
	mac.Write(iv)
	mac.Write(ciphertext)
	body := append(bytes.Clone(ciphertext), mac.Sum(nil)[:10]...)
	encSum, plainSum := sha256.Sum256(body), sha256.Sum256(plain)
	return sealedImage{plain: plain, body: body, message: &waE2E.ImageMessage{
		Mimetype:      proto.String("image/jpeg"),
		DirectPath:    proto.String("/v/t62.7118-24/sealed.enc?ccb=11-4"),
		MediaKey:      key,
		FileEncSHA256: encSum[:],
		FileSHA256:    plainSum[:],
		FileLength:    proto.Uint64(uint64(len(plain))),
	}}
}

// mediaHosts points the session's own whatsmeow client at the given test servers, in
// order, as the media hosts WhatsApp would have handed out. Both reflect into whatsmeow's
// cache of the media connection, which is the one thing it would otherwise ask the socket.
func mediaHosts(t *testing.T, session *Session, hosts ...*httptest.Server) {
	t.Helper()
	client := session.current()
	client.SetMediaHTTPClient(hosts[0].Client())
	conn := &wm.MediaConn{TTL: 3600, FetchedAt: time.Now()}
	for _, host := range hosts {
		conn.Hosts = append(conn.Hosts, wm.MediaConnHost{Hostname: strings.TrimPrefix(host.URL, "https://")})
	}
	field := reflect.ValueOf(client).Elem().FieldByName("mediaConnCache")
	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Set(reflect.ValueOf(conn))
}

// mediaHost is a media host that answers every request with the same bytes, and counts them.
func mediaHost(t *testing.T, body []byte, asked *atomic.Int64) *httptest.Server {
	t.Helper()
	host := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		asked.Add(1)
		_, _ = w.Write(body)
	}))
	t.Cleanup(host.Close)
	return host
}

// A host that serves bytes the message does not describe is skipped for the next one, and
// the next one's good file is kept. whatsmeow walks the hosts writing into the one file it
// was handed without rewinding or truncating between them, so the good transfer used to be
// appended to the bad one, the MAC was read from the wrong offset, and a file that was
// never wrong was announced as corrupt for good (#90). A second attempt on a clean file
// does not help: it walks the same hosts into the same trap.
func TestAGoodHostAfterABadOneIsKeptAsServed(t *testing.T) {
	t.Parallel()

	session, _ := mediaSession(t, media.Options{})
	session.download = downloadOverClient
	sealed := sealImage(t, bytes.Repeat([]byte("jpeg"), 4096))
	var badAsked, goodAsked atomic.Int64
	bad := append(bytes.Repeat([]byte{0x55}, len(sealed.body)/2), sealed.body[:10]...)
	mediaHosts(t, session, mediaHost(t, bad, &badAsked), mediaHost(t, sealed.body, &goodAsked))

	emissions, acknowledged := deliverAll(t, session, mediaEvent("3EB0HOSTS", &waE2E.Message{ImageMessage: sealed.message}))
	if !acknowledged {
		t.Fatal("a message whose file the second host served was left to be redelivered")
	}
	if len(emissions) != 1 {
		t.Fatalf("published %d events, want the message alone: %s", len(emissions), emissions[len(emissions)-1].Payload)
	}
	content := mediaContentOf(t, emissions[0])
	if content.Ref == nil {
		t.Fatalf("the message carries no file (%+v), want the one the good host served", content)
	}
	if got := goodAsked.Load(); got != 1 {
		t.Errorf("the good host was asked %d times, want once", got)
	}
	if got := badAsked.Load(); got != 1 {
		t.Errorf("the bad host was asked %d times, want once", got)
	}
}

// The verdict stays honest: a file every host serves wrong is announced as corrupt, after
// one walk of the hosts and not two.
func TestAFileEveryHostServesWrongIsCorrupt(t *testing.T) {
	t.Parallel()

	session, _ := mediaSession(t, media.Options{})
	session.download = downloadOverClient
	sealed := sealImage(t, bytes.Repeat([]byte("jpeg"), 4096))
	var asked atomic.Int64
	tampered := bytes.Clone(sealed.body)
	tampered[len(tampered)-1] ^= 0xff
	mediaHosts(t, session, mediaHost(t, tampered, &asked), mediaHost(t, tampered, &asked))

	emissions, _ := deliver(t, session, mediaEvent("3EB0TAMPERED", &waE2E.Message{ImageMessage: sealed.message}), 2)
	if content := mediaContentOf(t, emissions[0]); content.Ref != nil {
		t.Fatal("a file every host served wrong was kept")
	}
	var failure protocol.MediaDownloadFailure
	if err := json.Unmarshal(emissions[1].Payload, &failure); err != nil {
		t.Fatalf("decode the failure: %v", err)
	}
	if failure.Reason != reasonCorrupt {
		t.Fatalf("the reason is %q, want %q", failure.Reason, reasonCorrupt)
	}
	if got := asked.Load(); got != 2 {
		t.Errorf("the hosts were asked %d times in all, want one walk of two", got)
	}
}

// deliverAll hands the session one message and reads everything it publishes until the
// handler comes back, however many events that is.
func deliverAll(t *testing.T, session *Session, event *waEvents.Message) ([]*engine.Emission, bool) {
	t.Helper()
	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.receive(event) }()
	var emissions []*engine.Emission
	for {
		select {
		case emission := <-session.Events():
			emission.Settle(nil)
			emissions = append(emissions, &emission)
		case got := <-acknowledged:
			return emissions, got
		case <-time.After(testwait.Budget):
			t.Fatal("the handler never came back from the message")
			return nil, false
		}
	}
}
