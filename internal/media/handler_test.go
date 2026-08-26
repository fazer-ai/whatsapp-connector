package media_test

import (
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/fazer-ai/whatsapp-connector/internal/media"
)

func serve(t *testing.T, store *media.Store, token string) http.Handler {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("GET /media/{id}", media.Handler(media.HandlerOptions{Blobs: store, Token: token}))
	mux.Handle("HEAD /media/{id}", media.Handler(media.HandlerOptions{Blobs: store, Token: token}))
	return mux
}

func request(t *testing.T, handler http.Handler, method, target, auth string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), method, target, http.NoBody)
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestTheEndpointServesABlobWithWhatIsKnownAboutIt(t *testing.T) {
	t.Parallel()

	store, _ := newStore(t, media.Options{})
	stored := put(t, store, "the bytes", &media.Blob{Mime: "image/jpeg", Filename: "photo.jpg"})
	handler := serve(t, store, "s3cret")

	rec := request(t, handler, http.MethodGet, "/media/"+stored.ID, "Bearer s3cret")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET answered %d, want 200", rec.Code)
	}
	if body := rec.Body.String(); body != "the bytes" {
		t.Fatalf("the endpoint served %q", body)
	}
	if got := rec.Header().Get("Content-Type"); got != "image/jpeg" {
		t.Fatalf("Content-Type is %q, want the blob's own type", got)
	}
	if got := rec.Header().Get("Content-Disposition"); !strings.Contains(got, "photo.jpg") {
		t.Fatalf("Content-Disposition is %q, want the filename the message carried", got)
	}
	if got := rec.Header().Get("ETag"); got != `"`+stored.SHA256+`"` {
		t.Fatalf("ETag is %q, want the digest of what was stored", got)
	}
}

// The endpoint hands out message contents, and the token is the only thing in front of
// it. A refusal is told apart from a 404 on purpose: the client marks media that is gone
// unsupported for good, and a connector that will not take its token is an operational
// problem to surface instead.
func TestTheEndpointRefusesWithoutTheRightToken(t *testing.T) {
	t.Parallel()

	store, _ := newStore(t, media.Options{})
	stored := put(t, store, "the bytes", &media.Blob{})
	handler := serve(t, store, "s3cret")

	for _, auth := range []string{"", "Bearer", "Bearer wrong", "s3cret", "Basic s3cret", "bearer s3cret"} {
		rec := request(t, handler, http.MethodGet, "/media/"+stored.ID, auth)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("Authorization %q answered %d, want 401", auth, rec.Code)
		}
		if strings.Contains(rec.Body.String(), "the bytes") {
			t.Fatalf("Authorization %q was served the blob anyway", auth)
		}
	}
}

// An endpoint whose guard was left unset must refuse rather than serve everything: a
// missing setting is how a deployment opens something it did not mean to.
func TestAnEndpointWithNoTokenRefusesEverything(t *testing.T) {
	t.Parallel()

	store, _ := newStore(t, media.Options{})
	stored := put(t, store, "the bytes", &media.Blob{})
	handler := serve(t, store, "")

	for _, auth := range []string{"", "Bearer ", "Bearer anything"} {
		if rec := request(t, handler, http.MethodGet, "/media/"+stored.ID, auth); rec.Code != http.StatusUnauthorized {
			t.Fatalf("a store with no token answered %d to %q, want 401", rec.Code, auth)
		}
	}
}

func TestTheEndpointAnswers404ForABlobThatIsGone(t *testing.T) {
	t.Parallel()

	store, _ := newStore(t, media.Options{})
	handler := serve(t, store, "s3cret")

	rec := request(t, handler, http.MethodGet, "/media/blob_000102030405060708090a0b", "Bearer s3cret")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("a blob that is not here answered %d, want 404", rec.Code)
	}
}

// A client checking whether the bytes are still there before it queues a download should
// not have to pull the file to find out.
func TestAHeadRequestAnswersWithoutTheBody(t *testing.T) {
	t.Parallel()

	store, _ := newStore(t, media.Options{})
	stored := put(t, store, "the bytes", &media.Blob{Mime: "image/jpeg"})
	handler := serve(t, store, "s3cret")

	rec := request(t, handler, http.MethodHead, "/media/"+stored.ID, "Bearer s3cret")
	if rec.Code != http.StatusOK {
		t.Fatalf("HEAD answered %d, want 200", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("HEAD carried a body of %d bytes", rec.Body.Len())
	}
	if got := rec.Header().Get("Content-Length"); got != "9" {
		t.Fatalf("HEAD reported Content-Length %q, want the blob's length", got)
	}
}

// A large file is what makes a resumed download worth having, and a client that lost its
// connection halfway must not have to start over.
func TestTheEndpointAnswersARangeRequest(t *testing.T) {
	t.Parallel()

	store, _ := newStore(t, media.Options{})
	stored := put(t, store, "0123456789", &media.Blob{})
	handler := serve(t, store, "s3cret")

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/media/"+stored.ID, http.NoBody)
	req.Header.Set("Authorization", "Bearer s3cret")
	req.Header.Set("Range", "bytes=4-6")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusPartialContent {
		t.Fatalf("a range request answered %d, want 206", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	if string(body) != "456" {
		t.Fatalf("the range served %q, want the bytes asked for", body)
	}
}

// The filename comes off a message somebody else wrote, so a path or a quote in it must
// not reach the header as one.
func TestAFilenameFromAMessageCannotShapeTheHeader(t *testing.T) {
	t.Parallel()

	store, _ := newStore(t, media.Options{})
	stored := put(t, store, "the bytes", &media.Blob{Filename: `../../etc/pa"sswd`})
	handler := serve(t, store, "s3cret")

	rec := request(t, handler, http.MethodGet, "/media/"+stored.ID, "Bearer s3cret")
	got := rec.Header().Get("Content-Disposition")
	if strings.Contains(got, "..") || strings.Contains(got, "/") {
		t.Fatalf("Content-Disposition carries a path: %q", got)
	}
	if _, params, err := parseDisposition(got); err != nil || params["filename"] != `pa"sswd` {
		t.Fatalf("Content-Disposition is %q, want the base name quoted as one value (err=%v)", got, err)
	}
}

func parseDisposition(header string) (kind string, params map[string]string, err error) {
	return mime.ParseMediaType(header)
}
