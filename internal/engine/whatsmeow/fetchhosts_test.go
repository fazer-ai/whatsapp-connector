package whatsmeow

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/rs/zerolog"
	wm "go.mau.fi/whatsmeow"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// What an operator writes is read as written, and what is not a host is refused rather
// than read as a nearby one: a list that matches nothing refuses every file, and one that
// matches the wrong thing allows what it was set to close (#31).
func TestFetchHostsAreReadAsWritten(t *testing.T) {
	t.Parallel()

	for raw, want := range map[string]FetchHosts{
		"":                         nil,
		" , ":                      nil,
		"rails":                    {{name: "rails"}},
		"Rails:3000":               {{name: "rails", port: "3000"}},
		"rails, blobs.example.com": {{name: "rails"}, {name: "blobs.example.com"}},
		"[::1]:8080":               {{name: "::1", port: "8080"}},
		"[::1]":                    {{name: "::1"}},
		"::1":                      {{name: "::1"}},
		"10.0.0.5:9000":            {{name: "10.0.0.5", port: "9000"}},
		"minio_1":                  {{name: "minio_1"}},
	} {
		got, err := ParseFetchHosts(raw)
		if err != nil {
			t.Fatalf("ParseFetchHosts(%q): %v", raw, err)
		}
		if len(got) != len(want) {
			t.Fatalf("ParseFetchHosts(%q) = %+v, want %+v", raw, got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("ParseFetchHosts(%q) = %+v, want %+v", raw, got, want)
			}
		}
	}

	for _, bad := range []string{
		"http://rails", "rails/blobs", "rails:0", "rails:70000", "rails:abc", ":80", "user@rails", "rails?x",
		"rails:3000:80", "[rails]", "[::1", "rai ls", "rails;x",
	} {
		if _, err := ParseFetchHosts(bad); err == nil {
			t.Errorf("ParseFetchHosts(%q) was accepted", bad)
		}
	}
}

// A host named without a port is allowed on any; with one, only on that one, counting the
// port a scheme implies. An empty list is every host, which is what an instance that
// never set it has always done.
func TestFetchHostsAllow(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		list, address string
		want          bool
	}{
		{"", "http://anything:9/x", true},
		{"rails", "http://rails:3000/x", true},
		{"rails", "https://RAILS/x", true},
		{"rails", "http://rails.evil/x", false},
		{"rails", "http://other/x", false},
		{"rails:3000", "http://rails:3000/x", true},
		{"rails:3000", "http://rails:3001/x", false},
		{"rails:3000", "http://rails/x", false},
		{"blobs:443", "https://blobs/x", true},
		{"blobs:80", "http://blobs/x", true},
		{"blobs:80", "https://blobs/x", false},
		{"[::1]:8080", "http://[::1]:8080/x", true},
	} {
		hosts, err := ParseFetchHosts(tc.list)
		if err != nil {
			t.Fatalf("ParseFetchHosts(%q): %v", tc.list, err)
		}
		address, err := url.Parse(tc.address)
		if err != nil {
			t.Fatalf("url.Parse(%q): %v", tc.address, err)
		}
		if got := hosts.allows(address); got != tc.want {
			t.Errorf("%q allows %s = %v, want %v", tc.list, tc.address, got, tc.want)
		}
	}
}

// counted is a file server that says how many requests reached it.
func counted(t *testing.T, handler http.HandlerFunc) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		handler(w, r)
	}))
	t.Cleanup(server.Close)
	return server, &hits
}

func serveFile(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/pdf")
	_, _ = w.Write([]byte("%PDF-1.4"))
}

// hostPort is a server's address as an entry in the list names it.
func hostPort(t *testing.T, server *httptest.Server) string {
	t.Helper()
	address, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("url.Parse: %v", err)
	}
	return address.Host
}

// A host outside the list is refused before it is dialled, as the caller's payload: the
// server never hears of it, and the answer is not one a retry changes.
func TestAFetchFromAHostOutsideTheListIsRefusedWithoutDialling(t *testing.T) {
	t.Parallel()

	server, hits := counted(t, serveFile)
	hosts, err := ParseFetchHosts("rails:3000")
	if err != nil {
		t.Fatalf("ParseFetchHosts: %v", err)
	}
	_, err = retrieveOverHTTP(t.Context(), server.URL+"/f.pdf", nil, hosts)
	assertCode(t, err, protocol.ErrorInvalidPayload)
	if n := hits.Load(); n != 0 {
		t.Fatalf("the refused host was asked %d times", n)
	}
}

// A host in the list is fetched, and so is any host with no list at all.
func TestAFetchFromAListedHostGoesThrough(t *testing.T) {
	t.Parallel()

	server, _ := counted(t, serveFile)
	for _, list := range []string{hostPort(t, server), ""} {
		hosts, err := ParseFetchHosts(list)
		if err != nil {
			t.Fatalf("ParseFetchHosts: %v", err)
		}
		file, err := retrieveOverHTTP(t.Context(), server.URL+"/f.pdf", nil, hosts)
		if err != nil {
			t.Fatalf("list %q: %v", list, err)
		}
		_ = file.body.Close()
	}
}

// The hop an allowlist checked only on the way in would miss: a listed host that
// redirects somewhere else. The destination is never asked, and the refusal says it was
// the redirect. Between two listed hosts the redirect is followed.
func TestARedirectOutOfTheListIsRefused(t *testing.T) {
	t.Parallel()

	outside, outsideHits := counted(t, serveFile)
	// Named by `localhost` rather than the loopback address, so the two servers differ by
	// host and not only by port.
	outsideURL := strings.Replace(outside.URL, "127.0.0.1", "localhost", 1)
	listed, _ := counted(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, outsideURL+"/f.pdf", http.StatusFound)
	})

	hosts, err := ParseFetchHosts(hostPort(t, listed))
	if err != nil {
		t.Fatalf("ParseFetchHosts: %v", err)
	}
	_, err = retrieveOverHTTP(t.Context(), listed.URL+"/f.pdf", nil, hosts)
	assertCode(t, err, protocol.ErrorInvalidPayload)
	if !strings.Contains(err.Error(), "redirects") {
		t.Fatalf("the refusal does not say it was the redirect: %v", err)
	}
	if n := outsideHits.Load(); n != 0 {
		t.Fatalf("the host outside the list was asked %d times", n)
	}

	both, err := ParseFetchHosts(hostPort(t, listed) + ",localhost")
	if err != nil {
		t.Fatalf("ParseFetchHosts: %v", err)
	}
	file, err := retrieveOverHTTP(t.Context(), listed.URL+"/f.pdf", nil, both)
	if err != nil {
		t.Fatalf("a redirect between two listed hosts: %v", err)
	}
	_ = file.body.Close()
}

// The list reaches the fetch a session makes, and not only the function that takes it:
// a session built with one refuses a host outside it without dialling.
func TestASessionFetchesOnlyFromItsList(t *testing.T) {
	t.Parallel()

	server, hits := counted(t, serveFile)
	hosts, err := ParseFetchHosts("rails:3000")
	if err != nil {
		t.Fatalf("ParseFetchHosts: %v", err)
	}
	container := openStore(t)
	sid := "sid-" + t.Name()
	scoped := container.For(sid)
	device, err := scoped.Device(t.Context())
	if err != nil {
		t.Fatalf("Device: %v", err)
	}
	session := newSession(t.Context(), sid, wm.NewClient(device, nil), scoped, MediaOptions{FetchHosts: hosts}, nil,
		zerolog.Nop(), newLibraryLogger(zerolog.Nop(), sid))
	t.Cleanup(func() { _ = session.Close() })

	_, err = session.retrieve(t.Context(), server.URL+"/f.pdf", nil)
	assertCode(t, err, protocol.ErrorInvalidPayload)
	if n := hits.Load(); n != 0 {
		t.Fatalf("a session with a list asked a host outside it %d times", n)
	}
}
