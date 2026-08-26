package media

import (
	"crypto/subtle"
	"errors"
	"io"
	"mime"
	"net/http"
	"path"
	"strings"
	"time"
)

// Blobs is what the handler serves from. It is the store's read half, named separately
// so a test can stand in for it.
type Blobs interface {
	Open(id string) (io.ReadSeekCloser, Blob, error)
}

// HandlerOptions configures the endpoint.
type HandlerOptions struct {
	Blobs Blobs
	// Token is what a caller has to present. Instances publish it in the registry, so
	// whoever can read the Redis can read the media and an operator has nothing to
	// configure. An empty token refuses everything rather than allowing everything: an
	// endpoint that hands out message contents must not be opened by a missing setting.
	Token string
}

// Handler serves one blob per request.
//
// It answers 404 for a blob that is not here and does not distinguish that from one
// that never existed. The client's answer to both is the same, and the difference is
// only ever interesting to somebody guessing ids.
func Handler(opts HandlerOptions) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !authorized(opts.Token, r) {
			// Distinguished from a 404 on purpose. A client that is refused has an
			// operational problem worth surfacing, and one that is told the media is
			// gone marks the message unsupported for good.
			w.Header().Set("WWW-Authenticate", `Bearer realm="media"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		id := r.PathValue("id")
		body, about, err := opts.Blobs.Open(id)
		switch {
		case errors.Is(err, ErrNotFound):
			http.Error(w, "no such blob", http.StatusNotFound)
			return
		case err != nil:
			http.Error(w, "could not read that blob", http.StatusInternalServerError)
			return
		}
		defer func() { _ = body.Close() }()

		if about.Mime != "" {
			w.Header().Set("Content-Type", about.Mime)
		}
		if about.Filename != "" {
			// The name only, and quoted by the standard encoder: it comes off a message
			// somebody else wrote, so a path or a quote in it must not reach the header
			// as one.
			w.Header().Set("Content-Disposition",
				mime.FormatMediaType("attachment", map[string]string{"filename": path.Base(about.Filename)}))
		}
		if about.SHA256 != "" {
			w.Header().Set("ETag", `"`+about.SHA256+`"`)
		}
		// ServeContent rather than io.Copy: it answers a range request, which is what
		// lets a client resume a large file instead of starting the download over.
		// A zero time keeps it from writing a Last-Modified this store cannot honour,
		// since a blob's modification time is when it was last collected.
		http.ServeContent(w, r, about.Filename, zeroTime, body)
	})
}

// authorized compares in constant time, so the endpoint does not tell a caller how much
// of a token it got right.
func authorized(token string, r *http.Request) bool {
	if token == "" {
		return false
	}
	presented, found := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !found {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(presented), []byte(token)) == 1
}

// zeroTime is what the handler tells ServeContent, because a blob's modification time
// is when it was last handed out rather than anything about its contents, and a client
// caching on it would refetch a file that had not changed.
var zeroTime time.Time
