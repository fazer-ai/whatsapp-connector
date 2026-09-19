package engine_test

import (
	"errors"
	"testing"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/engine/fake"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// The answer to a connect this build does not serve is the connector's, not the engine's.
//
// Both production engines ask `ConnectRequest.Validate()`, and this is the half of that
// claim the fake engine is responsible for. Before #266 it answered two of these with a
// bare Go error, which reaches a client as `internal` -- the code that means "something
// went wrong here and we are not telling you what" -- while the whatsmeow engine answered
// `invalid_payload` for the identical request. A client's error code depended on which
// engine its deployment happened to run, and `internal` is the one code a client cannot
// act on.
//
// The whatsmeow half is `TestAConnectRefusedForItsShapeChangesNothing`, in that engine's
// own package, because building one of its sessions needs a device store it does not
// export.
func TestTheFakeEngineRefusesAConnectTheWayTheConnectorDoes(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		name    string
		request engine.ConnectRequest
		code    protocol.ErrorCode
	}{
		{"proxy", engine.ConnectRequest{Pairing: "qr", Proxy: &engine.ProxyRequest{URL: "socks5://10.0.0.1:1080"}}, protocol.ErrorUnsupported},
		{"history sync", engine.ConnectRequest{Pairing: "qr", HistorySync: true}, protocol.ErrorUnsupported},
		{"unknown pairing mode", engine.ConnectRequest{Pairing: "telepatia"}, protocol.ErrorInvalidPayload},
		{"code pairing without a phone", engine.ConnectRequest{Pairing: "code", Phone: "+ ()-"}, protocol.ErrorInvalidPayload},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			session, err := fake.New().Open(t.Context(), "sid-"+c.name)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			t.Cleanup(func() { _ = session.Close() })

			err = session.Connect(t.Context(), c.request)
			var coded *protocol.Error
			switch {
			case err == nil:
				t.Fatalf("a connect carrying %s was accepted", c.name)
			case !errors.As(err, &coded):
				t.Fatalf("a connect carrying %s was refused with %v, which carries no code and "+
					"reaches the client as `internal`, where the other engine answers %q for the "+
					"same request.", c.name, err, c.code)
			case coded.Code != c.code:
				t.Fatalf("a connect carrying %s was refused with %q, want %q", c.name, coded.Code, c.code)
			}
		})
	}
}
