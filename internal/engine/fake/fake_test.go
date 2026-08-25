package fake_test

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/engine/fake"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// The fake is what `serve --engine fake` runs, so what it publishes is what a client
// validates against the schema in the end-to-end check. A payload the contract rejects
// there proves nothing about the real engine and hides that the client would refuse it.
func TestPairingBurstMatchesTheContract(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		request engine.ConnectRequest
		want    []protocol.EventType
	}{
		"scanning a code": {
			request: engine.ConnectRequest{Pairing: "qr"},
			want: []protocol.EventType{
				protocol.EventPairingQR, protocol.EventPairingSuccess, protocol.EventSessionState,
			},
		},
		"typing a code": {
			request: engine.ConnectRequest{Pairing: "code", Phone: "5511999990001"},
			want: []protocol.EventType{
				protocol.EventPairingCode, protocol.EventPairingSuccess, protocol.EventSessionState,
			},
		},
		"resuming": {
			request: engine.ConnectRequest{Pairing: "resume"},
			want:    []protocol.EventType{protocol.EventSessionState},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			waEngine := fake.New()
			t.Cleanup(func() { _ = waEngine.Close() })
			session, err := waEngine.Open(t.Context(), "sid-1")
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			if err := session.Connect(t.Context(), test.request); err != nil {
				t.Fatalf("Connect: %v", err)
			}

			for _, want := range test.want {
				emission := <-session.Events()
				if emission.Type != want {
					t.Fatalf("published %q, want %q", emission.Type, want)
				}
				validate(t, want, emission.Payload)
			}
		})
	}
}

// The reply to session.connect and session.status is a `connection_state`, which is a
// different shape from the `session.state` event that reports the same change. The fake
// is what the end-to-end check reads, so a fake answering with the event's shape would
// prove the client accepts a reply it must reject.
func TestSessionStatusAnswersAConnectionState(t *testing.T) {
	t.Parallel()

	waEngine := fake.New()
	t.Cleanup(func() { _ = waEngine.Close() })
	session, err := waEngine.Open(t.Context(), "sid-1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := session.Connect(t.Context(), engine.ConnectRequest{Pairing: "qr"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	result, err := session.Execute(t.Context(), &protocol.Command{Type: protocol.CommandSessionStatus})
	if err != nil {
		t.Fatalf("session.status: %v", err)
	}
	validateAgainst(t, "connection_state", result)

	var status map[string]any
	if err := json.Unmarshal(result, &status); err != nil {
		t.Fatalf("unmarshal the status: %v", err)
	}
	if status["connection"] != "open" {
		t.Fatalf("connection=%v, want open", status["connection"])
	}
	if status["state"] != nil {
		t.Fatalf("the result carries the event's key too: state=%v", status["state"])
	}
}

// validate checks one payload against the definition the contract names for its type.
func validate(t *testing.T, eventType protocol.EventType, payload json.RawMessage) {
	t.Helper()

	pointer := "event_" + string(eventType)
	for i := range pointer {
		if pointer[i] == '.' {
			pointer = pointer[:i] + "_" + pointer[i+1:]
		}
	}

	validateAgainst(t, pointer, payload)
}

// validateAgainst checks a payload against one definition in the contract's schema.
func validateAgainst(t *testing.T, definition string, payload json.RawMessage) {
	t.Helper()

	compiler := jsonschema.NewCompiler()
	path := filepath.Join("..", "..", "..", "contract", "schema", "protocol.schema.json")
	schema, err := compiler.Compile(path + "#/definitions/" + definition)
	if err != nil {
		t.Fatalf("compile %s: %v", definition, err)
	}

	var decoded any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal the payload for %s: %v", definition, err)
	}
	if err := schema.Validate(decoded); err != nil {
		t.Fatalf("the payload does not match %s: %v", definition, err)
	}
}
