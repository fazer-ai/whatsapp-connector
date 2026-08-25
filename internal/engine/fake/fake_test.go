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

// validate checks one payload against the definition the contract names for its type.
func validate(t *testing.T, eventType protocol.EventType, payload json.RawMessage) {
	t.Helper()

	pointer := "event_" + string(eventType)
	for i := range pointer {
		if pointer[i] == '.' {
			pointer = pointer[:i] + "_" + pointer[i+1:]
		}
	}

	compiler := jsonschema.NewCompiler()
	path := filepath.Join("..", "..", "..", "contract", "schema", "protocol.schema.json")
	schema, err := compiler.Compile(path + "#/definitions/" + pointer)
	if err != nil {
		t.Fatalf("compile %s: %v", pointer, err)
	}

	var decoded any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal the payload of %s: %v", eventType, err)
	}
	if err := schema.Validate(decoded); err != nil {
		t.Fatalf("the payload of %s does not match the contract: %v", eventType, err)
	}
}
