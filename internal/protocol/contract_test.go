package protocol_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// contractDir is the language-neutral contract this package binds to. Tests read it
// from disk rather than embedding it so contract/ stays free of Go files: clients
// vendor that directory verbatim and compare checksums.
const contractDir = "../../contract"

func TestVersionMatchesContract(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(contractDir, "PROTOCOL_VERSION"))
	if err != nil {
		t.Fatalf("read PROTOCOL_VERSION: %v", err)
	}
	version, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatalf("parse PROTOCOL_VERSION: %v", err)
	}
	if version != protocol.Version {
		t.Fatalf("protocol.Version is %d, contract says %d", protocol.Version, version)
	}
}

func TestFixturesValidateAgainstSchema(t *testing.T) {
	for _, kind := range []string{"event", "command"} {
		schema := compile(t, "#/definitions/"+kind)
		for name, fixture := range fixtures(t, kind) {
			t.Run(kind+"/"+name, func(t *testing.T) {
				if err := schema.Validate(fixture); err != nil {
					t.Fatalf("does not validate: %v", err)
				}
			})
		}
	}
}

// Every type in the catalog must have a golden frame. Without this a type can be
// added to the schema and to Go, and never be exercised by either side.
func TestEveryTypeHasAFixture(t *testing.T) {
	events := typesInFixtures(t, "event")
	for _, known := range protocol.AllEventTypes {
		if !events[string(known)] {
			t.Errorf("event type %q has no fixture", known)
		}
	}
	commands := typesInFixtures(t, "command")
	for _, known := range protocol.AllCommandTypes {
		if !commands[string(known)] {
			t.Errorf("command type %q has no fixture", known)
		}
	}
}

// And the reverse: a fixture whose type this build does not know about means the
// Go catalog is behind the contract.
func TestEveryFixtureTypeIsKnown(t *testing.T) {
	for name := range typesInFixtures(t, "event") {
		if !protocol.EventType(name).Valid() {
			t.Errorf("fixture uses unknown event type %q", name)
		}
	}
	for name := range typesInFixtures(t, "command") {
		if !protocol.CommandType(name).Valid() {
			t.Errorf("fixture uses unknown command type %q", name)
		}
	}
}

func TestErrorCodesMatchSchema(t *testing.T) {
	var document struct {
		Definitions struct {
			ErrorCode struct {
				Enum []string `json:"enum"`
			} `json:"error_code"`
		} `json:"definitions"`
	}
	read(t, filepath.Join(contractDir, "schema", "protocol.schema.json"), &document)

	known := make([]string, 0, len(protocol.AllErrorCodes))
	for _, code := range protocol.AllErrorCodes {
		known = append(known, string(code))
	}
	if !reflect.DeepEqual(known, document.Definitions.ErrorCode.Enum) {
		t.Fatalf("error codes drifted:\n go:     %v\n schema: %v", known, document.Definitions.ErrorCode.Enum)
	}
}

func TestNewErrorDegradesUnknownCode(t *testing.T) {
	err := protocol.NewError("not_a_contract_code", "boom")
	if err.Code != protocol.ErrorInternal {
		t.Fatalf("expected an unknown code to degrade to internal, got %q", err.Code)
	}
}

// A frame survives the round trip through the flat string map a stream entry is.
func TestFramesRoundTripThroughStreamFields(t *testing.T) {
	for name, fixture := range fixtures(t, "event") {
		t.Run("event/"+name, func(t *testing.T) {
			original := decodeEvent(t, fixture)
			fields, err := original.Fields()
			if err != nil {
				t.Fatalf("Fields: %v", err)
			}
			parsed, err := protocol.ParseEvent(fields)
			if err != nil {
				t.Fatalf("ParseEvent: %v", err)
			}
			assertSameFrame(t, original, parsed)
		})
	}
	for name, fixture := range fixtures(t, "command") {
		t.Run("command/"+name, func(t *testing.T) {
			original := decodeCommand(t, fixture)
			fields, err := original.Fields()
			if err != nil {
				t.Fatalf("Fields: %v", err)
			}
			parsed, err := protocol.ParseCommand(fields)
			if err != nil {
				t.Fatalf("ParseCommand: %v", err)
			}
			assertSameFrame(t, original, parsed)
		})
	}
}

func TestParseEventRejectsAnotherMajor(t *testing.T) {
	fields := map[string]string{
		"v": strconv.Itoa(protocol.Version + 1), "id": "1", "type": "session.state",
		"sid": "s", "epoch": "1", "seq": "1", "ts": "1755440000123", "payload": "{}",
	}
	if _, err := protocol.ParseEvent(fields); err == nil {
		t.Fatal("expected a newer major to be rejected")
	}
}

func TestCursorOrdersByEpochThenSeq(t *testing.T) {
	cases := []struct {
		newer, older protocol.Cursor
		want         bool
	}{
		{protocol.Cursor{Epoch: 1, Seq: 2}, protocol.Cursor{Epoch: 1, Seq: 1}, true},
		{protocol.Cursor{Epoch: 2, Seq: 0}, protocol.Cursor{Epoch: 1, Seq: 999}, true},
		{protocol.Cursor{Epoch: 1, Seq: 1}, protocol.Cursor{Epoch: 1, Seq: 1}, false},
		{protocol.Cursor{Epoch: 1, Seq: 999}, protocol.Cursor{Epoch: 2, Seq: 0}, false},
	}
	for _, c := range cases {
		if got := c.newer.After(c.older); got != c.want {
			t.Errorf("%s after %s = %v, want %v", c.newer, c.older, got, c.want)
		}
		parsed, err := protocol.ParseCursor(c.newer.String())
		if err != nil || parsed != c.newer {
			t.Errorf("round trip of %s gave %v (%v)", c.newer, parsed, err)
		}
	}
}

func compile(t *testing.T, pointer string) *jsonschema.Schema {
	t.Helper()
	file, err := os.Open(filepath.Join(contractDir, "schema", "protocol.schema.json"))
	if err != nil {
		t.Fatalf("open schema: %v", err)
	}
	defer file.Close()

	document, err := jsonschema.UnmarshalJSON(file)
	if err != nil {
		t.Fatalf("parse schema: %v", err)
	}
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource("protocol.schema.json", document); err != nil {
		t.Fatalf("add schema: %v", err)
	}
	schema, err := compiler.Compile("protocol.schema.json" + pointer)
	if err != nil {
		t.Fatalf("compile %s: %v", pointer, err)
	}
	return schema
}

// fixtures returns every golden frame of a kind, keyed by file name.
func fixtures(t *testing.T, kind string) map[string]any {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(contractDir, "fixtures", kind+"s", "*.json"))
	if err != nil {
		t.Fatalf("glob fixtures: %v", err)
	}
	if len(paths) == 0 {
		t.Fatalf("no %s fixtures found", kind)
	}
	all := make(map[string]any, len(paths))
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			t.Fatalf("open %s: %v", path, err)
		}
		fixture, err := jsonschema.UnmarshalJSON(file)
		file.Close()
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		all[strings.TrimSuffix(filepath.Base(path), ".json")] = fixture
	}
	return all
}

func typesInFixtures(t *testing.T, kind string) map[string]bool {
	t.Helper()
	seen := map[string]bool{}
	for _, fixture := range fixtures(t, kind) {
		frame, ok := fixture.(map[string]any)
		if !ok {
			t.Fatalf("%s fixture is not an object", kind)
		}
		name, _ := frame["type"].(string)
		seen[name] = true
	}
	return seen
}

func decodeEvent(t *testing.T, fixture any) protocol.Event {
	t.Helper()
	var event protocol.Event
	remarshal(t, fixture, &event)
	return event
}

func decodeCommand(t *testing.T, fixture any) protocol.Command {
	t.Helper()
	var command protocol.Command
	remarshal(t, fixture, &command)
	return command
}

// assertSameFrame compares two frames, treating the payload as JSON rather than as
// bytes: the round trip is not expected to preserve key order or whitespace.
func assertSameFrame[T protocol.Event | protocol.Command](t *testing.T, original, parsed T) {
	t.Helper()
	var want, got any
	remarshal(t, original, &want)
	remarshal(t, parsed, &got)
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("frame changed across the round trip:\n want %v\n got  %v", want, got)
	}
}

func remarshal(t *testing.T, from, into any) {
	t.Helper()
	raw, err := json.Marshal(from)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := json.Unmarshal(raw, into); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
}

func read(t *testing.T, path string, into any) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if err := json.Unmarshal(raw, into); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
}
