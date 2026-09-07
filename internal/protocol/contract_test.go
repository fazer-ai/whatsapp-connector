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
// Per frame type, and only per frame type. What a fixture pins is a shape, so the enums
// a payload can carry are not walked here and are not meant to be: they are held to the
// schema by TestErrorCodesMatchSchema, which is the check that actually catches a
// catalogue drifting from the contract. AGENTS.md used to read as though this test
// covered them, and for as long as it did nobody looked (#70).
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

// A command fixture carrying reply_to is a golden example of an RPC, so the two ways
// of saying "this command answers" have to agree. Without this, a fixture can promise a
// reply for a command no implementation ever replies to, and the caller hangs until its
// deadline. That is exactly how pairing.request_code shipped wrong.
func TestReplyToMatchesRPCClassification(t *testing.T) {
	for name, fixture := range fixtures(t, "command") {
		frame, ok := fixture.(map[string]any)
		if !ok {
			t.Fatalf("command fixture %s is not an object", name)
		}
		commandType := protocol.CommandType(frame["type"].(string))
		_, hasReplyTo := frame["reply_to"]
		_, hasDeadline := frame["deadline"]

		t.Run(name, func(t *testing.T) {
			if isRPC := protocol.IsRPC(commandType); hasReplyTo != isRPC {
				t.Fatalf("fixture has reply_to=%v but IsRPC(%s)=%v", hasReplyTo, commandType, isRPC)
			}
			if hasDeadline && !hasReplyTo {
				t.Fatalf("fixture carries a deadline without a reply_to: nobody would read the expiry")
			}
		})
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

// Every enum a payload can carry, held to the schema the same way the error codes are.
// A value added to the Go catalogue and not to the schema is a connector sending what
// the client's validator rejects; added to the schema and not to Go, it is a client
// sending what this build refuses to parse. Neither shows up in a fixture, because a
// fixture pins a frame's shape and these are values inside one.
//
// A catalogue is checked against every path the schema spells it out at, not one of
// them. The schema repeats an enum wherever it appears rather than referencing a shared
// definition, so a value added to the event's copy and not the command's would otherwise
// pass here while a client validating a command rejects what this build sends.
func TestPayloadEnumsMatchSchema(t *testing.T) {
	for name, enum := range payloadEnums() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			for _, path := range enum.paths {
				if published := schemaEnum(t, path); !reflect.DeepEqual(enum.known, published) {
					t.Errorf("%s drifted at %v:\n go:     %v\n schema: %v",
						name, path, enum.known, published)
				}
			}
		})
	}
}

// And the reverse, which is what keeps the two lists above honest. Every enum in the
// schema is either compared against a catalogue or named here as one the Go side carries
// as a plain string -- so an enum added to the contract fails this until somebody decides
// which it is, rather than being quietly unchecked. Listing the exceptions by hand is
// what let three of them go unlisted in the first version of this test.
func TestEverySchemaEnumIsAccountedFor(t *testing.T) {
	// Carried as plain strings, with no catalogue to compare. Giving one of these a type
	// in `internal/protocol` means a catalogue, a row above, and a line struck from here.
	unchecked := map[string]bool{
		"definitions/ban/properties/kind":                                               true,
		"definitions/connection_state/properties/connection":                            true,
		"definitions/group_participant/properties/role":                                 true,
		"definitions/group_info/properties/member_add_mode":                             true,
		"definitions/event_session_state/properties/state":                              true,
		"definitions/event_group_updated/properties/changes/properties/member_add_mode": true,
		"definitions/command_session_connect/properties/pairing":                        true,
		"definitions/command_session_wake/properties/desired":                           true,
		"definitions/command_message_mark_read/properties/type":                         true,
		"definitions/command_group_participants_update/properties/action":               true,
		"definitions/command_group_settings_set/properties/setting":                     true,
		"definitions/command_group_join_requests_update/properties/action":              true,
	}
	// error_code has a comparison of its own, in TestErrorCodesMatchSchema.
	checked := map[string]bool{"definitions/error_code": true}
	for _, enum := range payloadEnums() {
		for _, path := range enum.paths {
			checked[strings.Join(path, "/")] = true
		}
	}

	for _, path := range schemaEnumPaths(t) {
		if !checked[path] && !unchecked[path] {
			t.Errorf("the schema has an enum at %q that no catalogue is compared against "+
				"and nothing lists as unchecked", path)
		}
	}
	for path := range unchecked {
		if checked[path] {
			t.Errorf("%q is listed as unchecked and is also compared against a catalogue", path)
		}
	}
}

type payloadEnum struct {
	known []string
	paths [][]string
}

// payloadEnums is the catalogues and where the schema spells each one out.
func payloadEnums() map[string]payloadEnum {
	return map[string]payloadEnum{
		"address kind": {
			known: asStrings(protocol.AllAddressKinds),
			paths: [][]string{{"definitions", "address", "properties", "kind"}},
		},
		"media kind": {
			known: asStrings(protocol.AllMediaKinds),
			paths: [][]string{{"definitions", "content_media", "properties", "kind"}},
		},
		"media ref kind": {
			known: asStrings(protocol.AllMediaRefKinds),
			paths: [][]string{{"definitions", "media_ref", "properties", "kind"}},
		},
		"revoked by": {
			known: asStrings(protocol.AllRevokedBy),
			paths: [][]string{{"definitions", "event_message_revoked", "properties", "by"}},
		},
		"receipt kind": {
			known: asStrings(protocol.AllReceiptKinds),
			paths: [][]string{{"definitions", "event_message_receipt", "properties", "type"}},
		},
		"typing state": {
			known: asStrings(protocol.AllTypingStates),
			paths: [][]string{
				{"definitions", "event_chat_presence", "properties", "state"},
				{"definitions", "command_chat_presence", "properties", "state"},
			},
		},
		"presence state": {
			known: asStrings(protocol.AllPresenceStates),
			paths: [][]string{
				{"definitions", "event_presence_update", "properties", "state"},
				{"definitions", "command_presence_set", "properties", "state"},
			},
		},
		"unsupported reason": {
			known: asStrings(protocol.AllUnsupportedReasons),
			paths: [][]string{{"definitions", "content_unsupported", "properties", "reason"}},
		},
	}
}

// asStrings is the catalogue as the schema spells it. The catalogues are typed, and the
// enums they are compared against are plain strings.
func asStrings[T ~string](catalogue []T) []string {
	out := make([]string, 0, len(catalogue))
	for _, value := range catalogue {
		out = append(out, string(value))
	}
	return out
}

// schemaEnumPaths is every place the schema spells an enum out, as slash-joined paths.
func schemaEnumPaths(t *testing.T) []string {
	t.Helper()

	var document map[string]any
	read(t, filepath.Join(contractDir, "schema", "protocol.schema.json"), &document)

	var found []string
	var walk func(node any, path []string)
	// Down arrays as well as objects. A frame is a union, so `oneOf`, `anyOf` and
	// `allOf` decode as slices, and a walk that only descends into maps stops at the
	// edge of every one of them -- reporting that every enum is accounted for while not
	// having looked at the places JSON Schema puts alternatives.
	walk = func(node any, path []string) {
		switch node := node.(type) {
		case map[string]any:
			if _, has := node["enum"]; has {
				found = append(found, strings.Join(path, "/"))
			}
			for key, value := range node {
				walk(value, append(append([]string{}, path...), key))
			}
		case []any:
			for i, value := range node {
				walk(value, append(append([]string{}, path...), strconv.Itoa(i)))
			}
		}
	}
	walk(document, nil)
	if len(found) == 0 {
		t.Fatal("the schema has no enums at all, so this check is comparing nothing")
	}
	return found
}

// schemaEnum reads one enum out of the schema by the path it lives at, and fails when
// the path names nothing: a check that silently finds no enum is a check that passes on
// an empty comparison.
func schemaEnum(t *testing.T, path []string) []string {
	t.Helper()

	var document map[string]any
	read(t, filepath.Join(contractDir, "schema", "protocol.schema.json"), &document)

	var node any = document
	for _, step := range path {
		object, ok := node.(map[string]any)
		if !ok {
			t.Fatalf("%v is not an object at %q", path, step)
		}
		if node, ok = object[step]; !ok {
			t.Fatalf("the schema has nothing at %v (missing %q)", path, step)
		}
	}
	object, ok := node.(map[string]any)
	if !ok {
		t.Fatalf("the schema has no object at %v", path)
	}
	raw, ok := object["enum"].([]any)
	if !ok {
		t.Fatalf("the schema has no enum at %v", path)
	}
	published := make([]string, 0, len(raw))
	for _, value := range raw {
		published = append(published, value.(string))
	}
	return published
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

// A field cannot become required inside a protocol version. A connector pinned to an
// earlier v1 contract goes on sending the frame it was built to send, and a client
// vendoring this one has to keep accepting it: the version is what says whether the two
// can talk, and it has not moved.
//
// The frames listed here are the ones a version of this contract has published without a
// field a later one added. Adding to the list is how a new optional field is recorded;
// making one required is what bumps the version instead.
func TestAFieldAddedInsideAVersionStaysOptional(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		definition string
		payload    string
	}{
		"a passkey confirmation from before it was addressable": {
			definition: "event_pairing_passkey_confirmation",
			payload:    `{"code":"4821"}`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			schema := compile(t, "#/definitions/"+test.definition)
			var decoded any
			if err := json.Unmarshal([]byte(test.payload), &decoded); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if err := schema.Validate(decoded); err != nil {
				t.Fatalf("a frame an earlier %s published no longer validates: %v", protocolVersionLabel(), err)
			}
		})
	}
}

// protocolVersionLabel is the version this contract advertises, for the message above.
func protocolVersionLabel() string { return "v" + strconv.Itoa(protocol.Version) }
