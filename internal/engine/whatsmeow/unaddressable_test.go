package whatsmeow

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"
)

// A message this build cannot address is one no redelivery will let it publish: the chat
// or the sender is on a server the contract does not name, or the stanza has no id to
// publish it under. Withholding its acknowledgement bought nothing but WhatsApp dropping
// the socket over it every fifty minutes, as #402 measured on the path for changes, so
// it is acknowledged and dropped where an operator can see it (#403).
func TestAnInboundMessageThisBuildCannotAddressIsAcknowledgedAndDropped(t *testing.T) {
	t.Parallel()

	unknown := waTypes.NewJID("someone", "interop.example")
	for _, tc := range []struct {
		name  string
		build func() *waEvents.Message
	}{
		{"a chat on a server the contract does not name", func() *waEvents.Message {
			event := textMessage("3EB0INTEROP", "segredo-403")
			event.Info.Chat = unknown
			event.Info.Sender = unknown
			return event
		}},
		{"a group message from somebody the contract cannot name", func() *waEvents.Message {
			event := textMessage("3EB0NOBODY", "segredo-403")
			event.Info.Chat = waTypes.NewJID("120363040000000001", waTypes.GroupServer)
			event.Info.IsGroup = true
			event.Info.Sender = unknown
			return event
		}},
		{"a stanza with no id", func() *waEvents.Message {
			return textMessage("", "segredo-403")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, written := newLoggedTestSession(t, "5511999990001")
			session.deliverWait = 50 * time.Millisecond
			// Subscribed, so a group message is not dropped for that before its sender is read.
			session.setGroups(true)
			for delivery := range 2 {
				if !publishedNothing(t, session, tc.build()) {
					t.Fatalf("delivery %d was left for WhatsApp to send again, and no redelivery will let this build publish it", delivery+1)
				}
			}

			logged := written.String()
			if strings.Contains(logged, "segredo-403") {
				t.Fatalf("the body of a dropped message reached the log: %s", logged)
			}
			if dropped := warnings(t, logged, "cannot address"); dropped != 2 {
				t.Fatalf("logged %d warnings for two dropped deliveries, want 2: %s", dropped, logged)
			}
		})
	}
}

// The drop is for the envelope alone. A message this build can address still goes out,
// and is still the one acknowledged after its event, on the same session right after one
// it dropped.
func TestDroppingAnUnaddressableMessageLeavesTheNextOneAlone(t *testing.T) {
	t.Parallel()

	session, _ := newLoggedTestSession(t, "5511999990001")
	session.deliverWait = 50 * time.Millisecond
	stray := textMessage("3EB0STRAY", "x")
	stray.Info.Chat = waTypes.NewJID("someone", "interop.example")
	stray.Info.Sender = stray.Info.Chat
	if !publishedNothing(t, session, stray) {
		t.Fatal("the unaddressable message was withheld")
	}

	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.receive(textMessage("3EB0NEXT", "bom dia")) }()
	select {
	case emission := <-session.Events():
		if !strings.Contains(string(emission.Payload), "3EB0NEXT") {
			t.Fatalf("published %s for something else: %s", emission.Type, emission.Payload)
		}
		emission.Settle(nil)
	case <-time.After(2 * time.Second):
		t.Fatal("the next message was never published")
	}
	if !<-acknowledged {
		t.Fatal("the next message was withheld")
	}
}

// warnings counts the warn-level lines whose message mentions what.
func warnings(t *testing.T, logged, what string) int {
	t.Helper()

	count := 0
	for line := range strings.SplitSeq(strings.TrimSpace(logged), "\n") {
		if line == "" {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("a log line is not JSON: %q", line)
		}
		if entry["level"] == "warn" && strings.Contains(entry["message"].(string), what) {
			count++
		}
	}
	return count
}
