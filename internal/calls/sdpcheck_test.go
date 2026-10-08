package calls

import (
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/pion/webrtc/v4"
	"github.com/rs/zerolog"
)

// An answer from the browser that cannot carry a voice both ways is refused before it
// touches the call's peer, and the peer can still take a good one afterwards.
func TestAnAnswerWithoutAVoiceBothWaysLeavesThePeerAsItWas(t *testing.T) {
	t.Parallel()
	m := openMedia(t)
	leg, offer, err := m.Offer(t.Context(), zerolog.Nop())
	if err != nil {
		t.Fatalf("offer: %v", err)
	}
	t.Cleanup(func() { _ = leg.Close() })
	b := newBrowser(t, webrtc.MimeTypeG722)
	good := b.answer(offer)

	drop := func(pattern string) string { return regexp.MustCompile(pattern).ReplaceAllString(good, "") }
	for name, bad := range map[string]string{
		"inactive":       strings.Replace(good, "a=sendrecv", "a=inactive", 1),
		"receive only":   strings.Replace(good, "a=sendrecv", "a=recvonly", 1),
		"send only":      strings.Replace(good, "a=sendrecv", "a=sendonly", 1),
		"no fingerprint": drop(`(?m)^a=fingerprint:.*\r?\n`),
		"no ice-ufrag":   drop(`(?m)^a=ice-ufrag:.*\r?\n`),
		"no ice-pwd":     drop(`(?m)^a=ice-pwd:.*\r?\n`),
		"audio rejected": regexp.MustCompile(`m=audio \d+`).ReplaceAllString(good, "m=audio 0"),
		// Present and malformed: pion commits the answer before it reads this, and
		// refuses it after.
		"a fingerprint with no value": regexp.MustCompile(`(?m)^a=fingerprint:(\S+) \S+`).ReplaceAllString(good, "a=fingerprint:$1"),
		// The direction set for the whole session, and none on the audio.
		"receive only for the session": strings.Replace(strings.Replace(good, "a=sendrecv\r\n", "", 1), "t=0 0\r\n", "t=0 0\r\na=recvonly\r\n", 1),
	} {
		if bad == good {
			t.Fatalf("%s: the edit changed nothing, so the case tests nothing", name)
		}
		if err := leg.Accept(bad); !errors.Is(err, ErrBadSDP) {
			t.Errorf("%s: Accept = %v, want ErrBadSDP", name, err)
		}
	}
	if err := leg.Accept(good); err != nil {
		t.Fatalf("a good answer after the bad ones: %v", err)
	}
	waitConnected(t, leg.pc, b.pc)
}
