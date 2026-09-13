package app

import (
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// A wake a read recovers is handed out with the age it gathered unseen, and a peer claims
// it once it has sat for ClaimMinIdle. Handed out at the oldest the transport allows, it
// must still not be claimable before the lease this instance takes for it could expire, or
// a peer finds that lease live, is told the session is owned, and retires the only wake
// there was. The transport's own default knows nothing of leases.
func TestARecoveredWakeIsNotClaimableBeforeTheLeaseItTakesCouldExpire(t *testing.T) {
	loaded, err := LoadConfig("test-host")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	discard := zerolog.Nop()
	tight := loaded
	tight.ClaimMinIdle = tight.LeaseTTL + time.Second
	for name, cfg := range map[string]Config{"the defaults": loaded, "a claim delay just past the lease": tight} {
		opts := streamOptions(&cfg, &discard)
		if opts.ReadBackMaxAge <= 0 {
			t.Errorf("%s: no ReadBackMaxAge, so the transport's default applies", name)
			continue
		}
		if claimable := cfg.ClaimMinIdle - opts.ReadBackMaxAge; claimable <= cfg.LeaseTTL {
			t.Errorf("%s: a wake recovered at its max age %s is claimable %s after it is handed out, within the %s lease",
				name, opts.ReadBackMaxAge, claimable, cfg.LeaseTTL)
		}
	}
}
