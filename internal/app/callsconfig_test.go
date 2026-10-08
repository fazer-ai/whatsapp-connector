package app_test

import (
	"slices"
	"testing"

	"github.com/fazer-ai/whatsapp-connector/internal/app"
)

// Calls are off unless the deployment names a port for their media, and half a setting --
// an address to announce with no port to announce it for, a value that is not a port or
// not an address -- stops the instance instead of starting it with calls quietly off.
func TestTheCallsPortAndAddressAreReadAtStartup(t *testing.T) {
	t.Setenv("WAC_INSTANCE", "inst-a")

	cfg, err := app.LoadConfig("host")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.CallsUDPPort != 0 || len(cfg.CallsPublicIPs) != 0 {
		t.Fatalf("with nothing set calls read as port %d, %v; want off", cfg.CallsUDPPort, cfg.CallsPublicIPs)
	}

	t.Setenv("WAC_CALLS_UDP_PORT", "40000")
	t.Setenv("WAC_CALLS_PUBLIC_IP", " 203.0.113.7, 2001:db8::7 ")
	if cfg, err = app.LoadConfig("host"); err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.CallsUDPPort != 40000 || !slices.Equal(cfg.CallsPublicIPs, []string{"203.0.113.7", "2001:db8::7"}) {
		t.Fatalf("calls read as port %d, %v", cfg.CallsUDPPort, cfg.CallsPublicIPs)
	}

	for name, env := range map[string][2]string{
		"an address with no port": {"", "203.0.113.7"},
		"a port out of range":     {"70000", ""},
		"a negative port":         {"-1", ""},
		"a port that is a word":   {"forty", ""},
		"an address that is not":  {"40000", "calls.example.com"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("WAC_CALLS_UDP_PORT", env[0])
			t.Setenv("WAC_CALLS_PUBLIC_IP", env[1])
			if _, err := app.LoadConfig("host"); err == nil {
				t.Fatalf("WAC_CALLS_UDP_PORT=%q WAC_CALLS_PUBLIC_IP=%q was accepted", env[0], env[1])
			}
		})
	}
}
