package app_test

import (
	"net"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/alicebob/miniredis/v2"
)

// What an instance tells the registry about calls has to follow what it did when it
// started, not what it was asked: a client reads it to decide whether to offer calls,
// and an instance that says it carries them without the socket open is an enable switch
// whose every call fails. The fake engine opens no socket whatever the port says.
func TestTheRegistrySaysWhetherAnInstanceOpenedTheCallSocket(t *testing.T) {
	cases := []struct {
		name   string
		engine string
		port   bool
		want   string
	}{
		{name: "whatsmeow with the port", engine: "whatsmeow", port: true, want: "true"},
		{name: "whatsmeow without the port", engine: "whatsmeow", port: false, want: "false"},
		{name: "fake with the port", engine: "fake", port: true, want: "false"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := miniredis.RunT(t)
			env := map[string]string{
				"WAC_ENGINE":          tc.engine,
				"WAC_DATABASE_URL":    "sqlite:" + filepath.Join(t.TempDir(), "wa.db"),
				"WAC_CALLS_UDP_PORT":  "",
				"WAC_CALLS_PUBLIC_IP": "",
			}
			if tc.port {
				env["WAC_CALLS_UDP_PORT"] = strconv.Itoa(freeUDPPort(t))
			}
			start(t, server.Addr(), "inst-calls", env)

			waitFor(t, "the instance to announce itself", func() bool {
				return server.Exists("wa:instance:inst-calls")
			})
			if got := server.HGet("wa:instance:inst-calls", "calls"); got != tc.want {
				t.Fatalf("calls = %q, want %q", got, tc.want)
			}
		})
	}
}

func freeUDPPort(t *testing.T) int {
	t.Helper()
	conn, err := (&net.ListenConfig{}).ListenPacket(t.Context(), "udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick a UDP port: %v", err)
	}
	port := conn.LocalAddr().(*net.UDPAddr).Port
	_ = conn.Close()
	return port
}
