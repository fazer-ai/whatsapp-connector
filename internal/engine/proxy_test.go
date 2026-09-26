package engine_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// A proxy is accepted when it is one the library can dial, and refused as the client's
// mistake when it is not.
//
// `invalid_payload` and never `unsupported`: the build routes through a proxy, so what is
// wrong is the address, and the client can correct it. The schemes are the ones
// whatsmeow's own `SetProxyAddress` takes, so nothing is accepted here that the pinned
// library would not dial.
//
// None of the refusals may repeat the URL. Go's parse error quotes it whole, and the
// refusal travels back as a reply a client stores and shows -- with the credentials in it.
func TestAProxyIsCheckedForWhatCanBeDialled(t *testing.T) {
	t.Parallel()

	for _, url := range []string{
		"socks5://u217:p4ss-217-xyzzy@127.0.0.1:1080",
		"http://u217:p4ss-217-xyzzy@10.0.0.1:3128",
		"https://proxy.example:8443",
		"socks5://[::1]:1080",
	} {
		request := engine.ConnectRequest{Pairing: "qr", Proxy: &engine.ProxyRequest{URL: url}}
		if err := request.Validate(); err != nil {
			t.Errorf("a proxy the library dials, %s, was refused: %v", url, err)
		}
	}
	for name, request := range map[string]engine.ConnectRequest{
		"no proxy object":  {Pairing: "qr"},
		"an empty url":     {Pairing: "qr", Proxy: &engine.ProxyRequest{}},
		"an absent object": {Pairing: "qr", Proxy: nil},
	} {
		if err := request.Validate(); err != nil {
			t.Errorf("%s is a session without a proxy, and it was refused: %v", name, err)
		}
		if request.ProxyURL() != "" {
			t.Errorf("%s reads as the proxy %q", name, request.ProxyURL())
		}
	}

	for _, url := range []string{
		"ftp://u217:p4ss-217-xyzzy@127.0.0.1:21",
		"socks4://127.0.0.1:1080",
		"socks5h://127.0.0.1:1080",
		"isto nao e url",
		"http://",
		"socks5://:1080",
		"socks5://u217:p4ss-217-xyzzy@[::1",
		"//u217:p4ss-217-xyzzy@127.0.0.1:1080",
	} {
		t.Run(url, func(t *testing.T) {
			t.Parallel()

			err := engine.ConnectRequest{Pairing: "qr", Proxy: &engine.ProxyRequest{URL: url}}.Validate()
			var coded *protocol.Error
			if !errors.As(err, &coded) || coded.Code != protocol.ErrorInvalidPayload {
				t.Fatalf("a proxy nobody can dial answered %v, want invalid_payload", err)
			}
			for _, leaked := range []string{"p4ss-217-xyzzy", "u217", url} {
				if strings.Contains(coded.Message, leaked) {
					t.Fatalf("the refusal %q repeats %q from the URL", coded.Message, leaked)
				}
			}
		})
	}
}
