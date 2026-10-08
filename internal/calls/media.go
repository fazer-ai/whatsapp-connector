// Package calls carries the voice of a WhatsApp call between meowcaller and the browser
// of whoever answers it. The connector stands where Meta stands in a WhatsApp Cloud API
// call: the browser is a WebRTC peer, and this package is the peer it talks to.
//
// The WhatsApp side speaks 16 kHz mono PCM in 60 ms frames, which is what meowcaller
// decodes to and encodes from. The browser side speaks G.722, which is wideband at the
// same rate, with PCMU kept for a browser that does not offer it. Both codecs are pure Go,
// so the image stays static.
package calls

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/pion/ice/v4"
	"github.com/pion/webrtc/v4"
	"github.com/rs/zerolog"
)

// gatherTimeout bounds how long building an SDP waits for its ICE candidates. With every
// candidate on the one UDP socket and no STUN server to ask, gathering is local and takes
// milliseconds; the bound is for the case where it does not, so a call offer is not held
// for ever behind it.
const gatherTimeout = 5 * time.Second

// Config is where the media of every call on this instance is reachable.
type Config struct {
	// UDPPort is the one UDP port every call's media shares. Required: a browser can only
	// reach this instance through a port the deployment published, and a random one is a
	// port nobody published. Zero is allowed only to let a test pick a free one.
	UDPPort int
	// PublicIPs are the addresses announced to the browser in place of this host's own,
	// for a host behind a 1:1 NAT. Empty announces the host's interface addresses.
	PublicIPs []string
}

// Media is the WebRTC side of every call on this instance: one UDP socket, and the codec
// table each browser leg is negotiated from.
type Media struct {
	api  *webrtc.API
	mux  ice.UDPMux
	port int
	log  zerolog.Logger
}

// Open binds the media socket. It is opened when the process starts rather than with the
// first call, so a port the deployment did not leave free fails the start instead of the
// first call somebody answers.
//
//nolint:gocritic // zerolog.Logger is designed to be copied; every With() returns one by value
func Open(cfg Config, log zerolog.Logger) (*Media, error) {
	if cfg.UDPPort < 0 || cfg.UDPPort > 65535 {
		return nil, fmt.Errorf("calls: UDP port %d is out of range", cfg.UDPPort)
	}
	for _, ip := range cfg.PublicIPs {
		if parsed := net.ParseIP(ip); parsed == nil || parsed.To4() == nil {
			return nil, fmt.Errorf("calls: %q is not an IPv4 address", ip)
		}
	}
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{Port: cfg.UDPPort})
	if err != nil {
		return nil, fmt.Errorf("calls: listen on UDP port %d: %w", cfg.UDPPort, err)
	}
	bound, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("calls: the media socket is bound to %v, which is not a UDP address", conn.LocalAddr())
	}
	mux := ice.NewUDPMuxDefault(ice.UDPMuxParams{UDPConn: conn})

	settings := webrtc.SettingEngine{}
	settings.SetICEUDPMux(mux)
	settings.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	// No multicast DNS: a container answering mDNS queries on the deployment's network is
	// noise nobody asked for, and the candidates here are addresses, not names.
	settings.SetICEMulticastDNSMode(ice.MulticastDNSModeDisabled)
	if len(cfg.PublicIPs) > 0 {
		if err := settings.SetICEAddressRewriteRules(webrtc.ICEAddressRewriteRule{
			External:        cfg.PublicIPs,
			AsCandidateType: webrtc.ICECandidateTypeHost,
		}); err != nil {
			_ = mux.Close()
			return nil, fmt.Errorf("calls: announce %v: %w", cfg.PublicIPs, err)
		}
	}

	codecs := &webrtc.MediaEngine{}
	for _, codec := range browserCodecs {
		if err := codecs.RegisterCodec(codec, webrtc.RTPCodecTypeAudio); err != nil {
			_ = mux.Close()
			return nil, fmt.Errorf("calls: register %s: %w", codec.MimeType, err)
		}
	}
	return &Media{
		api:  webrtc.NewAPI(webrtc.WithMediaEngine(codecs), webrtc.WithSettingEngine(settings)),
		mux:  mux,
		port: bound.Port,
		log:  log,
	}, nil
}

// Port is the UDP port the media socket is bound to.
func (m *Media) Port() int { return m.port }

// Close releases the socket. Every leg still open loses its media with it.
func (m *Media) Close() error { return m.mux.Close() }

// Offer builds the leg of a call this account is receiving: the connector's offer, for the
// browser to answer with Accept.
//
//nolint:gocritic // zerolog.Logger is designed to be copied; every With() returns one by value
func (m *Media) Offer(ctx context.Context, log zerolog.Logger) (*Leg, string, error) {
	leg, err := m.newLeg(log)
	if err != nil {
		return nil, "", err
	}
	offer, err := leg.pc.CreateOffer(nil)
	if err == nil {
		err = leg.pc.SetLocalDescription(offer)
	}
	if err != nil {
		_ = leg.Close()
		return nil, "", fmt.Errorf("calls: build the offer: %w", err)
	}
	sdp, err := leg.gathered(ctx)
	if err != nil {
		_ = leg.Close()
		return nil, "", err
	}
	return leg, sdp, nil
}

// Answer builds the leg of a call the client is placing from the browser's offer, and
// checks that it can be answered. The answer itself is made by Leg.Answer once the callee
// picks up.
//
//nolint:gocritic // zerolog.Logger is designed to be copied; every With() returns one by value
func (m *Media) Answer(offer string, log zerolog.Logger) (*Leg, error) {
	leg, err := m.newLeg(log)
	if err != nil {
		return nil, err
	}
	if err := leg.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: offer}); err != nil {
		_ = leg.Close()
		return nil, fmt.Errorf("%w: %w", ErrBadSDP, err)
	}
	answer, err := leg.pc.CreateAnswer(nil)
	if err != nil {
		_ = leg.Close()
		// An offer pion takes and cannot answer is one with no codec in common, which is
		// the browser's offer being unusable, not this side failing.
		return nil, fmt.Errorf("%w: %w", ErrBadSDP, err)
	}
	leg.pending = &answer
	return leg, nil
}

// ErrBadSDP is an SDP from the browser this side cannot use: unreadable, or with no codec
// in common.
var ErrBadSDP = errors.New("calls: the browser's SDP cannot be used")
