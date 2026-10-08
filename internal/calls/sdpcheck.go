package calls

import (
	"errors"
	"fmt"

	"github.com/pion/sdp/v3"
)

// usableAudio checks, before an SDP from the browser is applied to a call's peer, that it
// carries a voice both ways: an audio section that is not rejected, that sends and
// receives, and the ICE credentials and DTLS fingerprint the connection needs. pion takes
// some SDPs without those, an answer with no fingerprint among them, and commits it before
// it says so, which leaves the call's peer unable to take a corrected one; one marked
// `inactive` it takes without complaint, and the call would connect with no voice.
func usableAudio(text string) error {
	var desc sdp.SessionDescription
	if err := desc.UnmarshalString(text); err != nil {
		return fmt.Errorf("%w: %w", ErrBadSDP, err)
	}
	for _, media := range desc.MediaDescriptions {
		if media.MediaName.Media != "audio" || media.MediaName.Port.Value == 0 {
			continue
		}
		for _, direction := range []string{sdp.AttrKeySendOnly, sdp.AttrKeyRecvOnly, sdp.AttrKeyInactive} {
			if _, set := media.Attribute(direction); set {
				return fmt.Errorf("%w: its audio is %s, and a call needs voice both ways", ErrBadSDP, direction)
			}
		}
		for _, needed := range []string{"ice-ufrag", "ice-pwd", "fingerprint"} {
			if !hasAttribute(&desc, media, needed) {
				return fmt.Errorf("%w: its audio has no %s", ErrBadSDP, needed)
			}
		}
		return nil
	}
	return fmt.Errorf("%w: %w", ErrBadSDP, errNoAudio)
}

var errNoAudio = errors.New("it has no audio to carry a voice on")

// hasAttribute is whether an attribute is set on the media section or on the session,
// where ICE credentials and fingerprints may be written once for every section.
func hasAttribute(desc *sdp.SessionDescription, media *sdp.MediaDescription, key string) bool {
	if _, set := media.Attribute(key); set {
		return true
	}
	_, set := desc.Attribute(key)
	return set
}
