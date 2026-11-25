package whatsmeow

import (
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// referralOf is the Click-to-WhatsApp ad a message was sent from, nil when it carries
// none. WhatsApp puts the ad in the message's context as an external ad reply, which is
// also where the click id for Meta's Conversions API travels.
func referralOf(context *waE2E.ContextInfo) *protocol.Referral {
	ad := context.GetExternalAdReply()
	if ad == nil {
		return nil
	}
	referral := protocol.Referral{
		SourceType:   ad.GetSourceType(),
		SourceID:     ad.GetSourceID(),
		SourceURL:    ad.GetSourceURL(),
		CtwaClid:     ad.GetCtwaClid(),
		Title:        ad.GetTitle(),
		Body:         ad.GetBody(),
		MediaType:    adMediaTypeOf(ad.GetMediaType()),
		ThumbnailURL: ad.GetThumbnailURL(),
	}
	if referral == (protocol.Referral{}) {
		return nil
	}
	return &referral
}

// adMediaTypeOf names the ad's creative the way the Cloud API does. An ad with no
// creative says NONE, which is left out rather than published as a kind of media.
func adMediaTypeOf(kind waE2E.ContextInfo_ExternalAdReplyInfo_MediaType) string {
	switch kind {
	case waE2E.ContextInfo_ExternalAdReplyInfo_IMAGE:
		return "image"
	case waE2E.ContextInfo_ExternalAdReplyInfo_VIDEO:
		return "video"
	default:
		return ""
	}
}

// entryPointOf is where WhatsApp says the conversation started, nil when it does not say.
func entryPointOf(context *waE2E.ContextInfo) *protocol.EntryPoint {
	source := context.GetEntryPointConversionSource()
	if source == "" {
		return nil
	}
	return &protocol.EntryPoint{Source: source, App: context.GetEntryPointConversionApp()}
}
