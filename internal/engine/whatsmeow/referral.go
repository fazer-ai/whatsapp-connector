package whatsmeow

import (
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// referralOf is the Click-to-WhatsApp ad a message was sent from, nil when it carries
// none. WhatsApp puts the ad in the message's context as an external ad reply, which is
// also where the click id for Meta's Conversions API travels.
//
// An external ad reply is also how a link preview card is drawn, with a title, a
// thumbnail and a URL and no ad behind it. Only a card that says it came from an ad
// (a click id, an ad type, an ad or post source, or WhatsApp's own `ctwa_ad` entry
// point) is one,
// since a client files the first referral it keeps as the campaign that brought the
// contact in.
func referralOf(context *waE2E.ContextInfo) *protocol.Referral {
	ad := context.GetExternalAdReply()
	if ad == nil || !fromAnAd(context, ad) {
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

func fromAnAd(context *waE2E.ContextInfo, ad *waE2E.ContextInfo_ExternalAdReplyInfo) bool {
	switch {
	case ad.GetCtwaClid() != "":
		return true
	// Read off the field and not the getter: CTWA is the enum's zero value, so the
	// getter says CTWA for every preview card that never set an ad type.
	case ad.AdType != nil:
		return true
	case ad.GetSourceType() == "ad", ad.GetSourceType() == "post":
		return true
	default:
		return context.GetEntryPointConversionSource() == "ctwa_ad"
	}
}

// adMediaTypeOf names the ad's creative the way the Cloud API does. An ad with no
// creative says NONE, which is left out rather than published as a kind of media.
func adMediaTypeOf(kind waE2E.ContextInfo_ExternalAdReplyInfo_MediaType) protocol.AdMediaType {
	switch kind {
	case waE2E.ContextInfo_ExternalAdReplyInfo_IMAGE:
		return protocol.AdMediaImage
	case waE2E.ContextInfo_ExternalAdReplyInfo_VIDEO:
		return protocol.AdMediaVideo
	default:
		return ""
	}
}

// entryPointOf is where WhatsApp says the conversation started, nil when it says
// nothing. The source and the app are separate fields, and either can come alone.
func entryPointOf(context *waE2E.ContextInfo) *protocol.EntryPoint {
	entry := protocol.EntryPoint{
		Source: context.GetEntryPointConversionSource(),
		App:    context.GetEntryPointConversionApp(),
	}
	if entry == (protocol.EntryPoint{}) {
		return nil
	}
	return &entry
}
