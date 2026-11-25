package whatsmeow

import (
	"strings"

	waE2E "go.mau.fi/whatsmeow/proto/waE2E"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// referralOf is the Click-to-WhatsApp ad a message was sent from, nil when it carries
// none. WhatsApp puts the ad in the message's context as an external ad reply, which is
// also where the click id for Meta's Conversions API travels.
//
// An external ad reply is also how a link preview card is drawn, with a title, a
// thumbnail and a URL and no ad behind it, so the card alone does not make an ad: one
// of WhatsApp's ad signals has to say so (see fromAnAd). Once one does, the referral is
// published even with none of its fields readable, because that it came from an ad is
// itself the attribution.
func referralOf(context *waE2E.ContextInfo) *protocol.Referral {
	ad := context.GetExternalAdReply()
	if ad == nil || !fromAnAd(context, ad) {
		return nil
	}
	return &protocol.Referral{
		SourceType:   ad.GetSourceType(),
		SourceID:     ad.GetSourceID(),
		SourceURL:    ad.GetSourceURL(),
		CtwaClid:     ad.GetCtwaClid(),
		Title:        ad.GetTitle(),
		Body:         ad.GetBody(),
		MediaType:    adMediaTypeOf(ad.GetMediaType()),
		ThumbnailURL: ad.GetThumbnailURL(),
	}
}

// fromAnAd is every signal WhatsApp gives that a card is an ad and not a link preview:
// the click id, an ad type, an ad or post source on the card, and, on the context
// around it, the `ctwa_ad` entry point or the `FB_Ads` conversion source. Any one is
// enough, since WhatsApp does not send all of them every time.
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
	case context.GetEntryPointConversionSource() == "ctwa_ad":
		return true
	default:
		return strings.EqualFold(context.GetConversionSource(), "fb_ads")
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
