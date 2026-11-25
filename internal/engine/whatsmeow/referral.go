package whatsmeow

import (
	"net/url"
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
// published even with no card and none of its fields readable, because that it came
// from an ad is itself the attribution.
func referralOf(context *waE2E.ContextInfo) *protocol.Referral {
	if !fromAnAd(context) {
		return nil
	}
	ad := context.GetExternalAdReply()
	referral := &protocol.Referral{
		SourceType:   ad.GetSourceType(),
		SourceID:     ad.GetSourceID(),
		SourceURL:    webURL(ad.GetSourceURL()),
		CtwaClid:     ad.GetCtwaClid(),
		Title:        ad.GetTitle(),
		Body:         ad.GetBody(),
		MediaType:    adMediaTypeOf(ad.GetMediaType()),
		ThumbnailURL: webURL(ad.GetThumbnailURL()),
	}
	// A quoted ad is the older way WhatsApp attaches the ad, and the two fields it shares
	// with the card fill in what the card left out. Left out means the field is absent
	// from the card, not that it reads empty: a card that says NONE has said its ad has
	// no creative, and the quoted ad does not get to overrule it.
	if quoted := context.GetQuotedAd(); quoted != nil {
		if ad == nil || ad.Body == nil {
			referral.Body = quoted.GetCaption()
		}
		if ad == nil || ad.MediaType == nil {
			referral.MediaType = quotedAdMediaTypeOf(quoted.GetMediaType())
		}
	}
	return referral
}

// webURL is a URL the sender wrote, kept only when it is an http or https address with a
// host. Anything else (a `data:` URI carrying the image itself, a `javascript:` link, a
// path) is dropped on its own, leaving the rest of the referral: a frame carries a
// reference to media, never the media, and a client links to what it is handed.
func webURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Hostname() == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return ""
	}
	return raw
}

// fromAnAd is whether anything around the message says it came from an ad. Any one
// signal is enough, since WhatsApp does not send all of them every time. The list is
// every field of the pinned proto that only an ad sets, taken off the proto rather than
// off the messages seen so far; a field a link preview also sets (title, body, URLs,
// thumbnail, media type, source id, `ContainsAutoReply`, `RenderLargerThumbnail`) is
// left out, because it would make every preview card an ad.
func fromAnAd(context *waE2E.ContextInfo) bool {
	return adCard(context.GetExternalAdReply()) ||
		context.GetEntryPointConversionSource() == "ctwa_ad" ||
		strings.EqualFold(context.GetConversionSource(), "fb_ads") ||
		context.GetAlwaysShowAdAttribution() ||
		context.GetCtwaSignals() != "" ||
		len(context.GetCtwaPayload()) > 0 ||
		context.GetQuotedAd() != nil
}

// adCard is whether the external ad reply says it is an ad's card, and not a link
// preview drawn the same way.
func adCard(ad *waE2E.ContextInfo_ExternalAdReplyInfo) bool {
	if ad == nil {
		return false
	}
	return ad.GetCtwaClid() != "" ||
		// Read off the field and not the getter: CTWA is the enum's zero value, so the
		// getter says CTWA for every preview card that never set an ad type.
		ad.AdType != nil ||
		ad.GetSourceType() == "ad" || ad.GetSourceType() == "post" ||
		ad.GetShowAdAttribution() ||
		ad.GetClickToWhatsappCall() ||
		ad.GetAutomatedGreetingMessageShown() ||
		ad.GetGreetingMessageBody() != "" ||
		ad.GetAutomatedGreetingMessageCtaType() != "" ||
		ad.GetCtaPayload() != "" ||
		ad.GetWtwaAdFormat() ||
		ad.GetAdPreviewURL() != "" ||
		ad.GetContainsCtwaFlowsAutoReply()
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

// quotedAdMediaTypeOf is adMediaTypeOf for the quoted ad, whose enum is a separate type
// with the same three values.
func quotedAdMediaTypeOf(kind waE2E.ContextInfo_AdReplyInfo_MediaType) protocol.AdMediaType {
	switch kind {
	case waE2E.ContextInfo_AdReplyInfo_IMAGE:
		return protocol.AdMediaImage
	case waE2E.ContextInfo_AdReplyInfo_VIDEO:
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
