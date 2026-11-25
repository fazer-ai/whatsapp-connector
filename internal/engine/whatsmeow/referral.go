package whatsmeow

import (
	"net/url"
	"strconv"
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
	if forwarded(context) || !fromAnAd(context) {
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
//
// The port is held to the same shapes the blob address is in engine.go: url.Parse only
// checks that a port is digits, so 99999 and 0 come through it, and a colon with nothing
// after it parses as no port at all. The host is not judged past being there: whether
// a name or an address opens is the client's to find out, as with any link, and a
// second copy of a browser's host parser here would only disagree with the browser.
func webURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Hostname() == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return ""
	}
	if strings.HasSuffix(parsed.Host, ":") {
		return ""
	}
	if port := parsed.Port(); port != "" {
		number, err := strconv.Atoi(port)
		if err != nil || number < 1 || number > 65535 {
			return ""
		}
	}
	return raw
}

// forwarded is a message passed on from another chat. A forward keeps the context of
// the message it copies, ad card and entry point included, and those say how the
// original conversation started, not this one: published, they would credit the ad
// with a contact it never brought in.
//
// Every field of the pinned proto that says a message is a forward counts, since each
// kind of forward (from a chat, a channel, a business, Meta AI) sets its own and none
// of them sets all. ForwardOrigin is read off the field because UNKNOWN is its zero
// value.
func forwarded(context *waE2E.ContextInfo) bool {
	return context.GetIsForwarded() ||
		context.GetForwardingScore() > 0 ||
		context.GetForwardedNewsletterMessageInfo() != nil ||
		context.GetBusinessMessageForwardInfo() != nil ||
		context.GetForwardedAiBotMessageInfo() != nil ||
		(context != nil && context.ForwardOrigin != nil)
}

// fromAnAd is whether anything around the message says it came from an ad. Any one
// signal is enough, since WhatsApp does not send all of them every time. The list is
// every field of the pinned proto that only an ad sets, taken off the proto rather than
// off the messages seen so far; a field a link preview also sets (title, body, URLs,
// thumbnail, media type, source id, `ContainsAutoReply`, `RenderLargerThumbnail`) is
// left out, because it would make every preview card an ad. So are `SourceApp`,
// `OriginalImageURL`, `MediaURL`, `Thumbnail`, `Ref` and `DisableNudge`, which describe
// the card or the link rather than saying an ad stands behind it.
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
		ad.GetContainsCtwaFlowsAutoReply() ||
		ad.GetAdContextPreviewDismissed() ||
		ad.GetWtwaWebsiteURL() != "" ||
		// The automated greeting strategies are read off the fields: zero is a value the
		// ad can set, and only an ad sets them at all.
		ad.AgmThumbnailStrategy != nil || ad.AgmTitleStrategy != nil ||
		ad.AgmSubtitleStrategy != nil || ad.AgmHeaderInteractionStrategy != nil
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
	if forwarded(context) {
		return nil
	}
	entry := protocol.EntryPoint{
		Source: context.GetEntryPointConversionSource(),
		App:    context.GetEntryPointConversionApp(),
	}
	if entry == (protocol.EntryPoint{}) {
		return nil
	}
	return &entry
}
