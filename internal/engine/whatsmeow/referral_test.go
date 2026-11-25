package whatsmeow

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	waAICommon "go.mau.fi/whatsmeow/proto/waAICommon"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// adClick is the message WhatsApp delivers when somebody taps the button of a
// Click-to-WhatsApp ad: the prefilled text, with the ad and where it ran in its context.
func adClick(context *waE2E.ContextInfo) *waE2E.Message {
	return &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{
		Text:        proto.String("Olá! Quero mais informações sobre isso."),
		ContextInfo: context,
	}}
}

// The ad WhatsApp sends comes out as the contract's fixture says. The input is written
// out here and the fixture is only the expected output, so a field dropped from either
// side is a difference rather than two absences that agree.
func TestAMessageFromAnAdCarriesTheAdAndWhereItRan(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "contract", "fixtures", "events", "message_received_ctwa_referral.json"))
	if err != nil {
		t.Fatalf("read the fixture: %v", err)
	}
	var fixture struct {
		Payload struct {
			Message map[string]any `json:"message"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatalf("decode the fixture: %v", err)
	}

	session, _ := newTestSession(t, "5511999990001")
	event := textMessage("3EB0ADCLICK", "")
	event.Message = adClick(&waE2E.ContextInfo{
		ExternalAdReply: &waE2E.ContextInfo_ExternalAdReplyInfo{
			SourceType:   proto.String("ad"),
			SourceID:     proto.String("120210000000000001"),
			SourceURL:    proto.String("https://fb.me/2AbCdEfGh"),
			CtwaClid:     proto.String("ARAkLkA8rmlFeiCktEJQ0000"),
			Title:        proto.String("Consultoria gratuita"),
			Body:         proto.String("Fale com a gente pelo WhatsApp"),
			MediaType:    waE2E.ContextInfo_ExternalAdReplyInfo_IMAGE.Enum(),
			ThumbnailURL: proto.String("https://scontent.whatsapp.net/v/t45/ad-thumbnail.jpg"),
		},
		EntryPointConversionSource: proto.String("ctwa_ad"),
		EntryPointConversionApp:    proto.String("instagram"),
	})

	message := messageMapOf(t, publishedBy(t, session, event))
	for _, field := range []string{"referral", "entry_point"} {
		want := fixture.Payload.Message[field]
		if want == nil {
			t.Fatalf("the fixture has no %s to compare with", field)
		}
		if !reflect.DeepEqual(message[field], want) {
			t.Errorf("the %s is %v, want the fixture's %v", field, message[field], want)
		}
	}
}

// What the sender wrote into a URL field is kept only when it is a web address. The
// rest of the ad is kept either way.
func TestAnAdKeepsOnlyWebAddressesInItsURLs(t *testing.T) {
	t.Parallel()

	for _, bad := range []string{
		"data:image/jpeg;base64," + strings.Repeat("A", 64*1024),
		"javascript:alert(1)",
		"/relative/path.jpg",
		"https://",
		"https://:443/ad.jpg",
		"https://example.com:99999/ad.jpg",
		"https://example.com:0/ad.jpg",
		"https://example.com:/ad.jpg",
		"ftp://example.com/ad.jpg",
	} {
		event := textMessage("3EB0BADURL", "")
		event.Message = adClick(&waE2E.ContextInfo{ExternalAdReply: &waE2E.ContextInfo_ExternalAdReplyInfo{
			CtwaClid: proto.String("ARAk"), SourceURL: proto.String(bad), ThumbnailURL: proto.String(bad),
		}})
		message, _, ok := plain(t).inboundOf(event, plainBody)
		if !ok {
			t.Fatal("the message from the ad was refused")
		}
		if message.Referral == nil || *message.Referral != (protocol.Referral{CtwaClid: "ARAk"}) {
			t.Errorf("with %.40q the ad is %+v, want the click id and no URL", bad, message.Referral)
		}
	}

	event := textMessage("3EB0GOODURL", "")
	event.Message = adClick(&waE2E.ContextInfo{ExternalAdReply: &waE2E.ContextInfo_ExternalAdReplyInfo{
		CtwaClid: proto.String("ARAk"), SourceURL: proto.String("http://fb.me/x"),
		ThumbnailURL: proto.String("https://scontent.whatsapp.net/t.jpg"),
	}})
	message, _, _ := plain(t).inboundOf(event, plainBody)
	if message.Referral == nil || message.Referral.SourceURL != "http://fb.me/x" ||
		message.Referral.ThumbnailURL != "https://scontent.whatsapp.net/t.jpg" {
		t.Errorf("the ad is %+v, want both web addresses kept", message.Referral)
	}
}

// The fixture's ad is an image, so the other creative is pinned here.
func TestAVideoAdSaysVideo(t *testing.T) {
	t.Parallel()

	event := textMessage("3EB0ADVIDEO", "")
	event.Message = adClick(&waE2E.ContextInfo{ExternalAdReply: &waE2E.ContextInfo_ExternalAdReplyInfo{
		CtwaClid:  proto.String("ARAkLkA8rmlFeiCktEJQ0002"),
		MediaType: waE2E.ContextInfo_ExternalAdReplyInfo_VIDEO.Enum(),
	}})

	message, _, ok := plain(t).inboundOf(event, plainBody)
	if !ok {
		t.Fatal("the message from the ad was refused")
	}
	if message.Referral == nil || message.Referral.MediaType != protocol.AdMediaVideo {
		t.Fatalf("the ad is %+v, want a video", message.Referral)
	}
}

// A quoted ad, the older way WhatsApp attaches one, fills in what the card left out.
func TestAQuotedAdFillsInTheCreativeAndTheCaption(t *testing.T) {
	t.Parallel()

	for name, context := range map[string]*waE2E.ContextInfo{
		"with no card": {QuotedAd: &waE2E.ContextInfo_AdReplyInfo{
			MediaType: waE2E.ContextInfo_AdReplyInfo_VIDEO.Enum(), Caption: proto.String("Fale com a gente"),
		}},
		"with a card that left them out": {
			ExternalAdReply: &waE2E.ContextInfo_ExternalAdReplyInfo{CtwaClid: proto.String("ARAk")},
			QuotedAd: &waE2E.ContextInfo_AdReplyInfo{
				MediaType: waE2E.ContextInfo_AdReplyInfo_VIDEO.Enum(), Caption: proto.String("Fale com a gente"),
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			event := textMessage("3EB0QUOTEDAD", "")
			event.Message = adClick(context)
			message, _, ok := plain(t).inboundOf(event, plainBody)
			if !ok {
				t.Fatal("the message from the ad was refused")
			}
			if message.Referral == nil || message.Referral.MediaType != protocol.AdMediaVideo ||
				message.Referral.Body != "Fale com a gente" {
				t.Fatalf("the ad is %+v, want the quoted ad's video and caption", message.Referral)
			}
		})
	}

	// A card that says NONE has said its ad has no creative.
	none := textMessage("3EB0NONE", "")
	none.Message = adClick(&waE2E.ContextInfo{
		ExternalAdReply: &waE2E.ContextInfo_ExternalAdReplyInfo{
			CtwaClid: proto.String("ARAk"), MediaType: waE2E.ContextInfo_ExternalAdReplyInfo_NONE.Enum(),
		},
		QuotedAd: &waE2E.ContextInfo_AdReplyInfo{MediaType: waE2E.ContextInfo_AdReplyInfo_VIDEO.Enum()},
	})
	if message, _, _ := plain(t).inboundOf(none, plainBody); message.Referral == nil || message.Referral.MediaType != "" {
		t.Errorf("the ad is %+v, want no creative, as the card said", message.Referral)
	}

	// The card's own values win over the quoted ad's.
	event := textMessage("3EB0BOTH", "")
	event.Message = adClick(&waE2E.ContextInfo{
		ExternalAdReply: &waE2E.ContextInfo_ExternalAdReplyInfo{
			CtwaClid: proto.String("ARAk"), Body: proto.String("Do card"),
			MediaType: waE2E.ContextInfo_ExternalAdReplyInfo_IMAGE.Enum(),
		},
		QuotedAd: &waE2E.ContextInfo_AdReplyInfo{
			MediaType: waE2E.ContextInfo_AdReplyInfo_VIDEO.Enum(), Caption: proto.String("Do anúncio citado"),
		},
	})
	message, _, _ := plain(t).inboundOf(event, plainBody)
	if message.Referral == nil || message.Referral.MediaType != protocol.AdMediaImage || message.Referral.Body != "Do card" {
		t.Errorf("the ad is %+v, want the card's image and body", message.Referral)
	}
}

// What WhatsApp leaves out is left out here too, and an ad with no creative says NONE,
// which is not a kind of media.
func TestAnAdWithPartsMissingPublishesWhatItHas(t *testing.T) {
	t.Parallel()

	event := textMessage("3EB0ADBARE", "")
	event.Message = adClick(&waE2E.ContextInfo{
		ExternalAdReply: &waE2E.ContextInfo_ExternalAdReplyInfo{
			CtwaClid:  proto.String("ARAkLkA8rmlFeiCktEJQ0001"),
			MediaType: waE2E.ContextInfo_ExternalAdReplyInfo_NONE.Enum(),
		},
	})

	message, _, ok := plain(t).inboundOf(event, plainBody)
	if !ok {
		t.Fatal("the message from the ad was refused")
	}
	want := protocol.Referral{CtwaClid: "ARAkLkA8rmlFeiCktEJQ0001"}
	if message.Referral == nil || *message.Referral != want {
		t.Fatalf("the ad is %+v, want only its click id", message.Referral)
	}
	if message.EntryPoint != nil {
		t.Errorf("an entry point nobody reported was published: %+v", message.EntryPoint)
	}
	validateInboundAgainstContract(t, &message)
}

// A wa.me link reports where the conversation started with no ad behind it.
func TestAMessageFromAClickToChatLinkCarriesTheEntryPointAlone(t *testing.T) {
	t.Parallel()

	event := textMessage("3EB0WAME", "")
	event.Message = adClick(&waE2E.ContextInfo{
		EntryPointConversionSource: proto.String("click_to_chat_link"),
	})

	message, _, ok := plain(t).inboundOf(event, plainBody)
	if !ok {
		t.Fatal("the message from the link was refused")
	}
	if message.Referral != nil {
		t.Errorf("a message with no ad was published with one: %+v", message.Referral)
	}
	if message.EntryPoint == nil || *message.EntryPoint != (protocol.EntryPoint{Source: "click_to_chat_link"}) {
		t.Errorf("the entry point is %+v, want click_to_chat_link", message.EntryPoint)
	}
	validateInboundAgainstContract(t, &message)
}

func TestAnOrdinaryMessageCarriesNoAdNorEntryPoint(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	message := messageMapOf(t, publishedBy(t, session, textMessage("3EB0PLAIN", "bom dia")))
	for _, field := range []string{"referral", "entry_point"} {
		if value, there := message[field]; there {
			t.Errorf("an ordinary message was published with %s %v", field, value)
		}
	}
}

// A link preview is drawn with the same external ad reply as an ad, and a client that
// keeps the first referral as the campaign behind the contact must not be handed one.
func TestALinkPreviewWithNothingSayingItIsAnAdCarriesNoReferral(t *testing.T) {
	t.Parallel()

	event := textMessage("3EB0PREVIEW", "")
	event.Message = adClick(&waE2E.ContextInfo{
		ExternalAdReply: &waE2E.ContextInfo_ExternalAdReplyInfo{
			Title:        proto.String("Nosso cardápio"),
			Body:         proto.String("Veja os pratos da semana"),
			SourceURL:    proto.String("https://example.com/cardapio"),
			ThumbnailURL: proto.String("https://example.com/cardapio.jpg"),
			MediaType:    waE2E.ContextInfo_ExternalAdReplyInfo_IMAGE.Enum(),
		},
	})

	message, _, ok := plain(t).inboundOf(event, plainBody)
	if !ok {
		t.Fatal("the message with a preview was refused")
	}
	if message.Referral != nil {
		t.Errorf("a link preview was published as an ad: %+v", message.Referral)
	}
}

// Any one of the signals is enough: WhatsApp does not send all of them every time.
func TestAnAdIsRecognisedByAnyOfItsSignals(t *testing.T) {
	t.Parallel()

	card := func(set func(*waE2E.ContextInfo_ExternalAdReplyInfo)) *waE2E.ContextInfo {
		ad := &waE2E.ContextInfo_ExternalAdReplyInfo{Title: proto.String("Consultoria gratuita")}
		set(ad)
		return &waE2E.ContextInfo{ExternalAdReply: ad}
	}
	around := func(set func(*waE2E.ContextInfo)) *waE2E.ContextInfo {
		context := &waE2E.ContextInfo{
			ExternalAdReply: &waE2E.ContextInfo_ExternalAdReplyInfo{Title: proto.String("Consultoria gratuita")},
		}
		set(context)
		return context
	}
	for name, context := range map[string]*waE2E.ContextInfo{
		"a click id": card(func(ad *waE2E.ContextInfo_ExternalAdReplyInfo) { ad.CtwaClid = proto.String("ARAk") }),
		"an ad type": card(func(ad *waE2E.ContextInfo_ExternalAdReplyInfo) {
			ad.AdType = waE2E.ContextInfo_ExternalAdReplyInfo_CTWA.Enum()
		}),
		"an ad source":  card(func(ad *waE2E.ContextInfo_ExternalAdReplyInfo) { ad.SourceType = proto.String("ad") }),
		"a post source": card(func(ad *waE2E.ContextInfo_ExternalAdReplyInfo) { ad.SourceType = proto.String("post") }),
		"the ad attribution flag": card(func(ad *waE2E.ContextInfo_ExternalAdReplyInfo) {
			ad.ShowAdAttribution = proto.Bool(true)
		}),
		"a call ad": card(func(ad *waE2E.ContextInfo_ExternalAdReplyInfo) { ad.ClickToWhatsappCall = proto.Bool(true) }),
		"the ad's greeting shown": card(func(ad *waE2E.ContextInfo_ExternalAdReplyInfo) {
			ad.AutomatedGreetingMessageShown = proto.Bool(true)
		}),
		"the ad's greeting": card(func(ad *waE2E.ContextInfo_ExternalAdReplyInfo) {
			ad.GreetingMessageBody = proto.String("Olá! Como podemos ajudar?")
		}),
		"the greeting's call to action": card(func(ad *waE2E.ContextInfo_ExternalAdReplyInfo) {
			ad.AutomatedGreetingMessageCtaType = proto.String("quick_reply")
		}),
		"a call-to-action payload": card(func(ad *waE2E.ContextInfo_ExternalAdReplyInfo) { ad.CtaPayload = proto.String("p") }),
		"a website ad format":      card(func(ad *waE2E.ContextInfo_ExternalAdReplyInfo) { ad.WtwaAdFormat = proto.Bool(true) }),
		"an ad preview": card(func(ad *waE2E.ContextInfo_ExternalAdReplyInfo) {
			ad.AdPreviewURL = proto.String("https://fb.me/preview")
		}),
		"a flows auto reply": card(func(ad *waE2E.ContextInfo_ExternalAdReplyInfo) {
			ad.ContainsCtwaFlowsAutoReply = proto.Bool(true)
		}),
		"a dismissed ad preview": card(func(ad *waE2E.ContextInfo_ExternalAdReplyInfo) {
			ad.AdContextPreviewDismissed = proto.Bool(true)
		}),
		"a website ad": card(func(ad *waE2E.ContextInfo_ExternalAdReplyInfo) {
			ad.WtwaWebsiteURL = proto.String("https://example.com")
		}),
		"a greeting thumbnail strategy": card(func(ad *waE2E.ContextInfo_ExternalAdReplyInfo) {
			ad.AgmThumbnailStrategy = proto.Int32(0)
		}),
		"a greeting title strategy": card(func(ad *waE2E.ContextInfo_ExternalAdReplyInfo) {
			ad.AgmTitleStrategy = proto.Int32(0)
		}),
		"a greeting subtitle strategy": card(func(ad *waE2E.ContextInfo_ExternalAdReplyInfo) {
			ad.AgmSubtitleStrategy = proto.Int32(0)
		}),
		"a greeting header strategy": card(func(ad *waE2E.ContextInfo_ExternalAdReplyInfo) {
			ad.AgmHeaderInteractionStrategy = proto.Int32(0)
		}),
		"the ctwa_ad entry point": around(func(c *waE2E.ContextInfo) {
			c.EntryPointConversionSource = proto.String("ctwa_ad")
		}),
		"the FB_Ads conversion source": around(func(c *waE2E.ContextInfo) { c.ConversionSource = proto.String("FB_Ads") }),
		"the context's ad attribution flag": around(func(c *waE2E.ContextInfo) {
			c.AlwaysShowAdAttribution = proto.Bool(true)
		}),
		"ctwa signals":   around(func(c *waE2E.ContextInfo) { c.CtwaSignals = proto.String("s") }),
		"a ctwa payload": around(func(c *waE2E.ContextInfo) { c.CtwaPayload = []byte{1} }),
		"a quoted ad":    around(func(c *waE2E.ContextInfo) { c.QuotedAd = &waE2E.ContextInfo_AdReplyInfo{} }),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			event := textMessage("3EB0SIGNAL", "")
			event.Message = adClick(context)
			message, _, ok := plain(t).inboundOf(event, plainBody)
			if !ok {
				t.Fatal("the message from the ad was refused")
			}
			if message.Referral == nil || message.Referral.Title != "Consultoria gratuita" {
				t.Errorf("the ad is %+v, want the one WhatsApp sent", message.Referral)
			}
		})
	}
}

// WhatsApp can say the message came from an ad and send no card for it.
func TestAnAdWithNoCardIsStillAReferral(t *testing.T) {
	t.Parallel()

	event := textMessage("3EB0NOCARD", "")
	event.Message = adClick(&waE2E.ContextInfo{ConversionSource: proto.String("FB_Ads")})

	message, _, ok := plain(t).inboundOf(event, plainBody)
	if !ok {
		t.Fatal("the message from the ad was refused")
	}
	if message.Referral == nil || *message.Referral != (protocol.Referral{}) {
		t.Fatalf("the ad is %+v, want an empty referral", message.Referral)
	}
	validateInboundAgainstContract(t, &message)
}

func TestAnEntryPointWithOnlyTheAppKeepsTheApp(t *testing.T) {
	t.Parallel()

	event := textMessage("3EB0APPONLY", "")
	event.Message = adClick(&waE2E.ContextInfo{EntryPointConversionApp: proto.String("instagram")})

	message, _, ok := plain(t).inboundOf(event, plainBody)
	if !ok {
		t.Fatal("the message was refused")
	}
	if message.EntryPoint == nil || *message.EntryPoint != (protocol.EntryPoint{App: "instagram"}) {
		t.Errorf("the entry point is %+v, want the app alone", message.EntryPoint)
	}
	validateInboundAgainstContract(t, &message)
}

// That the message came from an ad is the attribution, even when WhatsApp sent nothing
// else about the ad.
func TestAnAdWithNothingReadableIsStillAReferral(t *testing.T) {
	t.Parallel()

	event := textMessage("3EB0ADEMPTY", "")
	event.Message = adClick(&waE2E.ContextInfo{ExternalAdReply: &waE2E.ContextInfo_ExternalAdReplyInfo{
		AdType: waE2E.ContextInfo_ExternalAdReplyInfo_CTWA.Enum(),
	}})

	message, _, ok := plain(t).inboundOf(event, plainBody)
	if !ok {
		t.Fatal("the message from the ad was refused")
	}
	if message.Referral == nil || *message.Referral != (protocol.Referral{}) {
		t.Fatalf("the ad is %+v, want an empty referral", message.Referral)
	}
	validateInboundAgainstContract(t, &message)
}

// A forward keeps the context of what it copies, and that says how the original
// conversation started, not this one.
func TestAForwardedAdCarriesNoAttribution(t *testing.T) {
	t.Parallel()

	for name, context := range map[string]*waE2E.ContextInfo{
		"flagged as forwarded":    {IsForwarded: proto.Bool(true)},
		"with a forwarding score": {ForwardingScore: proto.Uint32(1)},
		"from a channel": {
			ForwardedNewsletterMessageInfo: &waE2E.ContextInfo_ForwardedNewsletterMessageInfo{},
		},
		"from a business": {BusinessMessageForwardInfo: &waE2E.ContextInfo_BusinessMessageForwardInfo{}},
		"from Meta AI":    {ForwardedAiBotMessageInfo: &waAICommon.ForwardedAIBotMessageInfo{}},
		"with an origin":  {ForwardOrigin: waE2E.ContextInfo_UNKNOWN.Enum()},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			context.ExternalAdReply = &waE2E.ContextInfo_ExternalAdReplyInfo{CtwaClid: proto.String("ARAk")}
			context.EntryPointConversionSource = proto.String("ctwa_ad")
			context.EntryPointConversionApp = proto.String("instagram")
			event := textMessage("3EB0FORWARD", "")
			event.Message = adClick(context)
			message, _, ok := plain(t).inboundOf(event, plainBody)
			if !ok {
				t.Fatal("the forwarded message was refused")
			}
			if message.Referral != nil || message.EntryPoint != nil {
				t.Errorf("a forward was published with %+v and %+v", message.Referral, message.EntryPoint)
			}
		})
	}
}
