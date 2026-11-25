package whatsmeow

import (
	"testing"

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

func TestAMessageFromAnAdCarriesTheAdAndWhereItRan(t *testing.T) {
	t.Parallel()

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
			MediaType:    waE2E.ContextInfo_ExternalAdReplyInfo_VIDEO.Enum(),
			ThumbnailURL: proto.String("https://scontent.whatsapp.net/v/t45/ad-thumbnail.jpg"),
		},
		EntryPointConversionSource: proto.String("ctwa_ad"),
		EntryPointConversionApp:    proto.String("instagram"),
	})

	message := messageMapOf(t, publishedBy(t, session, event))
	referral, ok := message["referral"].(map[string]any)
	if !ok {
		t.Fatalf("the message from the ad was published without the ad: %v", message)
	}
	for field, want := range map[string]any{
		"source_type": "ad", "source_id": "120210000000000001", "source_url": "https://fb.me/2AbCdEfGh",
		"ctwa_clid": "ARAkLkA8rmlFeiCktEJQ0000", "title": "Consultoria gratuita",
		"body": "Fale com a gente pelo WhatsApp", "media_type": "video",
		"thumbnail_url": "https://scontent.whatsapp.net/v/t45/ad-thumbnail.jpg",
	} {
		if referral[field] != want {
			t.Errorf("the ad's %s is %v, want %v", field, referral[field], want)
		}
	}
	entry, ok := message["entry_point"].(map[string]any)
	if !ok || entry["source"] != "ctwa_ad" || entry["app"] != "instagram" {
		t.Errorf("the entry point is %v, want ctwa_ad from instagram", message["entry_point"])
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

	for name, context := range map[string]*waE2E.ContextInfo{
		"an ad source": {ExternalAdReply: &waE2E.ContextInfo_ExternalAdReplyInfo{
			SourceType: proto.String("ad"), Title: proto.String("Consultoria gratuita"),
		}},
		"a post source": {ExternalAdReply: &waE2E.ContextInfo_ExternalAdReplyInfo{
			SourceType: proto.String("post"), Title: proto.String("Consultoria gratuita"),
		}},
		"an ad type": {ExternalAdReply: &waE2E.ContextInfo_ExternalAdReplyInfo{
			AdType: waE2E.ContextInfo_ExternalAdReplyInfo_CTWA.Enum(), Title: proto.String("Consultoria gratuita"),
		}},
		"the ctwa_ad entry point": {
			ExternalAdReply:            &waE2E.ContextInfo_ExternalAdReplyInfo{Title: proto.String("Consultoria gratuita")},
			EntryPointConversionSource: proto.String("ctwa_ad"),
		},
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
