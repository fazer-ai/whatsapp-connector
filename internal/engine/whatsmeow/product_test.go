package whatsmeow

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"strings"
	"testing"

	wm "go.mau.fi/whatsmeow"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waHistorySync "go.mau.fi/whatsmeow/proto/waHistorySync"
	"google.golang.org/protobuf/proto"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/media"
)

// A product from a business's catalog reaches a chat two ways: the business sends it on
// its own, or the customer asks about it from the catalog ("Message business") and the
// question arrives quoting it. WhatsApp shows the product both times, with its name, price
// and picture; the connector published the first as a placeholder and the second as a
// bare question, so the agent could not tell what was being asked about (#426).
//
// The shapes below are the ones measured live on 17/11/2026, with the numbers and keys
// replaced.

// productThumbnail is the inline JPEG preview WhatsApp carries for a product.
var productThumbnail = []byte("\xff\xd8\xff\xe0 miniatura do produto")

func catalogProduct() *waE2E.ProductMessage_ProductSnapshot {
	return &waE2E.ProductMessage_ProductSnapshot{
		ProductID:         proto.String("29548935431457387"),
		Title:             proto.String("Produto teste"),
		Description:       proto.String("Item de teste, não comprar"),
		CurrencyCode:      proto.String("BRL"),
		PriceAmount1000:   proto.Int64(12500),
		ProductImageCount: proto.Uint32(1),
		ProductImage: &waE2E.ImageMessage{
			Mimetype: proto.String("image/jpeg"), Width: proto.Uint32(400), Height: proto.Uint32(400),
			DirectPath: proto.String(directPath), MediaKey: []byte("key"), FileEncSHA256: encSHA256(),
			JPEGThumbnail: productThumbnail,
		},
	}
}

// productInquiry is the customer's question asked from the catalog, quoting the product.
func productInquiry(snapshot *waE2E.ProductMessage_ProductSnapshot, owner string) *waE2E.Message {
	quoted := &waE2E.ProductMessage{Product: snapshot}
	if owner != "" {
		quoted.BusinessOwnerJID = proto.String(owner)
	}
	return &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{
		Text: proto.String("Qual o prazo de entrega desse?"),
		ContextInfo: &waE2E.ContextInfo{
			StanzaID:      proto.String("product_inquiry_3EB07F41BAB4D0182DD7"),
			Participant:   proto.String("100000000000001@lid"),
			QuotedMessage: &waE2E.Message{ProductMessage: quoted},
		},
	}}
}

// sentProduct is a product the business sent into the chat on its own.
func sentProduct(snapshot *waE2E.ProductMessage_ProductSnapshot, owner string) *waE2E.Message {
	sent := &waE2E.ProductMessage{Product: snapshot}
	if owner != "" {
		sent.BusinessOwnerJID = proto.String(owner)
	}
	return &waE2E.Message{ProductMessage: sent}
}

// messageMapOf is the published message as a client decodes it, with the fields this
// build may not know about still in it.
func messageMapOf(t *testing.T, emission *engine.Emission) map[string]any {
	t.Helper()

	inboundContentOf(t, emission) // also validates the frame against the contract
	message, _ := decode(t, emission.Payload)["message"].(map[string]any)
	return message
}

func TestAQuestionFromTheCatalogCarriesTheProductItQuotes(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	event := textMessage("3EB0INQUIRY", "")
	event.Message = productInquiry(catalogProduct(), "100000000000001@lid")

	emission := publishedBy(t, session, event)
	message := messageMapOf(t, emission)
	content, _ := message["content"].(map[string]any)
	if content["type"] != "text" || content["body"] != "Qual o prazo de entrega desse?" {
		t.Fatalf("the question was published as %v", content)
	}
	if message["quoted_id"] != "product_inquiry_3EB07F41BAB4D0182DD7" {
		t.Fatalf("the question quotes %v", message["quoted_id"])
	}
	product, ok := message["quoted_product"].(map[string]any)
	if !ok {
		t.Fatalf("the question was published without the product it quotes: %v", message)
	}
	for field, want := range map[string]any{
		"id": "29548935431457387", "title": "Produto teste", "description": "Item de teste, não comprar",
		"currency": "BRL", "price_1000": float64(12500), "image_count": float64(1),
	} {
		if product[field] != want {
			t.Errorf("the quoted product's %s is %v, want %v", field, product[field], want)
		}
	}
	owner, _ := product["owner"].(map[string]any)
	if owner["kind"] != "lid" || owner["id"] != "100000000000001" {
		t.Errorf("the quoted product's owner is %v, want the lid 100000000000001", owner)
	}
	thumbnail, _ := product["thumbnail"].(string)
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(thumbnail, "data:image/jpeg;base64,"))
	if err != nil || !bytes.Equal(raw, productThumbnail) {
		t.Errorf("the quoted product's thumbnail is %q, want the preview WhatsApp carried", thumbnail)
	}
}

// What WhatsApp leaves out is left out here too: a price nobody gave is not a price of
// zero, and a currency nobody named is not the real.
func TestAQuotedProductWithPartsMissingPublishesWhatItHas(t *testing.T) {
	t.Parallel()

	snapshot := catalogProduct()
	snapshot.PriceAmount1000, snapshot.CurrencyCode, snapshot.ProductImage = nil, nil, nil

	session, _ := newTestSession(t, "5511999990001")
	event := textMessage("3EB0PARTIAL", "")
	event.Message = productInquiry(snapshot, "")

	product, ok := messageMapOf(t, publishedBy(t, session, event))["quoted_product"].(map[string]any)
	if !ok {
		t.Fatal("the question was published without the product it quotes")
	}
	if product["title"] != "Produto teste" || product["id"] != "29548935431457387" {
		t.Errorf("the quoted product is %v", product)
	}
	for _, absent := range []string{"price_1000", "currency", "thumbnail", "owner"} {
		if value, there := product[absent]; there {
			t.Errorf("the quoted product carries %s = %v, which WhatsApp did not send", absent, value)
		}
	}
}

// A quote that names a product but carries nothing readable about it still says so: the
// agent is told there was a product to look at on the phone, instead of a question about
// nothing.
func TestAQuotedProductWithNothingReadableStillSaysThereIsOne(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	event := textMessage("3EB0BLANK", "")
	event.Message = productInquiry(&waE2E.ProductMessage_ProductSnapshot{}, "")

	message := messageMapOf(t, publishedBy(t, session, event))
	product, ok := message["quoted_product"].(map[string]any)
	if !ok {
		t.Fatalf("a quote of an unreadable product was published as an ordinary question: %v", message)
	}
	if len(product) != 0 {
		t.Errorf("the unreadable product was published as %v, with values WhatsApp did not send", product)
	}
}

// An answer to an ordinary message is not about a product.
func TestAnOrdinaryReplyCarriesNoProduct(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	event := textMessage("3EB0REPLY", "")
	event.Message = &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{
		Text: proto.String("sim, pode ser"),
		ContextInfo: &waE2E.ContextInfo{
			StanzaID:      proto.String("3EB0A1B2C3D4E5F60718"),
			QuotedMessage: &waE2E.Message{Conversation: proto.String("Pode ser amanhã?")},
		},
	}}

	if product, there := messageMapOf(t, publishedBy(t, session, event))["quoted_product"]; there {
		t.Fatalf("an ordinary reply carries a product: %v", product)
	}
}

// A product sent on its own is a card, the way WhatsApp shows it: the picture on top, the
// name, the description, and the product itself for a client that shows the price.
func TestAProductSentOnItsOwnIsACardWithItsPicture(t *testing.T) {
	t.Parallel()

	file := []byte("a foto do produto")
	session, downloads := mediaSession(t, media.Options{})
	downloads.answer(file, nil)
	event := mediaEvent("3EB0PRODUCT", sentProduct(catalogProduct(), "5511999990001@s.whatsapp.net"))

	emissions, acknowledged := deliver(t, session, event, 1)
	if !acknowledged {
		t.Fatal("a product whose picture was stored was left unacknowledged")
	}
	content := inboundContentOf(t, emissions[0])
	if content["type"] != "rich" || content["kind"] != "product" {
		t.Fatalf("the product was published as %v", content)
	}
	if content["title"] != "Produto teste" || content["body"] != "Item de teste, não comprar" {
		t.Errorf("the product card reads %q / %q", content["title"], content["body"])
	}
	product, _ := content["product"].(map[string]any)
	for field, want := range map[string]any{
		"id": "29548935431457387", "currency": "BRL", "price_1000": float64(12500), "image_count": float64(1),
	} {
		if product[field] != want {
			t.Errorf("the product's %s is %v, want %v", field, product[field], want)
		}
	}
	owner, _ := product["owner"].(map[string]any)
	if owner["kind"] != "phone" || owner["id"] != "5511999990001" {
		t.Errorf("the product's owner is %v, want the number 5511999990001", owner)
	}

	picture := richMediaOf(t, emissions[0])
	if picture == nil || picture.Ref == nil {
		t.Fatalf("the product's picture was published as %+v, want it with a reference", picture)
	}
	body, _, err := session.blobs.(*media.Store).Open(picture.Ref.ID)
	if err != nil {
		t.Fatalf("the reference names a blob the store does not have: %v", err)
	}
	defer func() { _ = body.Close() }()
	if kept, _ := io.ReadAll(body); !bytes.Equal(kept, file) {
		t.Fatalf("the blob holds %q, want %q", kept, file)
	}
}

// A product with nothing readable in it is still a product, not a message of a type this
// build does not know: the agent learns there is one to look at on the phone.
func TestAProductWithNothingReadableIsStillAProduct(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	event := textMessage("3EB0EMPTY", "")
	event.Message = sentProduct(&waE2E.ProductMessage_ProductSnapshot{}, "")

	content := inboundContentOf(t, publishedBy(t, session, event))
	if content["type"] != "rich" || content["kind"] != "product" {
		t.Fatalf("an unreadable product was published as %v", content)
	}
	product, ok := content["product"].(map[string]any)
	if !ok || len(product) != 0 {
		t.Errorf("the unreadable product was published as %v, want an empty product", content["product"])
	}
	for _, absent := range []string{"title", "body", "media"} {
		if value, there := content[absent]; there {
			t.Errorf("the unreadable product carries %s = %v", absent, value)
		}
	}
}

// Out of a history dump the product is the same card, with its picture kept to be fetched
// later rather than downloaded while the dump goes out, and a question quoting one keeps
// the product it quotes.
func TestProductsFromADumpGoOutLikeLiveOnes(t *testing.T) {
	t.Parallel()

	session, _ := mediaSession(t, media.Options{})
	session.setHistory(true)
	var fetched int
	session.download = func(context.Context, *wm.Client, wm.DownloadableMessage, media.File) error {
		fetched++
		return nil
	}
	chat := "5511999990002@s.whatsapp.net"
	bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_ON_DEMAND,
		&waHistorySync.Conversation{ID: proto.String(chat), Messages: []*waHistorySync.HistorySyncMsg{
			pastMessage(chat, "3EB0PASTQ", 1754000000, false, productInquiry(catalogProduct(), "100000000000001@lid")),
			pastMessage(chat, "3EB0PASTP", 1754000001, true, sentProduct(catalogProduct(), "5511999990001@s.whatsapp.net")),
		}})}
	bench.install(session)

	acknowledged := make(chan bool, 1)
	go func() {
		acknowledged <- session.receive(historyNotification("NOTIFPRODUCT", waE2E.HistorySyncType_ON_DEMAND))
	}()
	slices, got := slicesUntil(t, session, acknowledged)
	if !got || len(slices) != 1 || len(slices[0].Messages) != 2 {
		t.Fatalf("published %+v (acknowledged %v), want one slice with both messages", slices, got)
	}
	raw, err := json.Marshal(slices[0].Messages)
	if err != nil {
		t.Fatalf("marshal the slice: %v", err)
	}
	var messages []map[string]any
	if err := json.Unmarshal(raw, &messages); err != nil {
		t.Fatalf("decode the slice: %v", err)
	}
	quoted, _ := messages[0]["quoted_product"].(map[string]any)
	if quoted["title"] != "Produto teste" || quoted["price_1000"] != float64(12500) {
		t.Errorf("the question from the dump quotes %v", messages[0]["quoted_product"])
	}
	content, _ := messages[1]["content"].(map[string]any)
	if content["type"] != "rich" || content["kind"] != "product" || content["title"] != "Produto teste" {
		t.Fatalf("the product from the dump went out as %v", content)
	}
	picture, _ := content["media"].(map[string]any)
	if picture == nil || picture["ref"] != nil {
		t.Errorf("the product's picture from the dump went out as %v, want it with no reference", picture)
	}
	if fetched != 0 {
		t.Errorf("the picture was downloaded %d times while the dump was published", fetched)
	}
}

// WhatsApp may send a product with no snapshot at all, only the account whose catalog it
// is from. That is still a product to look up, with its owner, and never a crash.
func TestAProductWithNoSnapshotIsStillAProductWithItsOwner(t *testing.T) {
	t.Parallel()

	t.Run("sent on its own", func(t *testing.T) {
		t.Parallel()

		session, _ := newTestSession(t, "5511999990001")
		event := textMessage("3EB0NOSNAP", "")
		event.Message = &waE2E.Message{ProductMessage: &waE2E.ProductMessage{BusinessOwnerJID: proto.String("5511999990001@s.whatsapp.net")}}

		content := inboundContentOf(t, publishedBy(t, session, event))
		product, _ := content["product"].(map[string]any)
		owner, _ := product["owner"].(map[string]any)
		if content["kind"] != "product" || owner["id"] != "5511999990001" || len(product) != 1 {
			t.Fatalf("a product with no snapshot was published as %v", content)
		}
	})
	t.Run("quoted", func(t *testing.T) {
		t.Parallel()

		session, _ := newTestSession(t, "5511999990001")
		event := textMessage("3EB0NOSNAPQ", "")
		event.Message = productInquiry(nil, "100000000000001@lid")

		product, _ := messageMapOf(t, publishedBy(t, session, event))["quoted_product"].(map[string]any)
		owner, _ := product["owner"].(map[string]any)
		if owner["id"] != "100000000000001" || len(product) != 1 {
			t.Fatalf("a quoted product with no snapshot was published as %v", product)
		}
	})
}
