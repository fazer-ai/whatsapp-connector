package whatsmeow

import (
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// A product from a business's catalog reaches a chat two ways, and WhatsApp shows the
// product both times (#426):
//
//   - sent on its own, by the business sharing an item of its catalog, as a
//     `productMessage`: published as a card of kind `product`, with the picture on top;
//   - quoted, by the customer asking about an item from the catalog ("Message
//     business"): the question is plain text whose context quotes a `productMessage`
//     under an id no message has (`product_inquiry_...`), so the quote alone points at
//     nothing. The product is published beside the quote, in `quoted_product`.
//
// Both were measured live on 17/11/2026: the snapshot carries the id, name, description,
// price in thousandths, currency, picture count and an inline preview; the one sent on its
// own also carries the picture itself, downloadable like any image.

// productOf renders a catalog product the way the contract carries it, with only what
// WhatsApp sent: a product with nothing readable in it is an empty one, which still tells
// a client there is a product to look up on the phone.
func productOf(sent *waE2E.ProductMessage) *protocol.Product {
	snapshot := sent.GetProduct()
	product := &protocol.Product{
		ID:          snapshot.GetProductID(),
		Title:       snapshot.GetTitle(),
		Description: snapshot.GetDescription(),
		Currency:    snapshot.GetCurrencyCode(),
		RetailerID:  snapshot.GetRetailerID(),
		URL:         snapshot.GetURL(),
	}
	if snapshot.PriceAmount1000 != nil {
		price := snapshot.GetPriceAmount1000()
		product.Price1000 = &price
	}
	if snapshot.SalePriceAmount1000 != nil {
		price := snapshot.GetSalePriceAmount1000()
		product.SalePrice1000 = &price
	}
	if snapshot.ProductImageCount != nil {
		count := snapshot.GetProductImageCount()
		product.ImageCount = &count
	}
	if owner, err := waTypes.ParseJID(sent.GetBusinessOwnerJID()); err == nil && sent.GetBusinessOwnerJID() != "" {
		if address, ok := addressOf(owner); ok {
			product.Owner = &address
		}
	}
	if image := snapshot.GetProductImage(); image != nil {
		product.Thumbnail = thumbnailOf("image/jpeg", image.GetJPEGThumbnail())
	}
	return product
}

// productCard is a product sent on its own, as the card WhatsApp shows: the picture, the
// name, the description.
//
// The picture goes through the renderer every image does, so it is fetched, kept and
// offered for download the same way. The inline preview stays in the product, where a
// quoted one has nothing else to show.
func productCard(sent *waE2E.ProductMessage) card {
	content := protocol.Rich(protocol.RichProduct)
	content.Product = productOf(sent)
	content.Title = content.Product.Title
	content.Body = content.Product.Description
	c := card{content: content, context: sent.GetContextInfo()}
	if image := sent.GetProduct().GetProductImage(); image != nil {
		c.header = headerOf(&waE2E.Message{ImageMessage: image})
	}
	return c
}

// quotedProductOf is the product a message quotes, nil when the quote is not a product.
func quotedProductOf(context *waE2E.ContextInfo) *protocol.Product {
	quoted := context.GetQuotedMessage().GetProductMessage()
	if quoted == nil {
		return nil
	}
	return productOf(quoted)
}
