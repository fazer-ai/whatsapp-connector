package whatsmeow

import (
	"context"
	"time"

	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// What a customer sends a business from its catalog (#422).
//
// It used to fall through to the placeholder, so the agent had to open the phone to see
// what was bought. It is published as a card of kind `order`, because a client refuses a
// content type it does not know and would drop the message with it. The order itself
// rides inside the card, where a client that knows it reads it and one that does not
// shows the parts it already renders.
//
// The message carries WhatsApp's summary of the order. The items are not in it: they
// come from a query of their own, which a live order makes before it is published and a
// dump never does.

// orderDetailsWait bounds how long a live catalog order waits for its items. The query is
// one IQ that WhatsApp answers in well under a second, and every message behind this one
// waits with it.
const orderDetailsWait = 5 * time.Second

// orderCard is the card an order is published as, before anything is asked about it.
//
// The title and the body are the order's title and the customer's own words, as WhatsApp
// carried them. Nothing here writes words of its own into a card or formats an amount:
// the language and the currency's notation are the client's.
func orderCard(sent *waE2E.OrderMessage) protocol.RichContent {
	content := protocol.Rich(protocol.RichOrder)
	content.Title = sent.GetOrderTitle()
	content.Body = sent.GetMessage()
	order := &protocol.Order{
		ID:        sent.GetOrderID(),
		ItemCount: sent.ItemCount,
		Total1000: sent.TotalAmount1000,
		Currency:  sent.GetTotalCurrencyCode(),
		Seller:    sellerOf(sent.GetSellerJID()),
		Thumbnail: thumbnailOf("image/jpeg", sent.GetThumbnail()),
	}
	if sent.Status != nil {
		order.Status = sent.GetStatus().String()
	}
	if sent.Surface != nil {
		order.Surface = sent.GetSurface().String()
	}
	content.Order = order
	return content
}

// sellerOf is the selling account's number out of the address WhatsApp names it by.
func sellerOf(raw string) string {
	if raw == "" {
		return ""
	}
	jid, err := waTypes.ParseJID(raw)
	if err != nil {
		return ""
	}
	return jid.User
}

// orderBody is a live order, with its items when WhatsApp says what they are in time.
//
// The query is worth asking and not worth the order: one that fails or runs out of time
// leaves the order published as its summary, and nothing corrects it afterwards. The only
// correction the contract has is an edit, which a client shows as the customer having
// changed their message.
func (s *Session) orderBody(ctx context.Context, sent *waE2E.OrderMessage) (body, bool) {
	content := orderCard(sent)
	listed := false
	if id, token := sent.GetOrderID(), sent.GetToken(); id != "" && token != "" {
		ask, cancel := context.WithTimeout(ctx, s.orderWait)
		details, err := s.orderDetails(ask, s.current(), id, token)
		cancel()
		switch {
		case err != nil:
			s.log.Warn().Err(err).Str("order", id).Msg("publishing a catalog order without its items, which the order query did not return")
		case details != nil:
			listed = itemize(content.Order, details)
		}
	}
	// Which order and how big, and nothing of what was bought or what it cost: a support
	// log is read by people who are not the shop.
	s.log.Info().Str("order", sent.GetOrderID()).Int32("item_count", sent.GetItemCount()).Bool("items_listed", listed).
		Msg("publishing a catalog order")
	return body{content: content, context: sent.GetContextInfo()}, true
}

// itemize puts the items of an answered query on the order, and reports whether it
// listed any.
//
// The message's total is in thousandths, and the query's prices carry no unit at all.
// The query's own total is what proves the scale: equal to the message's, the prices are
// already thousandths, and a thousandth of it, they are whole units. A total that matches
// neither way leaves the prices off rather than showing an agent a figure off by a
// factor of a thousand. The items still go out, with their names and quantities.
func itemize(order *protocol.Order, details *waTypes.OrderDetails) bool {
	var scale int64
	switch total := details.Price.Total; {
	case order.Total1000 == nil || *order.Total1000 == 0:
	case total == *order.Total1000:
		scale = 1
	case total*1000 == *order.Total1000:
		scale = 1000
	}
	for _, product := range details.Products {
		item := protocol.OrderItem{
			ProductID: product.ID,
			Name:      product.Name,
			Quantity:  product.Quantity,
			Currency:  product.Currency,
			Variant:   product.VariantInfo.Properties,
		}
		if scale != 0 {
			price := product.Price * scale
			item.Price1000 = &price
		}
		order.Items = append(order.Items, item)
	}
	if scale != 0 && len(order.Items) > 0 {
		subtotal := details.Price.Subtotal * scale
		order.Subtotal1000 = &subtotal
	}
	return len(order.Items) > 0
}

// catalogOrderOf is the order a message carries, nil when it is not one.
func catalogOrderOf(event *waEvents.Message) *waE2E.OrderMessage {
	return event.Message.GetOrderMessage()
}
