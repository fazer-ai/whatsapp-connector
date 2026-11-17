package whatsmeow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"
	"golang.org/x/text/currency"
	"golang.org/x/text/language"
	"golang.org/x/text/message"

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
// The message carries WhatsApp's summary of the order. The items are not in it: they come
// from WhatsApp's catalog API, which a live order asks before it is published and a dump
// never does.

// The query WhatsApp Web Business makes for an order's items, measured on 17/11/2026 by
// opening "View order request" on a seller account paired as a linked device. It is
// HTTPS to the catalog API and not an IQ on the socket: whatsmeow's GetOrderDetails IQ,
// tried on the same account and the same order, answered 501 feature-not-implemented.
//
// The document id names the GraphQL query and the access token is the WhatsApp Web app's
// own public client token, the same one the page sends with its other catalog calls. No
// cookie and no account credential goes with them. Both are WhatsApp's internals and can
// change without notice: when they do, the query fails, every order is published as its
// summary, and the warning in the log says why. They live here and nowhere else.
const (
	orderCatalogEndpoint    = "https://graph.whatsapp.com/graphql/catalog"
	orderCatalogDocID       = "26593811266898374"
	orderCatalogAccessToken = "WA|787118555984857|7bb1544a3599aa180ac9a3f7688ba243"
	// orderImageSide is the size WhatsApp Web asks the product pictures in.
	orderImageSide = 80
	// orderAnswerLimit bounds what is read of an answer. A real one for a single item is
	// under a kilobyte, so this is room for a large order and not for anything else.
	orderAnswerLimit = 1 << 20
)

// orderDetailsWait bounds how long a live catalog order waits for its items. WhatsApp
// answers in well under a second, and every message behind this one waits with it.
const orderDetailsWait = 5 * time.Second

// orderCard is the card an order is published as, before anything is asked about it.
//
// The parts are what WhatsApp shows in the chat, on both sides: the item count, the
// estimated total, and the customer's own words. The order's title, which is the seller
// account's name, is not on WhatsApp's card, so it stays inside the order for whoever
// wants it rather than on top of the card. The count and the total are written the way
// WhatsApp Web writes them in English, which is the language the items are asked in, for
// a client that renders only the parts. A client that reads the order renders it in its
// own language from the numbers.
func orderCard(sent *waE2E.OrderMessage) protocol.RichContent {
	content := protocol.Rich(protocol.RichOrder)
	if sent.ItemCount != nil {
		content.Title = itemCountLabel(sent.GetItemCount())
	}
	content.Body = sent.GetMessage()
	if sent.TotalAmount1000 != nil {
		if total := amountLabel(sent.GetTotalAmount1000(), sent.GetTotalCurrencyCode()); total != "" {
			content.Footer = total + " (estimated total)"
		}
	}
	order := &protocol.Order{
		ID:        sent.GetOrderID(),
		Title:     sent.GetOrderTitle(),
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

func itemCountLabel(count int32) string {
	if count == 1 {
		return "🛒 1 item"
	}
	return fmt.Sprintf("🛒 %d items", count)
}

// amountLabel is an amount in thousandths written as WhatsApp Web writes a price: the
// currency's symbol and the digits its minor unit has. Empty for a currency code nothing
// here knows, rather than a guess at its digits.
func amountLabel(thousandths int64, code string) string {
	unit, err := currency.ParseISO(code)
	if err != nil {
		return ""
	}
	digits, _ := currency.Standard.Rounding(unit)
	symbol := message.NewPrinter(language.English).Sprint(currency.NarrowSymbol(unit))
	negative := thousandths < 0
	if negative {
		thousandths = -thousandths
	}
	// From thousandths to the currency's minor unit: down for one with fewer than three
	// decimals, rounding half up once at the target precision (rounding digit by digit
	// turns 1.490 yen into 2), and up for one with more.
	minor := thousandths
	if digits < 3 {
		divisor := int64(1)
		for range 3 - digits {
			divisor *= 10
		}
		minor = (thousandths + divisor/2) / divisor
	}
	for range max(digits-3, 0) {
		minor *= 10
	}
	scale := int64(1)
	for range digits {
		scale *= 10
	}
	written := strconv.FormatInt(minor/scale, 10)
	if digits > 0 {
		written += fmt.Sprintf(".%0*d", digits, minor%scale)
	}
	if negative {
		written = "-" + written
	}
	return symbol + written
}

// sellerOf is the seller account's address, a number or a LID, whichever the order
// carried. Nil when it carried none, or one the contract has no kind for.
func sellerOf(raw string) *protocol.Address {
	if raw == "" {
		return nil
	}
	jid, err := waTypes.ParseJID(raw)
	if err != nil {
		return nil
	}
	address, ok := addressOf(jid)
	if !ok {
		return nil
	}
	return &address
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
	if id, token := sent.GetOrderID(), sent.GetToken(); id != "" && token != "" && sent.GetSellerJID() != "" {
		ask, cancel := context.WithTimeout(ctx, s.orderWait)
		details, err := s.orderItems(ask, id, token, s.sellerForQuery(ask, sent.GetSellerJID()))
		cancel()
		switch {
		case err != nil:
			s.log.Warn().Err(err).Str("order", id).Msg("publishing a catalog order without its items, which the catalog query did not return")
		default:
			listed = itemize(content.Order, details)
		}
	}
	// Which order and how big, and nothing of what was bought or what it cost: a support
	// log is read by people who are not the shop.
	s.log.Info().Str("order", sent.GetOrderID()).Int32("item_count", sent.GetItemCount()).Bool("items_listed", listed).
		Msg("publishing a catalog order")
	return body{content: content, context: sent.GetContextInfo()}, true
}

// sellerForQuery is the seller's address the way WhatsApp Web sends it, which is the LID.
// An order naming the seller by number is asked about by the LID this account has for
// that number, and by the number when it has none.
func (s *Session) sellerForQuery(ctx context.Context, raw string) string {
	jid, err := waTypes.ParseJID(raw)
	if err != nil || jid.Server != waTypes.DefaultUserServer {
		return raw
	}
	client := s.current()
	if client == nil || client.Store == nil || client.Store.LIDs == nil {
		return raw
	}
	lid, err := client.Store.LIDs.GetLIDForPN(ctx, jid)
	if err != nil || lid.IsEmpty() {
		return raw
	}
	return lid.String()
}

// orderDetails is what the catalog API answers about an order.
type orderDetails struct {
	products []orderProduct
	subtotal *int64
	total    *int64
}

type orderProduct struct {
	ID       string          `json:"id"`
	Name     string          `json:"name"`
	Price    json.RawMessage `json:"price"`
	Currency string          `json:"currency"`
	Quantity int             `json:"quantity"`
	Variant  struct {
		Properties []json.RawMessage `json:"variant_properties"`
	} `json:"variant_info"`
	Media struct {
		Images []struct {
			URL string `json:"request_image_url"`
		} `json:"images"`
	} `json:"media"`
}

// orderItems asks WhatsApp's catalog API for the items of an order, the way WhatsApp Web
// does. The answer's amounts are in thousandths, the scale of the message's own total.
func (s *Session) orderItems(ctx context.Context, orderID, token, seller string) (*orderDetails, error) {
	request := map[string]any{
		"access_token": orderCatalogAccessToken,
		"doc_id":       orderCatalogDocID,
		"variables": map[string]any{"request": map[string]any{"order": map[string]any{
			"jid":              seller,
			"token":            map[string]string{"sensitive_string_value": token},
			"id":               orderID,
			"image_dimensions": map[string]int{"height": orderImageSide, "width": orderImageSide},
		}}},
		"lang": "en_US",
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("encode the order query: %w", err)
	}
	post, err := http.NewRequestWithContext(ctx, http.MethodPost, s.orderEndpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("build the order query: %w", err)
	}
	post.Header.Set("Content-Type", "application/json")
	post.Header.Set("Accept", "application/json")
	client := s.orderHTTP
	if client == nil {
		client = &http.Client{Transport: &s.route.media}
	}
	response, err := client.Do(post)
	if err != nil {
		return nil, fmt.Errorf("ask the catalog for the order: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(response.Body, orderAnswerLimit))
	if err != nil {
		return nil, fmt.Errorf("read the catalog's answer: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("the catalog answered %d", response.StatusCode)
	}
	var answer struct {
		Data struct {
			Info struct {
				Order *struct {
					Products []orderProduct `json:"products"`
					Price    struct {
						Subtotal *int64 `json:"subtotal_amount"`
						Total    *int64 `json:"total_amount"`
					} `json:"price_details"`
				} `json:"order"`
			} `json:"xwa_checkout_get_order_info"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(raw, &answer); err != nil {
		return nil, fmt.Errorf("decode the catalog's answer: %w", err)
	}
	if len(answer.Errors) > 0 {
		return nil, fmt.Errorf("the catalog refused the query: %s", answer.Errors[0].Message)
	}
	order := answer.Data.Info.Order
	if order == nil {
		return nil, errors.New("the catalog's answer has no order in it")
	}
	return &orderDetails{products: order.Products, subtotal: order.Price.Subtotal, total: order.Price.Total}, nil
}

// itemize puts the items of an answered query on the order, and reports whether it
// listed any.
//
// The catalog's prices were measured in thousandths, the scale of the message's total,
// and the message's total is still what proves it for each order: equal to the catalog's,
// the prices go out as they came, and a thousandth of it (the unit an older answer used)
// they are scaled. A total that matches neither way leaves the prices off rather than
// showing an agent a figure off by a factor of a thousand. The items still go out, with
// their names and quantities.
func itemize(order *protocol.Order, details *orderDetails) bool {
	var scale int64
	switch {
	case order.Total1000 == nil || *order.Total1000 == 0 || details.total == nil:
	case *details.total == *order.Total1000:
		scale = 1
	case *details.total*1000 == *order.Total1000:
		scale = 1000
	}
	for i := range details.products {
		product := &details.products[i]
		item := protocol.OrderItem{
			ProductID: product.ID,
			Name:      product.Name,
			Quantity:  product.Quantity,
			Currency:  product.Currency,
			Variant:   variantOf(product.Variant.Properties),
		}
		if len(product.Media.Images) > 0 {
			item.Image = product.Media.Images[0].URL
		}
		if price, ok := amountOf(product.Price); ok && scale != 0 {
			scaled := price * scale
			item.Price1000 = &scaled
		}
		order.Items = append(order.Items, item)
	}
	if scale != 0 && details.subtotal != nil && len(order.Items) > 0 {
		subtotal := *details.subtotal * scale
		order.Subtotal1000 = &subtotal
	}
	return len(order.Items) > 0
}

// amountOf reads an amount the catalog writes as a string of digits ("12500"), or as a
// number.
func amountOf(raw json.RawMessage) (int64, bool) {
	if len(raw) == 0 {
		return 0, false
	}
	var written string
	if err := json.Unmarshal(raw, &written); err == nil {
		amount, err := strconv.ParseInt(written, 10, 64)
		return amount, err == nil
	}
	var amount int64
	if err := json.Unmarshal(raw, &amount); err != nil {
		return 0, false
	}
	return amount, true
}

// variantOf is a product's variant as one line. The measured order had none, so the
// shape of a property is read loosely: a name and a value become "name: value", a plain
// string stays itself, and anything else is left out.
func variantOf(properties []json.RawMessage) string {
	var parts []string
	for _, raw := range properties {
		var written string
		if json.Unmarshal(raw, &written) == nil {
			if written != "" {
				parts = append(parts, written)
			}
			continue
		}
		var property struct {
			Name  string `json:"name"`
			Value string `json:"value"`
		}
		if json.Unmarshal(raw, &property) != nil || property.Value == "" {
			continue
		}
		if property.Name == "" {
			parts = append(parts, property.Value)
			continue
		}
		parts = append(parts, property.Name+": "+property.Value)
	}
	return strings.Join(parts, ", ")
}

// catalogOrderOf is the order a message carries, nil when it is not one.
func catalogOrderOf(event *waEvents.Message) *waE2E.OrderMessage {
	return event.Message.GetOrderMessage()
}
