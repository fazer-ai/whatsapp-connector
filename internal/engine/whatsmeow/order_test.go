package whatsmeow

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	wm "go.mau.fi/whatsmeow"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waHistorySync "go.mau.fi/whatsmeow/proto/waHistorySync"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/fazer-ai/whatsapp-connector/internal/media"
)

// An order a customer sends from a business's catalog used to reach the agent as a
// placeholder, so the agent had to open the phone to see what was bought (#422). It is a
// card now: the parts a client already renders, and the order itself beside them.

// orderQuery records what the order query was asked and answers with what a test set.
type orderQuery struct {
	mu      sync.Mutex
	asked   []string
	answer  *waTypes.OrderDetails
	err     error
	blocked bool
	// deadline is whether the context the query ran under had one.
	deadline bool
}

func (q *orderQuery) install(session *Session) {
	session.orderWait = 50 * time.Millisecond
	session.orderDetails = func(ctx context.Context, _ *wm.Client, orderID, token string) (*waTypes.OrderDetails, error) {
		q.mu.Lock()
		q.asked = append(q.asked, orderID+" "+token)
		_, q.deadline = ctx.Deadline()
		answer, err, blocked := q.answer, q.err, q.blocked
		q.mu.Unlock()
		if blocked {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return answer, err
	}
}

func (q *orderQuery) calls() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]string(nil), q.asked...)
}

func catalogOrder() *waE2E.OrderMessage {
	return &waE2E.OrderMessage{
		OrderID:           proto.String("ORD-1"),
		Token:             proto.String("dG9rZW4="),
		ItemCount:         proto.Int32(2),
		Status:            waE2E.OrderMessage_INQUIRY.Enum(),
		Surface:           waE2E.OrderMessage_CATALOG.Enum(),
		Message:           proto.String("Quero entregar amanhã"),
		OrderTitle:        proto.String("Pedido Loja X"),
		SellerJID:         proto.String("5511988887777@s.whatsapp.net"),
		TotalAmount1000:   proto.Int64(25000),
		TotalCurrencyCode: proto.String("BRL"),
		Thumbnail:         []byte{0xff, 0xd8, 0xff, 0xe0},
	}
}

func orderDetailsAt(scale, total int64) *waTypes.OrderDetails {
	return &waTypes.OrderDetails{
		ID: "ORD-1",
		Products: []waTypes.OrderProduct{
			{ID: "P1", Name: "Camiseta", Quantity: 1, Price: 10 * scale, Currency: "BRL", VariantInfo: waTypes.VariantInfo{Properties: "M"}},
			{ID: "P2", Name: "Caneca", Quantity: 1, Price: 15 * scale, Currency: "BRL"},
		},
		Price: waTypes.OrderPrice{Subtotal: 25 * scale, Total: total, Currency: "BRL"},
	}
}

func orderEvent(id string, order *waE2E.OrderMessage) *waEvents.Message {
	event := textMessage(id, "")
	event.Message = &waE2E.Message{OrderMessage: order}
	return event
}

// orderOf is the order a published card carries, failing when the message is not one.
func orderOf(t *testing.T, content map[string]any) map[string]any {
	t.Helper()

	if content["type"] != "rich" || content["kind"] != "order" {
		t.Fatalf("the order was published as %v", content)
	}
	order, ok := content["order"].(map[string]any)
	if !ok {
		t.Fatalf("the card carries no order: %v", content)
	}
	return order
}

func TestACatalogOrderIsPublishedWithItsItems(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	query := &orderQuery{answer: orderDetailsAt(1000, 25000)}
	query.install(session)

	content := inboundContentOf(t, publishedBy(t, session, orderEvent("3EB0ORDER", catalogOrder())))
	order := orderOf(t, content)

	if content["title"] != "Pedido Loja X" || content["body"] != "Quero entregar amanhã" {
		t.Errorf("the card reads %q / %q, want the order's title and the customer's words", content["title"], content["body"])
	}
	for field, want := range map[string]any{
		"id": "ORD-1", "status": "INQUIRY", "surface": "CATALOG", "item_count": float64(2),
		"total_1000": float64(25000), "currency": "BRL", "seller": "5511988887777", "subtotal_1000": float64(25000),
	} {
		if order[field] != want {
			t.Errorf("order.%s is %v, want %v", field, order[field], want)
		}
	}
	if thumbnail, _ := order["thumbnail"].(string); thumbnail != "data:image/jpeg;base64,/9j/4A==" {
		t.Errorf("order.thumbnail is %q", thumbnail)
	}
	items, _ := order["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("the order lists %d items, want 2: %v", len(items), order["items"])
	}
	first, _ := items[0].(map[string]any)
	second, _ := items[1].(map[string]any)
	for field, want := range map[string]any{
		"product_id": "P1", "name": "Camiseta", "quantity": float64(1), "price_1000": float64(10000), "currency": "BRL", "variant": "M",
	} {
		if first[field] != want {
			t.Errorf("items[0].%s is %v, want %v", field, first[field], want)
		}
	}
	if second["product_id"] != "P2" || second["price_1000"] != float64(15000) {
		t.Errorf("items[1] is %v", second)
	}
	if calls := query.calls(); len(calls) != 1 || calls[0] != "ORD-1 dG9rZW4=" {
		t.Errorf("the order query was asked %v, want once for ORD-1 with the message's token", calls)
	}
	if !query.deadline {
		t.Error("the order query ran with no deadline, so a server that never answers holds the message")
	}
}

// The order query's prices have no documented unit, while the message's total is in
// thousandths. The total is what tells the two apart, and a total that matches neither
// way leaves the prices out rather than showing an agent a guess.
func TestAnOrdersPricesAreReadInTheScaleTheMessagesTotalProves(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		details *waTypes.OrderDetails
		total   *int64
		prices  []any
	}{
		{name: "already in thousandths", details: orderDetailsAt(1000, 25000), prices: []any{float64(10000), float64(15000)}},
		{name: "in whole units", details: orderDetailsAt(1, 25), prices: []any{float64(10000), float64(15000)}},
		{name: "in neither", details: orderDetailsAt(1000, 999), prices: []any{nil, nil}},
		{name: "with no total on the message to compare", details: orderDetailsAt(1000, 25000), total: proto.Int64(0), prices: []any{nil, nil}},
		// Zero against zero matches either way, and proves nothing about the prices.
		{name: "with both totals zero", details: orderDetailsAt(1000, 0), total: proto.Int64(0), prices: []any{nil, nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			query := &orderQuery{answer: tc.details}
			query.install(session)
			sent := catalogOrder()
			if tc.total != nil {
				sent.TotalAmount1000 = tc.total
			}

			order := orderOf(t, inboundContentOf(t, publishedBy(t, session, orderEvent("3EB0SCALE", sent))))
			items, _ := order["items"].([]any)
			if len(items) != 2 {
				t.Fatalf("the order lists %v, want both items whatever their prices", order["items"])
			}
			for i, want := range tc.prices {
				item, _ := items[i].(map[string]any)
				if item["price_1000"] != want {
					t.Errorf("items[%d].price_1000 is %v, want %v", i, item["price_1000"], want)
				}
				if item["name"] == nil || item["quantity"] == nil {
					t.Errorf("items[%d] lost its name or quantity: %v", i, item)
				}
			}
			// The message's total goes out as WhatsApp sent it, zero included.
			wantTotal := any(float64(25000))
			if tc.total != nil {
				wantTotal = float64(*tc.total)
			}
			if order["total_1000"] != wantTotal {
				t.Errorf("total_1000 is %v, want %v", order["total_1000"], wantTotal)
			}
			wantSubtotal := any(float64(25000))
			if tc.prices[0] == nil {
				wantSubtotal = nil
			}
			if order["subtotal_1000"] != wantSubtotal {
				t.Errorf("subtotal_1000 is %v, want %v", order["subtotal_1000"], wantSubtotal)
			}
		})
	}
}

// The items are worth having and not worth the order: a query that fails or never
// answers publishes the summary, once, and nothing corrects it afterwards.
func TestAnOrderWhoseItemsCannotBeReadStillArrives(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		query *orderQuery
	}{
		{name: "the query fails", query: &orderQuery{err: errors.New("iq: 500")}},
		{name: "the query never answers", query: &orderQuery{blocked: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			tc.query.install(session)

			order := orderOf(t, inboundContentOf(t, publishedBy(t, session, orderEvent("3EB0NOITEMS", catalogOrder()))))
			if order["id"] != "ORD-1" || order["total_1000"] != float64(25000) || order["item_count"] != float64(2) {
				t.Errorf("the summary went out as %v", order)
			}
			if order["items"] != nil || order["subtotal_1000"] != nil {
				t.Errorf("an order whose query did not answer lists %v / %v", order["items"], order["subtotal_1000"])
			}
			select {
			case extra := <-session.Events():
				t.Fatalf("the order was followed by %s", extra.Type)
			case <-time.After(100 * time.Millisecond):
			}
		})
	}
}

// What the business's own phone sends about an order is the same kind of message, and
// the status is what it says.
func TestAnOrderTheAccountAnsweredCarriesItsStatus(t *testing.T) {
	t.Parallel()

	for _, status := range []waE2E.OrderMessage_OrderStatus{waE2E.OrderMessage_ACCEPTED, waE2E.OrderMessage_DECLINED} {
		t.Run(status.String(), func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			(&orderQuery{answer: orderDetailsAt(1000, 25000)}).install(session)
			sent := catalogOrder()
			sent.OrderID = proto.String("ORD-2")
			sent.Status = status.Enum()
			event := orderEvent("3EB0ANSWER", sent)
			event.Info.IsFromMe = true

			order := orderOf(t, inboundContentOf(t, publishedBy(t, session, event)))
			if order["status"] != status.String() || order["id"] != "ORD-2" {
				t.Errorf("the order went out as %v", order)
			}
		})
	}
}

// An order with nothing but its id is still an order, and the connector writes no words
// of its own into the card to fill it.
func TestAnOrderWithNothingButItsIDIsStillAnOrder(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	query := &orderQuery{answer: orderDetailsAt(1000, 25000)}
	query.install(session)

	content := inboundContentOf(t, publishedBy(t, session, orderEvent("3EB0BARE", &waE2E.OrderMessage{OrderID: proto.String("ORD-3")})))
	order := orderOf(t, content)
	if order["id"] != "ORD-3" {
		t.Errorf("order.id is %v", order["id"])
	}
	// A status WhatsApp did not send reads as the first of its values, which would say the
	// customer asked for something nobody asked for.
	if order["status"] != nil || order["surface"] != nil || order["total_1000"] != nil || order["item_count"] != nil {
		t.Errorf("a bare order went out with values WhatsApp never sent: %v", order)
	}
	if content["title"] != nil || content["body"] != nil || order["thumbnail"] != nil {
		t.Errorf("a bare order was dressed up: title %v, body %v, thumbnail %v", content["title"], content["body"], order["thumbnail"])
	}
	if calls := query.calls(); len(calls) != 0 {
		t.Errorf("an order with no token was looked up: %v", calls)
	}
}

// A dump is published without a network call in the middle of it, so an order out of
// one goes out as its summary, and the messages around it go out as they would.
func TestAnOrderInADumpIsPublishedAsItsSummary(t *testing.T) {
	t.Parallel()

	session, _ := mediaSession(t, media.Options{})
	session.setHistory(true)
	query := &orderQuery{answer: orderDetailsAt(1000, 5000)}
	query.install(session)
	past := &waE2E.OrderMessage{
		OrderID: proto.String("ORD-H"), Token: proto.String("dG9rZW4="), ItemCount: proto.Int32(1),
		TotalAmount1000: proto.Int64(5000), TotalCurrencyCode: proto.String("BRL"),
	}
	chat := "5511999990002@s.whatsapp.net"
	bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_ON_DEMAND,
		&waHistorySync.Conversation{ID: proto.String(chat), Messages: []*waHistorySync.HistorySyncMsg{
			pastMessage(chat, "3EB0DEPOIS", 1754000002, false, &waE2E.Message{Conversation: proto.String("depois")}),
			pastMessage(chat, "3EB0PEDIDO", 1754000001, false, &waE2E.Message{OrderMessage: past}),
			pastMessage(chat, "3EB0ANTES", 1754000000, false, &waE2E.Message{Conversation: proto.String("antes")}),
		}})}
	bench.install(session)

	acknowledged := make(chan bool, 1)
	go func() {
		acknowledged <- session.receive(historyNotification("NOTIFORDER", waE2E.HistorySyncType_ON_DEMAND))
	}()
	slices, got := slicesUntil(t, session, acknowledged)
	if !got || len(slices) != 1 || len(slices[0].Messages) != 3 {
		t.Fatalf("published %+v (acknowledged %v), want one slice with all three messages", slices, got)
	}
	var orders int
	for _, message := range slices[0].Messages {
		raw, err := json.Marshal(message.Content)
		if err != nil {
			t.Fatalf("marshal a message: %v", err)
		}
		var content map[string]any
		if err := json.Unmarshal(raw, &content); err != nil {
			t.Fatalf("decode a message: %v", err)
		}
		if content["type"] == "text" {
			continue
		}
		orders++
		order := orderOf(t, content)
		if order["id"] != "ORD-H" || order["item_count"] != float64(1) || order["total_1000"] != float64(5000) || order["items"] != nil {
			t.Errorf("the order out of the dump went out as %v", order)
		}
	}
	if orders != 1 {
		t.Errorf("the slice carries %d orders, want 1", orders)
	}
	if calls := query.calls(); len(calls) != 0 {
		t.Errorf("publishing the dump looked the order up: %v", calls)
	}
}

// The log says an order arrived and how big it was, and nothing of what was bought or
// what it cost: a support log is read by people who are not the shop.
func TestTheOrderLogNamesTheOrderAndNothingInIt(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		query *orderQuery
	}{
		{name: "with its items", query: &orderQuery{answer: orderDetailsAt(1000, 25000)}},
		{name: "without them", query: &orderQuery{err: errors.New("iq: 500")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			written := &syncBuffer{}
			session.log = zerolog.New(written)
			tc.query.install(session)

			publishedBy(t, session, orderEvent("3EB0LOGGED", catalogOrder()))
			var named bool
			for _, line := range strings.Split(written.String(), "\n") {
				if line == "" {
					continue
				}
				var entry map[string]any
				if err := json.Unmarshal([]byte(line), &entry); err != nil {
					t.Fatalf("a log line is not JSON: %q", line)
				}
				if entry["level"] == "debug" || entry["level"] == "trace" {
					continue
				}
				for _, secret := range []string{"Camiseta", "Caneca", "25000", "10000", "15000", "Quero entregar amanhã"} {
					if strings.Contains(line, secret) {
						t.Errorf("the log carries %q: %s", secret, line)
					}
				}
				if entry["level"] == "info" && entry["order"] == "ORD-1" && entry["item_count"] == float64(2) {
					named = true
				}
			}
			if !named {
				t.Errorf("no info line names the order and its item count:\n%s", written.String())
			}
		})
	}
}

// What is still not read stays the placeholder it was, and nothing looks it up as an order.
func TestAPollIsStillAPlaceholder(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	query := &orderQuery{answer: orderDetailsAt(1000, 25000)}
	query.install(session)
	event := textMessage("3EB0POLL", "")
	event.Message = &waE2E.Message{PollCreationMessage: &waE2E.PollCreationMessage{Name: proto.String("Qual dia?")}}

	content := inboundContentOf(t, publishedBy(t, session, event))
	if content["type"] != "unsupported" || content["reason"] != "unknown_type" || content["detail"] != "pollCreationMessage" {
		t.Errorf("the poll went out as %v", content)
	}
	if calls := query.calls(); len(calls) != 0 {
		t.Errorf("the poll was looked up as an order: %v", calls)
	}
}
