package whatsmeow

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waHistorySync "go.mau.fi/whatsmeow/proto/waHistorySync"
	waEvents "go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/fazer-ai/whatsapp-connector/internal/media"
	"github.com/fazer-ai/whatsapp-connector/internal/testwait"
)

// An order a customer sends from a business's catalog used to reach the agent as a
// placeholder, so the agent had to open the phone to see what was bought (#422). It is a
// card now: the parts a client already renders, and the order itself beside them.

// orderQuery stands in for WhatsApp's catalog API: it records what each query sent and
// answers with what a test set.
type orderQuery struct {
	mu      sync.Mutex
	asked   []map[string]any
	answer  string
	status  int
	blocked bool
}

func (q *orderQuery) install(t *testing.T, session *Session) {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var sent map[string]any
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &sent)
		q.mu.Lock()
		q.asked = append(q.asked, sent)
		answer, status, blocked := q.answer, q.status, q.blocked
		q.mu.Unlock()
		if blocked {
			<-r.Context().Done()
			return
		}
		if status == 0 {
			status = http.StatusOK
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, answer)
	}))
	t.Cleanup(server.Close)
	session.orderEndpoint = server.URL
	session.orderHTTP = server.Client()
	// A query that answers gets the shared budget, so a slow scheduler under -race does
	// not turn a success into a summary. Only the one that never answers is held to a
	// short wait, which is what it is there to show.
	session.orderWait = testwait.Budget
	if q.blocked {
		session.orderWait = 50 * time.Millisecond
	}
}

func (q *orderQuery) calls() []map[string]any {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]map[string]any(nil), q.asked...)
}

// catalogAnswer is the catalog's answer for the two-item order, in the shape WhatsApp
// Web was measured receiving, with the prices multiplied by scale and the given total.
func catalogAnswer(scale, total int64) string {
	return fmt.Sprintf(`{"data":{"xwa_checkout_get_order_info":{"order":{"creation_time_stamp":1794948016,
		"products":[
			{"id":"P1","name":"Camiseta","price":"%d","currency":"EUR","quantity":1,
			 "variant_info":{"variant_properties":[{"name":"Tamanho","value":"M"}]},
			 "media":{"images":[{"id":"1","request_image_url":"https://mmg.whatsapp.net/p1.jpg"}]}},
			{"id":"P2","name":"Caneca","price":"%d","currency":"EUR","quantity":1,
			 "variant_info":{"variant_properties":[]},"media":{"images":[]}}],
		"price_details":{"subtotal_amount":%d,"currency":"EUR","total_amount":%d}}}},"extensions":{"is_final":true}}`,
		10*scale, 15*scale, 25*scale, total)
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
		TotalCurrencyCode: proto.String("EUR"),
		Thumbnail:         []byte{0xff, 0xd8, 0xff, 0xe0},
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
	query := &orderQuery{answer: catalogAnswer(1000, 25000)}
	query.install(t, session)

	content := inboundContentOf(t, publishedBy(t, session, orderEvent("3EB0ORDER", catalogOrder())))
	order := orderOf(t, content)

	// What WhatsApp shows on the order in the chat: the count, the estimated total and the
	// customer's words. The seller's name is not on WhatsApp's card.
	for field, want := range map[string]any{
		"title": "🛒 2 items", "body": "Quero entregar amanhã", "footer": "€25.00 (estimated total)",
	} {
		if content[field] != want {
			t.Errorf("the card's %s is %v, want %v", field, content[field], want)
		}
	}
	for field, want := range map[string]any{
		"id": "ORD-1", "title": "Pedido Loja X", "status": "INQUIRY", "surface": "CATALOG", "item_count": float64(2),
		"total_1000": float64(25000), "currency": "EUR", "subtotal_1000": float64(25000),
	} {
		if order[field] != want {
			t.Errorf("order.%s is %v, want %v", field, order[field], want)
		}
	}
	// The seller as an address, so a number and a LID are never the same digits.
	if seller, _ := order["seller"].(map[string]any); seller["kind"] != "phone" || seller["id"] != "5511988887777" {
		t.Errorf("order.seller is %v", order["seller"])
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
		"product_id": "P1", "name": "Camiseta", "quantity": float64(1), "price_1000": float64(10000), "currency": "EUR",
		"variant": "Tamanho: M", "image": "https://mmg.whatsapp.net/p1.jpg",
	} {
		if first[field] != want {
			t.Errorf("items[0].%s is %v, want %v", field, first[field], want)
		}
	}
	if second["product_id"] != "P2" || second["price_1000"] != float64(15000) || second["variant"] != nil || second["image"] != nil {
		t.Errorf("items[1] is %v", second)
	}
	// The query WhatsApp Web makes, field for field.
	calls := query.calls()
	if len(calls) != 1 {
		t.Fatalf("the catalog was asked %d times, want once", len(calls))
	}
	asked, _ := json.Marshal(calls[0])
	want := `{"access_token":"WA|787118555984857|7bb1544a3599aa180ac9a3f7688ba243","doc_id":"26593811266898374",` +
		`"lang":"en_US","variables":{"request":{"order":{"id":"ORD-1","image_dimensions":{"height":80,"width":80},` +
		`"jid":"5511988887777@s.whatsapp.net","token":{"sensitive_string_value":"dG9rZW4="}}}}}`
	if string(asked) != want {
		t.Errorf("the catalog was asked\n%s\nwant\n%s", asked, want)
	}
}

// The catalog's prices were measured in thousandths, and the message's total still proves
// it for each order: a total that matches neither way leaves the prices out rather than
// showing an agent a figure off by a factor of a thousand.
func TestAnOrdersPricesAreReadInTheScaleTheMessagesTotalProves(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		details string
		total   *int64
		prices  []any
	}{
		{name: "already in thousandths", details: catalogAnswer(1000, 25000), prices: []any{float64(10000), float64(15000)}},
		{name: "in whole units", details: catalogAnswer(1, 25), prices: []any{float64(10000), float64(15000)}},
		{name: "in neither", details: catalogAnswer(1000, 999), prices: []any{nil, nil}},
		{name: "with no total on the message to compare", details: catalogAnswer(1000, 25000), total: proto.Int64(0), prices: []any{nil, nil}},
		// Zero against zero matches either way, and proves nothing about the prices.
		{name: "with both totals zero", details: catalogAnswer(1000, 0), total: proto.Int64(0), prices: []any{nil, nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			query := &orderQuery{answer: tc.details}
			query.install(t, session)
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
		{name: "the query fails", query: &orderQuery{status: http.StatusInternalServerError}},
		{name: "the query is refused", query: &orderQuery{answer: `{"errors":[{"message":"bad token"}]}`}},
		{name: "the query never answers", query: &orderQuery{blocked: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			tc.query.install(t, session)

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
			(&orderQuery{answer: catalogAnswer(1000, 25000)}).install(t, session)
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
	query := &orderQuery{answer: catalogAnswer(1000, 25000)}
	query.install(t, session)

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
	query := &orderQuery{answer: catalogAnswer(1000, 5000)}
	query.install(t, session)
	past := &waE2E.OrderMessage{
		OrderID: proto.String("ORD-H"), Token: proto.String("dG9rZW4="), ItemCount: proto.Int32(1),
		TotalAmount1000: proto.Int64(5000), TotalCurrencyCode: proto.String("EUR"),
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
		{name: "with its items", query: &orderQuery{answer: catalogAnswer(1000, 25000)}},
		{name: "without them", query: &orderQuery{status: http.StatusInternalServerError}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			written := &syncBuffer{}
			session.log = zerolog.New(written)
			tc.query.install(t, session)

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
	query := &orderQuery{answer: catalogAnswer(1000, 25000)}
	query.install(t, session)
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

// The parts a client that knows nothing of orders renders are what WhatsApp shows on the
// order in the chat: the count, and the total in the currency's own notation.
func TestAnOrdersCardReadsTheWayWhatsAppShowsIt(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name          string
		count         *int32
		total         *int64
		currency      string
		title, footer any
	}{
		{name: "one item", count: proto.Int32(1), total: proto.Int64(12500), currency: "EUR", title: "🛒 1 item", footer: "€12.50 (estimated total)"},
		{name: "a currency with no cents", count: proto.Int32(3), total: proto.Int64(1500000), currency: "JPY", title: "🛒 3 items", footer: "¥1500 (estimated total)"},
		// Rounded once at the yen, not digit by digit: 1.490 is 1, not 2.
		{name: "a fraction of a yen", count: proto.Int32(1), total: proto.Int64(1490), currency: "JPY", title: "🛒 1 item", footer: "¥1 (estimated total)"},
		{name: "a total in fractions of a cent", count: proto.Int32(2), total: proto.Int64(12345), currency: "USD", title: "🛒 2 items", footer: "$12.35 (estimated total)"},
		{name: "a currency with four decimals", count: proto.Int32(1), total: proto.Int64(12500), currency: "CLF", title: "🛒 1 item", footer: "CLF12.5000 (estimated total)"},
		{name: "a currency nothing here knows", count: proto.Int32(2), total: proto.Int64(12500), currency: "ZZZ", title: "🛒 2 items", footer: nil},
		{name: "no total", count: proto.Int32(2), currency: "EUR", title: "🛒 2 items", footer: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			(&orderQuery{status: http.StatusInternalServerError}).install(t, session)
			sent := &waE2E.OrderMessage{
				OrderID: proto.String("ORD-4"), ItemCount: tc.count, TotalAmount1000: tc.total, TotalCurrencyCode: proto.String(tc.currency),
			}

			content := inboundContentOf(t, publishedBy(t, session, orderEvent("3EB0LABEL", sent)))
			if content["title"] != tc.title || content["footer"] != tc.footer {
				t.Errorf("the card reads %v / %v, want %v / %v", content["title"], content["footer"], tc.title, tc.footer)
			}
		})
	}
}

// The order and the answer measured live on 17/11/2026, with the seller named by its LID:
// the query goes out naming it the same way, and the price arrives in thousandths.
func TestTheMeasuredOrderIsAskedAndReadAsWhatsAppWebDoes(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	query := &orderQuery{answer: `{"data":{"xwa_checkout_get_order_info":{"order":{"creation_time_stamp":1794948016,
		"products":[{"id":"70000000000000001","name":"Produto exemplo","price":"12500","currency":"EUR",
		"variant_info":{"variant_properties":[]},"media":{"images":[{"id":"70000000000000002","request_image_url":"https://mmg.whatsapp.net/v/t45.jpg"}]},"quantity":1}],
		"price_details":{"subtotal_amount":12500,"currency":"EUR","total_amount":12500}}}},"extensions":{"is_final":true}}`}
	query.install(t, session)
	sent := &waE2E.OrderMessage{
		OrderID: proto.String("70000000000000003"), Token: proto.String("dG9rZW4="), ItemCount: proto.Int32(1),
		Status: waE2E.OrderMessage_INQUIRY.Enum(), Surface: waE2E.OrderMessage_CATALOG.Enum(),
		OrderTitle: proto.String("Loja Exemplo"), SellerJID: proto.String("10000000000001@lid"),
		TotalAmount1000: proto.Int64(12500), TotalCurrencyCode: proto.String("EUR"),
	}

	order := orderOf(t, inboundContentOf(t, publishedBy(t, session, orderEvent("3EB0MEASURED", sent))))
	items, _ := order["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("the order lists %v", order["items"])
	}
	item, _ := items[0].(map[string]any)
	if item["name"] != "Produto exemplo" || item["price_1000"] != float64(12500) || item["image"] != "https://mmg.whatsapp.net/v/t45.jpg" {
		t.Errorf("the item went out as %v", item)
	}
	if seller, _ := order["seller"].(map[string]any); seller["kind"] != "lid" || seller["id"] != "10000000000001" || order["title"] != "Loja Exemplo" {
		t.Errorf("the order went out as %v", order)
	}
	calls := query.calls()
	if len(calls) != 1 {
		t.Fatalf("the catalog was asked %d times", len(calls))
	}
	variables, _ := calls[0]["variables"].(map[string]any)
	request, _ := variables["request"].(map[string]any)
	asked, _ := request["order"].(map[string]any)
	if asked["jid"] != "10000000000001@lid" || asked["id"] != "70000000000000003" {
		t.Errorf("the catalog was asked about %v", asked)
	}
}

// A price the catalog writes as a number reads the same, and one that is not a number is
// left off its item rather than taken as zero.
func TestAnItemsPriceIsReadWhicheverWayTheCatalogWritesIt(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	(&orderQuery{answer: `{"data":{"xwa_checkout_get_order_info":{"order":{
		"products":[{"id":"A","name":"Número","price":10000,"currency":"EUR","quantity":1},
		            {"id":"B","name":"Texto","price":"dez","currency":"EUR","quantity":2,
		             "variant_info":{"variant_properties":["Azul",{"value":"Grande"},{"name":"Vazio"},7]}}],
		"price_details":{"subtotal_amount":25000,"total_amount":25000}}}}}`}).install(t, session)

	order := orderOf(t, inboundContentOf(t, publishedBy(t, session, orderEvent("3EB0PRICES", catalogOrder()))))
	items, _ := order["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("the order lists %v", order["items"])
	}
	first, _ := items[0].(map[string]any)
	second, _ := items[1].(map[string]any)
	// A variant written as plain strings or as values with no name reads as what it says,
	// and what is not text is left out.
	if first["price_1000"] != float64(10000) || second["price_1000"] != nil || second["quantity"] != float64(2) ||
		second["variant"] != "Azul, Grande" {
		t.Errorf("the items went out as %v / %v", first, second)
	}
}

// The catalog is asked about an order on behalf of its seller, so an order that names
// none is published as its summary without a query that could only be refused.
func TestAnOrderWithNoSellerIsNotLookedUp(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	query := &orderQuery{answer: catalogAnswer(1000, 25000)}
	query.install(t, session)
	sent := catalogOrder()
	sent.SellerJID = nil

	order := orderOf(t, inboundContentOf(t, publishedBy(t, session, orderEvent("3EB0NOSELLER", sent))))
	if order["items"] != nil || order["seller"] != nil {
		t.Errorf("an order with no seller went out as %v", order)
	}
	if calls := query.calls(); len(calls) != 0 {
		t.Errorf("the catalog was asked about an order with no seller: %v", calls)
	}
}
