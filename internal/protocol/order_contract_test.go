package protocol_test

import (
	"encoding/json"
	"testing"
)

// An order travels inside a card rather than as content of its own, because a client
// refuses a content type it does not know and would drop the message, or the whole
// history slice around it (#422). What the contract does type is the order itself, so a
// client that reads it can rely on the shape.
func TestAnOrderTravelsInACardAndIsTyped(t *testing.T) {
	t.Parallel()

	schema := compile(t, "#/definitions/event")
	fixture, ok := fixtures(t, "event")["message_received_rich_order"]
	if !ok {
		t.Fatal("no fixture carries an order with its items")
	}
	if err := schema.Validate(fixture); err != nil {
		t.Fatalf("the order fixture does not validate: %v", err)
	}

	for _, tc := range []struct {
		name   string
		change func(content map[string]any)
	}{
		{name: "an order of its own type", change: func(content map[string]any) { content["type"] = "order" }},
		{name: "a count that is not a number", change: func(content map[string]any) {
			content["order"].(map[string]any)["item_count"] = "2"
		}},
		{name: "an order with no id", change: func(content map[string]any) {
			delete(content["order"].(map[string]any), "id")
		}},
		{name: "an item with no name", change: func(content map[string]any) {
			items := content["order"].(map[string]any)["items"].([]any)
			delete(items[0].(map[string]any), "name")
		}},
		{name: "a price that is not a whole number of thousandths", change: func(content map[string]any) {
			items := content["order"].(map[string]any)["items"].([]any)
			items[0].(map[string]any)["price_1000"] = 10.5
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var frame map[string]any
			raw, err := json.Marshal(fixture)
			if err != nil {
				t.Fatalf("copy the fixture: %v", err)
			}
			if err := json.Unmarshal(raw, &frame); err != nil {
				t.Fatalf("copy the fixture: %v", err)
			}
			message := frame["payload"].(map[string]any)["message"].(map[string]any)
			tc.change(message["content"].(map[string]any))
			if err := schema.Validate(frame); err == nil {
				t.Errorf("the contract accepts %s", tc.name)
			}
		})
	}
}
