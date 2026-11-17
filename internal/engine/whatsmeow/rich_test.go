package whatsmeow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"

	wm "go.mau.fi/whatsmeow"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waHistorySync "go.mau.fi/whatsmeow/proto/waHistorySync"
	"google.golang.org/protobuf/proto"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/media"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// A card is what a business sends with buttons on it: an appointment to confirm, an
// order to track, a menu to pick from. Published as a placeholder, the agent saw a blank
// bubble where the customer saw the whole message (#414).
func TestACardIsPublishedWithEveryPartAnAgentReads(t *testing.T) {
	t.Parallel()

	type button = map[string]any
	for _, tc := range []struct {
		name    string
		body    *waE2E.Message
		kind    string
		title   string
		text    string
		footer  string
		buttons []button
	}{
		{
			name: "buttons", kind: "buttons",
			body: &waE2E.Message{ButtonsMessage: &waE2E.ButtonsMessage{
				Header:      &waE2E.ButtonsMessage_Text{Text: "Confirmação de agendamento"},
				ContentText: proto.String("Sua consulta é amanhã às 10h."),
				FooterText:  proto.String("Clínica Exemplo"),
				Buttons: []*waE2E.ButtonsMessage_Button{
					{ButtonID: proto.String("b1"), ButtonText: &waE2E.ButtonsMessage_Button_ButtonText{DisplayText: proto.String("Confirmar")}},
					{ButtonID: proto.String("b2"), ButtonText: &waE2E.ButtonsMessage_Button_ButtonText{DisplayText: proto.String("Reagendar")}},
				},
			}},
			title: "Confirmação de agendamento", text: "Sua consulta é amanhã às 10h.", footer: "Clínica Exemplo",
			buttons: []button{{"text": "Confirmar"}, {"text": "Reagendar"}},
		},
		{
			// Each kind of template button keeps what it does: a link and a number are what
			// an agent would have to ask the customer for otherwise.
			name: "a template", kind: "template",
			body: &waE2E.Message{TemplateMessage: &waE2E.TemplateMessage{
				HydratedTemplate: &waE2E.TemplateMessage_HydratedFourRowTemplate{
					Title:               &waE2E.TemplateMessage_HydratedFourRowTemplate_HydratedTitleText{HydratedTitleText: "Pedido #4821"},
					HydratedContentText: proto.String("Seu pedido saiu para entrega."),
					HydratedFooterText:  proto.String("Loja Central"),
					HydratedButtons: []*waE2E.HydratedTemplateButton{
						{HydratedButton: &waE2E.HydratedTemplateButton_UrlButton{UrlButton: &waE2E.HydratedTemplateButton_HydratedURLButton{
							DisplayText: proto.String("Acompanhar"), URL: proto.String("https://exemplo.com/rastreio/4821"),
						}}},
						{HydratedButton: &waE2E.HydratedTemplateButton_QuickReplyButton{QuickReplyButton: &waE2E.HydratedTemplateButton_HydratedQuickReplyButton{
							DisplayText: proto.String("Falar com atendente"), ID: proto.String("q1"),
						}}},
						{HydratedButton: &waE2E.HydratedTemplateButton_CallButton{CallButton: &waE2E.HydratedTemplateButton_HydratedCallButton{
							DisplayText: proto.String("Ligar para a loja"), PhoneNumber: proto.String("+5541999990000"),
						}}},
					},
				},
			}},
			title: "Pedido #4821", text: "Seu pedido saiu para entrega.", footer: "Loja Central",
			buttons: []button{
				{"text": "Acompanhar", "url": "https://exemplo.com/rastreio/4821"},
				{"text": "Falar com atendente"},
				{"text": "Ligar para a loja", "phone": "+5541999990000"},
			},
		},
		{
			// The template in the four-row arm rather than the field beside it. Both are on
			// the wire, and reading only one leaves the other blank.
			name: "a template in its four-row arm", kind: "template",
			body: &waE2E.Message{TemplateMessage: &waE2E.TemplateMessage{
				Format: &waE2E.TemplateMessage_HydratedFourRowTemplate_{HydratedFourRowTemplate: &waE2E.TemplateMessage_HydratedFourRowTemplate{
					HydratedContentText: proto.String("Seu código de rastreio chegou."),
				}},
			}},
			text: "Seu código de rastreio chegou.",
		},
		{
			// The shape the WhatsApp Business API sends today. Its buttons are JSON inside
			// the proto, which is where a typo in a key would lose them without an error.
			name: "an interactive message", kind: "interactive",
			body: &waE2E.Message{InteractiveMessage: &waE2E.InteractiveMessage{
				Header: &waE2E.InteractiveMessage_Header{Title: proto.String("Confirme sua reunião")},
				Body:   &waE2E.InteractiveMessage_Body{Text: proto.String("Reunião quinta às 15h com o time comercial.")},
				Footer: &waE2E.InteractiveMessage_Footer{Text: proto.String("SDR Exemplo")},
				InteractiveMessage: &waE2E.InteractiveMessage_NativeFlowMessage_{NativeFlowMessage: &waE2E.InteractiveMessage_NativeFlowMessage{
					Buttons: []*waE2E.InteractiveMessage_NativeFlowMessage_NativeFlowButton{
						{Name: proto.String("quick_reply"), ButtonParamsJSON: proto.String(`{"display_text":"Confirmar","id":"ok"}`)},
						{Name: proto.String("cta_url"), ButtonParamsJSON: proto.String(`{"display_text":"Abrir agenda","url":"https://exemplo.com/agenda"}`)},
						{Name: proto.String("cta_call"), ButtonParamsJSON: proto.String(`{"display_text":"Ligar","phone_number":"+5541988887777"}`)},
						{Name: proto.String("open_webview"), ButtonParamsJSON: proto.String(`{"title":"Ver no site","link":{"url":"https://exemplo.com/site"}}`)},
						{Name: proto.String("single_select"), ButtonParamsJSON: proto.String(`{"title":"Opções","sections":[{"rows":[{"title":"Plano A"},{"title":"Plano B"}]}]}`)},
						// Not JSON at all: the button is lost, the card is not.
						{Name: proto.String("quick_reply"), ButtonParamsJSON: proto.String(`{não é json`)},
					},
				}},
			}},
			title: "Confirme sua reunião", text: "Reunião quinta às 15h com o time comercial.", footer: "SDR Exemplo",
			buttons: []button{
				{"text": "Confirmar"},
				{"text": "Abrir agenda", "url": "https://exemplo.com/agenda"},
				{"text": "Ligar", "phone": "+5541988887777"},
				{"text": "Ver no site", "url": "https://exemplo.com/site"},
				{"text": "Plano A"},
				{"text": "Plano B"},
			},
		},
		{
			// What a business's template looks like once Meta has turned it into an
			// interactive one: the card sits under the template rather than beside it.
			name: "an interactive message inside a template", kind: "interactive",
			body: &waE2E.Message{TemplateMessage: &waE2E.TemplateMessage{
				Format: &waE2E.TemplateMessage_InteractiveMessageTemplate{InteractiveMessageTemplate: &waE2E.InteractiveMessage{
					Header: &waE2E.InteractiveMessage_Header{Title: proto.String("Seu boleto")},
					Body:   &waE2E.InteractiveMessage_Body{Text: proto.String("O boleto vence dia 20.")},
					Footer: &waE2E.InteractiveMessage_Footer{Text: proto.String("Financeiro")},
					InteractiveMessage: &waE2E.InteractiveMessage_NativeFlowMessage_{NativeFlowMessage: &waE2E.InteractiveMessage_NativeFlowMessage{
						Buttons: []*waE2E.InteractiveMessage_NativeFlowMessage_NativeFlowButton{
							{Name: proto.String("cta_url"), ButtonParamsJSON: proto.String(`{"display_text":"Ver boleto","url":"https://exemplo.com/b/77"}`)},
						},
					}},
				}},
			}},
			title: "Seu boleto", text: "O boleto vence dia 20.", footer: "Financeiro",
			buttons: []button{{"text": "Ver boleto", "url": "https://exemplo.com/b/77"}},
		},
		{
			// Every row of every section is something the customer could pick, so a row
			// left out is a choice the agent cannot see was offered.
			name: "a list", kind: "list",
			body: &waE2E.Message{ListMessage: &waE2E.ListMessage{
				Title:       proto.String("Escolha o horário"),
				Description: proto.String("Horários disponíveis para sexta"),
				FooterText:  proto.String("Clínica Exemplo"),
				ButtonText:  proto.String("Ver horários"),
				Sections: []*waE2E.ListMessage_Section{
					{Title: proto.String("Manhã"), Rows: []*waE2E.ListMessage_Row{{Title: proto.String("09:00")}, {Title: proto.String("10:00")}}},
					{Title: proto.String("Tarde"), Rows: []*waE2E.ListMessage_Row{{Title: proto.String("14:00")}}},
				},
			}},
			title: "Escolha o horário", text: "Horários disponíveis para sexta", footer: "Clínica Exemplo",
			buttons: []button{{"text": "09:00"}, {"text": "10:00"}, {"text": "14:00"}},
		},
		{
			// Nothing but a button is still something the customer was asked to tap.
			name: "a card that is only a button", kind: "buttons",
			body: &waE2E.Message{ButtonsMessage: &waE2E.ButtonsMessage{
				Buttons: []*waE2E.ButtonsMessage_Button{{ButtonText: &waE2E.ButtonsMessage_Button_ButtonText{DisplayText: proto.String("OK")}}},
			}},
			buttons: []button{{"text": "OK"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			event := textMessage("3EB0CARD", "")
			event.Message = tc.body

			content := inboundContentOf(t, publishedBy(t, session, event))
			if content["type"] != "rich" {
				t.Fatalf("the card was published as %v", content)
			}
			for field, want := range map[string]string{"kind": tc.kind, "title": tc.title, "body": tc.text, "footer": tc.footer} {
				got, _ := content[field].(string)
				if got != want {
					t.Errorf("%s is %q, want %q", field, got, want)
				}
			}
			buttons, _ := content["buttons"].([]any)
			if len(buttons) != len(tc.buttons) {
				t.Fatalf("the card has %d buttons, want %d: %v", len(buttons), len(tc.buttons), buttons)
			}
			for i, want := range tc.buttons {
				got, _ := buttons[i].(map[string]any)
				if len(got) != len(want) {
					t.Errorf("button %d is %v, want %v", i, got, want)
					continue
				}
				for key, value := range want {
					if got[key] != value {
						t.Errorf("button %d is %v, want %v", i, got, want)
					}
				}
			}
		})
	}
}

// WhatsApp's own envelopes come off before the card is read: a business sending its card
// as view-once is the common case, and the parser sees what whatsmeow hands it.
func TestACardSentInsideAViewOnceEnvelopeIsStillACard(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	event := textMessage("3EB0VOCARD", "")
	event.Message = &waE2E.Message{ViewOnceMessage: &waE2E.FutureProofMessage{Message: &waE2E.Message{
		InteractiveMessage: &waE2E.InteractiveMessage{
			Body: &waE2E.InteractiveMessage_Body{Text: proto.String("Oferta da semana")},
			InteractiveMessage: &waE2E.InteractiveMessage_NativeFlowMessage_{NativeFlowMessage: &waE2E.InteractiveMessage_NativeFlowMessage{
				Buttons: []*waE2E.InteractiveMessage_NativeFlowMessage_NativeFlowButton{
					{Name: proto.String("cta_url"), ButtonParamsJSON: proto.String(`{"display_text":"Ver oferta","url":"https://exemplo.com/o"}`)},
				},
			}},
		},
	}}}
	event = event.UnwrapRaw()

	content := inboundContentOf(t, publishedBy(t, session, event))
	buttons, _ := content["buttons"].([]any)
	if content["type"] != "rich" || content["body"] != "Oferta da semana" || len(buttons) != 1 {
		t.Fatalf("a view-once card was published as %v", content)
	}
}

// A card replying to something is still a reply: the quote is what threads it.
func TestACardKeepsWhatItAnswers(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	event := textMessage("3EB0CARDQ", "")
	event.Message = &waE2E.Message{TemplateMessage: &waE2E.TemplateMessage{
		ContextInfo: &waE2E.ContextInfo{StanzaID: proto.String("3EB0ASKED")},
		Format: &waE2E.TemplateMessage_InteractiveMessageTemplate{InteractiveMessageTemplate: &waE2E.InteractiveMessage{
			Body: &waE2E.InteractiveMessage_Body{Text: proto.String("Segue o link pedido.")},
		}},
	}}

	if quoted := quotedIDOf(t, publishedBy(t, session, event)); quoted != "3EB0ASKED" {
		t.Fatalf("the card answering 3EB0ASKED was published quoting %q", quoted)
	}
}

// Tapping a button sends back the button's text. Published as anything but that text, the
// agent sees the customer said something and not what they chose.
func TestAnAnswerToACardIsPublishedAsTheTextTheContactChose(t *testing.T) {
	t.Parallel()

	quoting := func(id string) *waE2E.ContextInfo { return &waE2E.ContextInfo{StanzaID: proto.String(id)} }
	for _, tc := range []struct {
		name   string
		body   *waE2E.Message
		text   string
		quoted string
	}{
		{"a tapped button", &waE2E.Message{ButtonsResponseMessage: &waE2E.ButtonsResponseMessage{
			SelectedButtonID: proto.String("b1"),
			Response:         &waE2E.ButtonsResponseMessage_SelectedDisplayText{SelectedDisplayText: "Confirmar"},
			ContextInfo:      quoting("3EB0BUTTONS"),
		}}, "Confirmar", "3EB0BUTTONS"},
		{"a tapped template button", &waE2E.Message{TemplateButtonReplyMessage: &waE2E.TemplateButtonReplyMessage{
			SelectedID: proto.String("q1"), SelectedDisplayText: proto.String("Falar com atendente"),
			ContextInfo: quoting("3EB0TEMPLATE"),
		}}, "Falar com atendente", "3EB0TEMPLATE"},
		{"a picked row", &waE2E.Message{ListResponseMessage: &waE2E.ListResponseMessage{
			Title:             proto.String("10:00"),
			SingleSelectReply: &waE2E.ListResponseMessage_SingleSelectReply{SelectedRowID: proto.String("r2")},
			ContextInfo:       quoting("3EB0LIST"),
		}}, "10:00", "3EB0LIST"},
		{"a tapped interactive button", &waE2E.Message{InteractiveResponseMessage: &waE2E.InteractiveResponseMessage{
			Body: &waE2E.InteractiveResponseMessage_Body{Text: proto.String("Confirmar")},
			InteractiveResponseMessage: &waE2E.InteractiveResponseMessage_NativeFlowResponseMessage_{
				NativeFlowResponseMessage: &waE2E.InteractiveResponseMessage_NativeFlowResponseMessage{
					Name: proto.String("quick_reply"), ParamsJSON: proto.String(`{"id":"ok"}`),
				},
			},
			ContextInfo: quoting("3EB0INTERACTIVE"),
		}}, "Confirmar", "3EB0INTERACTIVE"},
		// No quote to keep, and none invented.
		{"a tapped button with nothing quoted", &waE2E.Message{ButtonsResponseMessage: &waE2E.ButtonsResponseMessage{
			Response: &waE2E.ButtonsResponseMessage_SelectedDisplayText{SelectedDisplayText: "Reagendar"},
		}}, "Reagendar", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			event := textMessage("3EB0ANSWER", "")
			event.Message = tc.body

			emission := publishedBy(t, session, event)
			content := inboundContentOf(t, emission)
			if content["type"] != "text" || content["body"] != tc.text {
				t.Fatalf("the answer was published as %v, want the text %q", content, tc.text)
			}
			if quoted := quotedIDOf(t, emission); quoted != tc.quoted {
				t.Fatalf("the answer quotes %q, want %q", quoted, tc.quoted)
			}
		})
	}
}

// An answer with no text in it says nothing the agent can read, and stays the placeholder
// it was rather than an empty bubble.
func TestAnAnswerWithNoTextStaysAPlaceholder(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	event := textMessage("3EB0NOANSWER", "")
	event.Message = &waE2E.Message{ButtonsResponseMessage: &waE2E.ButtonsResponseMessage{SelectedButtonID: proto.String("b1")}}

	if content := inboundContentOf(t, publishedBy(t, session, event)); content["type"] != "unsupported" {
		t.Fatalf("an answer with no text was published as %v", content)
	}
}

// The picture at the top of a card is the card's, and it is fetched like any other file:
// a header published without it is a menu without the menu.
func TestTheFileInACardsHeaderIsFetchedLikeAnyOther(t *testing.T) {
	t.Parallel()

	file := []byte("o cardápio de hoje")
	session, downloads := mediaSession(t, media.Options{})
	downloads.answer(file, nil)
	event := mediaEvent("3EB0MENU", headerCard())

	emissions, acknowledged := deliver(t, session, event, 1)
	if !acknowledged {
		t.Fatal("a card whose header was stored was left unacknowledged")
	}
	content := inboundContentOf(t, emissions[0])
	if content["type"] != "rich" || content["body"] != "Veja o cardápio de hoje" {
		t.Fatalf("the card was published as %v", content)
	}
	header := richMediaOf(t, emissions[0])
	if header == nil || header.Kind != protocol.MediaImage || header.Ref == nil {
		t.Fatalf("the card's header was published as %+v, want the picture with a reference to it", header)
	}
	body, _, err := session.blobs.(*media.Store).Open(header.Ref.ID)
	if err != nil {
		t.Fatalf("the reference names a blob the store does not have: %v", err)
	}
	defer func() { _ = body.Close() }()
	if kept, _ := io.ReadAll(body); !bytes.Equal(kept, file) {
		t.Fatalf("the blob holds %q, want %q", kept, file)
	}
}

// The same rule a file follows, for the same reason: a download that may work next time
// leaves the card on the phone to come again, rather than publishing it with a hole.
func TestACardWhoseHeaderMayArriveNextTimeIsLeftOnThePhone(t *testing.T) {
	t.Parallel()

	session, downloads := mediaSession(t, media.Options{})
	downloads.answer(nil, errors.New("connection reset"))

	if publishedNothing(t, session, mediaEvent("3EB0MENU", headerCard())) {
		t.Fatal("a card whose header may arrive next time was acknowledged")
	}
}

// A card out of a history dump gets what a file out of one gets: no download while the
// dump goes out, and a record that lets the client ask for the header later.
func TestACardsHeaderFromADumpIsKeptToBeFetchedLater(t *testing.T) {
	t.Parallel()

	session, _ := mediaSession(t, media.Options{})
	session.setHistory(true)
	var fetched int
	session.download = func(context.Context, *wm.Client, wm.DownloadableMessage, media.File) error {
		fetched++
		return nil
	}
	card := &waE2E.Message{TemplateMessage: &waE2E.TemplateMessage{HydratedTemplate: &waE2E.TemplateMessage_HydratedFourRowTemplate{
		Title: &waE2E.TemplateMessage_HydratedFourRowTemplate_DocumentMessage{DocumentMessage: &waE2E.DocumentMessage{
			Mimetype: proto.String("application/pdf"), FileName: proto.String("fatura.pdf"),
			URL: proto.String("https://mmg.whatsapp.net/f"), DirectPath: proto.String("/v/f"),
			MediaKey: make([]byte, 32), FileSHA256: make([]byte, 32), FileEncSHA256: make([]byte, 32), FileLength: proto.Uint64(2048),
		}},
		HydratedContentText: proto.String("Sua fatura chegou"),
		HydratedButtons: []*waE2E.HydratedTemplateButton{{HydratedButton: &waE2E.HydratedTemplateButton_QuickReplyButton{
			QuickReplyButton: &waE2E.HydratedTemplateButton_HydratedQuickReplyButton{DisplayText: proto.String("Paguei")},
		}}},
	}}}
	bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_ON_DEMAND,
		&waHistorySync.Conversation{ID: proto.String("5511999990002@s.whatsapp.net"), Messages: []*waHistorySync.HistorySyncMsg{
			pastMessage("5511999990002@s.whatsapp.net", "3EB0FATURA", 1754000000, false, card),
		}})}
	bench.install(session)

	acknowledged := make(chan bool, 1)
	go func() {
		acknowledged <- session.receive(historyNotification("NOTIFCARD", waE2E.HistorySyncType_ON_DEMAND))
	}()
	slices, got := slicesUntil(t, session, acknowledged)
	if !got || len(slices) != 1 || len(slices[0].Messages) != 1 {
		t.Fatalf("published %+v (acknowledged %v), want one slice with the card", slices, got)
	}
	raw, err := json.Marshal(slices[0].Messages[0].Content)
	if err != nil {
		t.Fatalf("marshal the content: %v", err)
	}
	var content protocol.RichContent
	if err := json.Unmarshal(raw, &content); err != nil {
		t.Fatalf("the card does not read back as one: %v", err)
	}
	switch {
	case content.Type != "rich" || content.Body != "Sua fatura chegou":
		t.Fatalf("the card went out as %s", raw)
	case content.Media == nil || content.Media.Kind != protocol.MediaDocument || content.Media.Ref != nil:
		t.Fatalf("the card's header went out as %+v, want the document with no file fetched for it", content.Media)
	case fetched != 0:
		t.Fatalf("the header was downloaded %d times while the dump was published", fetched)
	}

	kept, found, err := session.store.MediaPart(context.Background(), "3EB0FATURA")
	switch {
	case err != nil:
		t.Fatalf("read what was kept: %v", err)
	case !found:
		t.Fatal("nothing was kept to fetch the card's header with, so the client can never ask for it")
	case kept.Kind != string(protocol.MediaDocument) || kept.DirectPath != "/v/f":
		t.Fatalf("the header was kept as %+v", kept)
	}
}

// headerCard is an interactive card with a picture on top, for the tests about the file.
func headerCard() *waE2E.Message {
	return &waE2E.Message{InteractiveMessage: &waE2E.InteractiveMessage{
		Header: &waE2E.InteractiveMessage_Header{
			HasMediaAttachment: proto.Bool(true),
			Media: &waE2E.InteractiveMessage_Header_ImageMessage{ImageMessage: &waE2E.ImageMessage{
				Mimetype:   proto.String("image/jpeg"),
				DirectPath: proto.String(directPath), MediaKey: []byte("key"), FileEncSHA256: encSHA256(),
			}},
		},
		Body: &waE2E.InteractiveMessage_Body{Text: proto.String("Veja o cardápio de hoje")},
		InteractiveMessage: &waE2E.InteractiveMessage_NativeFlowMessage_{NativeFlowMessage: &waE2E.InteractiveMessage_NativeFlowMessage{
			Buttons: []*waE2E.InteractiveMessage_NativeFlowMessage_NativeFlowButton{
				{Name: proto.String("quick_reply"), ButtonParamsJSON: proto.String(`{"display_text":"Pedir","id":"pedir"}`)},
			},
		}},
	}}
}

// richMediaOf reads the header of a published card back the way a client decoding the
// event does.
func richMediaOf(t *testing.T, emission *engine.Emission) *protocol.MediaContent {
	t.Helper()

	var payload struct {
		Message struct {
			Content protocol.RichContent `json:"content"`
		} `json:"message"`
	}
	if err := json.Unmarshal(emission.Payload, &payload); err != nil {
		t.Fatalf("the card does not read back as one: %v", err)
	}
	return payload.Message.Content.Media
}

// quotedIDOf is the id of the message a published one quotes, empty when it quotes none.
func quotedIDOf(t *testing.T, emission *engine.Emission) string {
	t.Helper()

	var payload struct {
		Message protocol.InboundMessage `json:"message"`
	}
	if err := json.Unmarshal(emission.Payload, &payload); err != nil {
		t.Fatalf("decode the message: %v", err)
	}
	return payload.Message.QuotedID
}
