package whatsmeow

import (
	"context"
	"encoding/json"

	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// What a business sends with buttons on it, and what the customer sends back by tapping
// one (#414).
//
// Both used to fall through to the placeholder, so an appointment confirmation reached the
// agent as a blank bubble and the customer's "Confirmar" as another. The card is published
// as the parts it was laid out in, the way the contract's `rich` content carries them; the
// answer is published as the text the customer's phone shows them having sent, quoting
// the card, because that is all it is.
//
// The shapes follow the Baileys parser in the Chatwoot fork, which reads the same protos
// out of JSON: the client already renders what that parser stores, and a card should look
// the same whichever connection it came through.

// card is a card's content, the file at its top if it has one, and the context it came
// with.
type card struct {
	content protocol.RichContent
	header  *attachment
	context *waE2E.ContextInfo
}

// cardOf reads a card out of a message, and reports whether there is one worth showing.
//
// A card with nothing in it -- no text, no button, no file -- is not one: it falls through
// to the placeholder, which is what it was before and what says somebody sent something.
//
// The interactive shape is read first because a template Meta has turned into an
// interactive one can carry both, and the interactive one is what the phone shows.
func cardOf(message *waE2E.Message) (card, bool) {
	template := message.GetTemplateMessage()
	var c card
	switch {
	case message.GetInteractiveMessage() != nil:
		c = interactiveCard(message.GetInteractiveMessage())
	case template.GetInteractiveMessageTemplate() != nil:
		c = interactiveCard(template.GetInteractiveMessageTemplate())
		// The template is what WhatsApp threads a reply on, not the card inside it.
		if template.GetContextInfo() != nil {
			c.context = template.GetContextInfo()
		}
	case template.GetHydratedTemplate() != nil, template.GetHydratedFourRowTemplate() != nil:
		c = templateCard(template)
	case message.GetButtonsMessage() != nil:
		c = buttonsCard(message.GetButtonsMessage())
	case message.GetListMessage() != nil:
		c = listCard(message.GetListMessage())
	default:
		return card{}, false
	}
	shown := c.content
	return c, shown.Title != "" || shown.Body != "" || shown.Footer != "" || len(shown.Buttons) > 0 || c.header != nil
}

func templateCard(template *waE2E.TemplateMessage) card {
	hydrated := template.GetHydratedTemplate()
	if hydrated == nil {
		hydrated = template.GetHydratedFourRowTemplate()
	}
	content := protocol.Rich(protocol.RichTemplate)
	content.Title = hydrated.GetHydratedTitleText()
	content.Body = hydrated.GetHydratedContentText()
	content.Footer = hydrated.GetHydratedFooterText()
	for _, button := range hydrated.GetHydratedButtons() {
		switch {
		case button.GetQuickReplyButton() != nil:
			content.Buttons = labelled(content.Buttons, protocol.RichButton{Text: button.GetQuickReplyButton().GetDisplayText()})
		case button.GetUrlButton() != nil:
			link := button.GetUrlButton()
			content.Buttons = labelled(content.Buttons, protocol.RichButton{Text: link.GetDisplayText(), URL: link.GetURL()})
		case button.GetCallButton() != nil:
			call := button.GetCallButton()
			content.Buttons = labelled(content.Buttons, protocol.RichButton{Text: call.GetDisplayText(), Phone: call.GetPhoneNumber()})
		}
	}
	return card{
		content: content,
		header: headerOf(&waE2E.Message{
			ImageMessage: hydrated.GetImageMessage(), VideoMessage: hydrated.GetVideoMessage(), DocumentMessage: hydrated.GetDocumentMessage(),
		}),
		context: template.GetContextInfo(),
	}
}

func interactiveCard(interactive *waE2E.InteractiveMessage) card {
	header := interactive.GetHeader()
	content := protocol.Rich(protocol.RichInteractive)
	content.Title = header.GetTitle()
	content.Body = interactive.GetBody().GetText()
	content.Footer = interactive.GetFooter().GetText()
	for _, button := range interactive.GetNativeFlowMessage().GetButtons() {
		for _, rendered := range nativeFlowButtons(button) {
			content.Buttons = labelled(content.Buttons, rendered)
		}
	}
	return card{
		content: content,
		header: headerOf(&waE2E.Message{
			ImageMessage: header.GetImageMessage(), VideoMessage: header.GetVideoMessage(), DocumentMessage: header.GetDocumentMessage(),
		}),
		context: interactive.GetContextInfo(),
	}
}

func buttonsCard(buttons *waE2E.ButtonsMessage) card {
	content := protocol.Rich(protocol.RichButtons)
	content.Title = buttons.GetText()
	content.Body = buttons.GetContentText()
	content.Footer = buttons.GetFooterText()
	for _, button := range buttons.GetButtons() {
		content.Buttons = labelled(content.Buttons, protocol.RichButton{Text: button.GetButtonText().GetDisplayText()})
	}
	return card{
		content: content,
		header: headerOf(&waE2E.Message{
			ImageMessage: buttons.GetImageMessage(), VideoMessage: buttons.GetVideoMessage(), DocumentMessage: buttons.GetDocumentMessage(),
		}),
		context: buttons.GetContextInfo(),
	}
}

// listCard renders every row of every section as a button: each one is something the
// customer could have picked, and the section titles are only how the phone groups them.
func listCard(list *waE2E.ListMessage) card {
	content := protocol.Rich(protocol.RichList)
	content.Title = list.GetTitle()
	content.Body = list.GetDescription()
	content.Footer = list.GetFooterText()
	for _, section := range list.GetSections() {
		for _, row := range section.GetRows() {
			content.Buttons = labelled(content.Buttons, protocol.RichButton{Text: row.GetTitle()})
		}
	}
	return card{content: content, context: list.GetContextInfo()}
}

// nativeFlowButtons renders one button of an interactive message. Its parameters are JSON
// inside the proto, in snake case, and what a button does is in which keys it has.
//
// A list is one button that opens rows, so it renders as the rows. Parameters that are
// not JSON lose the button and nothing else: the rest of the card still says what it says.
func nativeFlowButtons(button *waE2E.InteractiveMessage_NativeFlowMessage_NativeFlowButton) []protocol.RichButton {
	var params struct {
		DisplayText string `json:"display_text"`
		Title       string `json:"title"`
		FlowCTA     string `json:"flow_cta"`
		URL         string `json:"url"`
		PhoneNumber string `json:"phone_number"`
		Link        struct {
			URL string `json:"url"`
		} `json:"link"`
		Sections []struct {
			Rows []struct {
				Title string `json:"title"`
			} `json:"rows"`
		} `json:"sections"`
	}
	if json.Unmarshal([]byte(button.GetButtonParamsJSON()), &params) != nil {
		return nil
	}
	switch button.GetName() {
	case "cta_url":
		return []protocol.RichButton{{Text: params.DisplayText, URL: params.URL}}
	case "cta_call":
		return []protocol.RichButton{{Text: params.DisplayText, Phone: params.PhoneNumber}}
	case "open_webview":
		return []protocol.RichButton{{Text: firstOf(params.Title, params.DisplayText), URL: params.Link.URL}}
	case "single_select":
		var rows []protocol.RichButton
		for _, section := range params.Sections {
			for _, row := range section.Rows {
				rows = append(rows, protocol.RichButton{Text: row.Title})
			}
		}
		return rows
	default:
		// quick_reply, cta_copy, a flow, and whatever WhatsApp adds next: the label is
		// what the customer saw, under whichever key this one keeps it.
		return []protocol.RichButton{{Text: firstOf(params.DisplayText, params.Title, params.FlowCTA)}}
	}
}

// labelled adds a button that has a label. One without is nothing anybody can read, and a
// card made of nothing but those is not a card.
func labelled(buttons []protocol.RichButton, button protocol.RichButton) []protocol.RichButton {
	if button.Text == "" {
		return buttons
	}
	return append(buttons, button)
}

func firstOf(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

// headerOf is the file at the top of a card, read the way the file of a message is.
//
// What it reads into is a message holding only the header, so the three kinds a header can
// be go through the same renderer that gives a file its mime, name and size. A location or
// a product at the top of a card is not a file, and the card renders without it.
func headerOf(header *waE2E.Message) *attachment {
	part, ok := attachmentOf(header)
	if !ok {
		return nil
	}
	return &part
}

// cardAnswerOf renders the customer's answer to a card: the text of what they tapped,
// quoting the card.
//
// Not a card itself, and not a placeholder: it is the words the customer's phone shows
// them having sent. An answer with no words in it is the placeholder it was before.
func cardAnswerOf(message *waE2E.Message) (body, bool) {
	var text string
	var quoting *waE2E.ContextInfo
	switch {
	case message.GetButtonsResponseMessage() != nil:
		answer := message.GetButtonsResponseMessage()
		text, quoting = answer.GetSelectedDisplayText(), answer.GetContextInfo()
	case message.GetTemplateButtonReplyMessage() != nil:
		answer := message.GetTemplateButtonReplyMessage()
		text, quoting = answer.GetSelectedDisplayText(), answer.GetContextInfo()
	case message.GetListResponseMessage() != nil:
		answer := message.GetListResponseMessage()
		text, quoting = answer.GetTitle(), answer.GetContextInfo()
	case message.GetInteractiveResponseMessage() != nil:
		answer := message.GetInteractiveResponseMessage()
		text, quoting = answer.GetBody().GetText(), answer.GetContextInfo()
	}
	if text == "" {
		return body{}, false
	}
	return body{content: protocol.Text(text), context: quoting}, true
}

// cardBody renders a card on the live path, fetching the file at its top the way the file
// of a message is fetched: a download that may work next time leaves the card on the
// phone, and one that never will publishes the card and says why its header is missing.
//
// Never as view-once. A business wrapping its card in that envelope is how the card gets
// delivered, not a picture meant to disappear, and the phone shows the card like any
// other.
func (s *Session) cardBody(ctx context.Context, event *waEvents.Message, found *card) (body, bool) {
	if found.header == nil {
		return body{content: found.content, context: found.context}, true
	}
	filed, ok := s.fileBody(ctx, event, found.header, false)
	if !ok {
		return body{}, false
	}
	file, _ := filed.content.(protocol.MediaContent)
	found.content.Media = &file
	return body{content: found.content, context: found.context, missing: filed.missing}, true
}
