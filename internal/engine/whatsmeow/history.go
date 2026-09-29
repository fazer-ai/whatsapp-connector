package whatsmeow

import (
	"context"

	wm "go.mau.fi/whatsmeow"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waHistorySync "go.mau.fi/whatsmeow/proto/waHistorySync"
	waTypes "go.mau.fi/whatsmeow/types"
)

// historySliceLimit is how many messages one history.sync event carries at most. A chat
// longer than this arrives in several events, so no stream entry holds a whole dump.
const historySliceLimit = 100

func (s *Session) setHistory(history bool) {
	s.mu.Lock()
	s.history = history
	s.mu.Unlock()
}

func (s *Session) wantsHistory() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.history
}

// downloadHistoryOverClient is the default for the seam of the same name. The storage is
// synchronous so that what whatsmeow keeps from a dump is written before it is receipted.
func downloadHistoryOverClient(ctx context.Context, client *wm.Client, notification *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
	return client.DownloadHistorySync(ctx, notification, true) //nolint:wrapcheck // wrapped by its caller
}

func receiptHistoryOverClient(ctx context.Context, client *wm.Client, id waTypes.MessageID) error {
	return client.SendProtocolMessageReceipt(ctx, id, waTypes.ReceiptTypeHistorySync) //nolint:wrapcheck // wrapped by its caller
}

func sendPeerOverClient(ctx context.Context, client *wm.Client, message *waE2E.Message) error {
	_, err := client.SendPeerMessage(ctx, message)
	return err //nolint:wrapcheck // wrapped by its caller
}
