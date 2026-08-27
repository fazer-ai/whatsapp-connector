package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"time"
)

// MediaPart is what it takes to fetch the file of a message a second time.
//
// A blob is instance-local and time-bounded, so the reference published with an event
// stops working: the instance that downloaded it is replaced, or the cache drops it.
// The client's answer to that is to ask for the file again, and answering needs what
// WhatsApp wants in order to hand the same file over, which is nowhere else once the
// event has been published and the message forgotten.
//
// The fields are named one by one rather than kept as the message whatsmeow decoded.
// That message carries the caption, the preview and whoever was mentioned, and none of
// that is needed to fetch anything: writing the file's coordinates and nothing else is
// what keeps the contents of a conversation out of this table by construction rather
// than by a call to strip them that somebody has to remember.
type MediaPart struct {
	SID       string
	MessageID string
	// ChatKind and ChatID are the chat the message was published under. They are not
	// part of the key -- see PutMediaPart for why -- and they are here so a lookup can
	// tell that the row it found belongs to a different conversation than the one asking
	// for it, and refuse rather than hand over somebody else's file.
	ChatKind string
	ChatID   string
	// Kind is the contract's media kind, and it decides which of whatsmeow's message
	// types the download is rebuilt as, which is how whatsmeow knows the media type.
	Kind string

	DirectPath    string
	MediaKey      []byte
	FileEncSHA256 []byte
	FileSHA256    []byte
	FileLength    int64

	Mime     string
	Filename string

	// StoredAt is when the part was written, in milliseconds, and it is what the sweep
	// reads. Set by PutMediaPart.
	StoredAt int64
}

// PutMediaPart records how to fetch a message's file again, replacing whatever was
// there for the same message.
//
// The newest write wins, and the WHERE is what makes that true rather than "whichever
// statement ran last". A message that arrives twice is the same file both times and the
// later delivery carries the fresher coordinates, so an older write landing afterwards
// has nothing to add and can only take something away.
//
// The key is the session and the message, and the chat is a column rather than part of
// it. Making it part of the key would give a message id in two chats a row each, and the
// client's own command makes `chat` optional -- so a lookup arriving without one would
// have two rows to choose between and no way to choose. Kept as a column, a lookup that
// does bring a chat can tell it found somebody else's row and refuse; one that does not
// is no worse off than before.
//
// That case is not hypothetical: ownership of a session moves between instances, and the
// old owner's handler can still be inside this call when the new one writes. This does
// not fence the write against a lost lease -- it cannot, there is no epoch in this schema
// and adding one is the architecture change the package doc calls open -- it removes the
// harm reordering does here, which is a stale directPath installed over a fresh one and a
// later download answered with a 404.
func (c *Container) PutMediaPart(ctx context.Context, part *MediaPart, now time.Time) error {
	if part.SID == "" || part.MessageID == "" {
		return fmt.Errorf("store: a media part needs a session and a message, got %q and %q", part.SID, part.MessageID)
	}
	// The caller's StoredAt is ignored rather than read: when a row was written is this
	// package's to say, because it is the only thing the retention sweep goes by.
	stamp := now.UnixMilli()

	const upsert = `
		INSERT INTO wac_media_part
			(sid, message_id, chat_kind, chat_id, kind, direct_path, media_key,
			 file_enc_sha256, file_sha256, file_length, mime, filename, stored_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (sid, message_id) DO UPDATE SET
			chat_kind = excluded.chat_kind, chat_id = excluded.chat_id,
			kind = excluded.kind, direct_path = excluded.direct_path, media_key = excluded.media_key,
			file_enc_sha256 = excluded.file_enc_sha256, file_sha256 = excluded.file_sha256,
			file_length = excluded.file_length, mime = excluded.mime, filename = excluded.filename,
			stored_at = excluded.stored_at
		WHERE excluded.stored_at > wac_media_part.stored_at`
	_, err := c.db.ExecContext(ctx, c.rebind(upsert),
		part.SID, part.MessageID, part.ChatKind, part.ChatID, part.Kind, part.DirectPath,
		encode(part.MediaKey), encode(part.FileEncSHA256), encode(part.FileSHA256),
		part.FileLength, part.Mime, part.Filename, stamp)
	if err != nil {
		return fmt.Errorf("store: record how to fetch the file of %s: %w", part.MessageID, err)
	}
	return nil
}

// MediaPart reads back how to fetch one message's file, and whether anything was kept
// for it at all.
func (c *Container) MediaPart(ctx context.Context, sid, messageID string) (MediaPart, bool, error) {
	const query = `
		SELECT chat_kind, chat_id, kind, direct_path, media_key, file_enc_sha256, file_sha256,
		       file_length, mime, filename, stored_at
		FROM wac_media_part WHERE sid = ? AND message_id = ?`

	part := MediaPart{SID: sid, MessageID: messageID}
	var key, encDigest, digest string
	err := c.db.QueryRowContext(ctx, c.rebind(query), sid, messageID).Scan(
		&part.ChatKind, &part.ChatID,
		&part.Kind, &part.DirectPath, &key, &encDigest, &digest,
		&part.FileLength, &part.Mime, &part.Filename, &part.StoredAt)
	if errors.Is(err, sql.ErrNoRows) {
		return MediaPart{}, false, nil
	}
	if err != nil {
		return MediaPart{}, false, fmt.Errorf("store: read how to fetch the file of %s: %w", messageID, err)
	}

	for _, field := range []struct {
		raw  string
		into *[]byte
		name string
	}{
		{key, &part.MediaKey, "media key"},
		{encDigest, &part.FileEncSHA256, "ciphertext digest"},
		{digest, &part.FileSHA256, "file digest"},
	} {
		decoded, err := decode(field.raw)
		if err != nil {
			// A row we wrote that we cannot read back is corruption. Reported rather
			// than treated as a message nobody kept anything for, because the two ask
			// for different things: one is a file that is gone, the other is a store
			// that needs looking at.
			return MediaPart{}, false, fmt.Errorf(
				"store: the %s kept for %s is unreadable: %w", field.name, messageID, err)
		}
		*field.into = decoded
	}
	return part, true, nil
}

// SweepMediaParts drops what was kept for messages older than the cutoff, and reports
// how many rows went.
//
// The table would otherwise grow for the life of the deployment: a row per media message
// ever received, each one holding the key to a file. The cutoff is a retention decision
// rather than a cache one -- see MediaRefetch in the app config for why it is the length
// it is.
func (c *Container) SweepMediaParts(ctx context.Context, before time.Time) (int64, error) {
	result, err := c.db.ExecContext(ctx,
		c.rebind(`DELETE FROM wac_media_part WHERE stored_at < ?`), before.UnixMilli())
	if err != nil {
		return 0, fmt.Errorf("store: drop the media parts kept past their retention: %w", err)
	}
	dropped, err := result.RowsAffected()
	if err != nil {
		// The delete went through; only the count did not come back. Both drivers this
		// connector uses report it, and the interface allows not to, so a sweep that
		// worked is not turned into a failure over how much it says it did.
		return 0, nil //nolint:nilerr // the statement succeeded, only its count is missing
	}
	return dropped, nil
}

// Keys and digests are kept base64 rather than as bytes: the column is then TEXT in both
// dialects, which spares the schema and the driver round trip a difference that buys
// nothing here. Three fields of 32 bytes each is not a size worth a dialect for.
func encode(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	return base64.StdEncoding.EncodeToString(raw)
}

func decode(raw string) ([]byte, error) {
	if raw == "" {
		return nil, nil
	}
	return base64.StdEncoding.DecodeString(raw)
}
