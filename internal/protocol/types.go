package protocol

import "encoding/json"

// EventType is the discriminator of an event frame. The set is closed: an unknown
// type means the peer speaks a newer protocol and the frame must be skipped, not
// guessed at.
type EventType string

// Every event type in the contract, grouped as the catalog groups them.
const (
	EventSessionState          EventType = "session.state"
	EventSessionLoggedOut      EventType = "session.logged_out"
	EventSessionStreamReplaced EventType = "session.stream_replaced"
	EventSessionTemporaryBan   EventType = "session.temporary_ban"
	EventSessionClientOutdated EventType = "session.client_outdated"
	EventSessionConnectFailure EventType = "session.connect_failure"
	// The replay of what a session missed while it was down, bracketed by a preview and
	// a completion. Only a backend whose library receives it can emit these.
	EventSessionOfflineSyncPreview   EventType = "session.offline_sync_preview"
	EventSessionOfflineSyncCompleted EventType = "session.offline_sync_completed"
	EventPairingQR                   EventType = "pairing.qr"
	EventPairingCode                 EventType = "pairing.code"
	EventPairingSuccess              EventType = "pairing.success"
	EventPairingError                EventType = "pairing.error"
	EventPairingPasskeyRequest       EventType = "pairing.passkey_request"
	EventPairingPasskeyConfirmation  EventType = "pairing.passkey_confirmation"
	EventMessageReceived             EventType = "message.received"
	EventMessageReceipt              EventType = "message.receipt"
	EventMessageEdited               EventType = "message.edited"
	EventMessageRevoked              EventType = "message.revoked"
	EventMessageReaction             EventType = "message.reaction"
	EventMediaDownloadFailed         EventType = "media.download_failed"
	EventCommandFailed               EventType = "command.failed"
	EventChatPresence                EventType = "chat.presence"
	EventPresenceUpdate              EventType = "presence.update"
	EventContactPictureChanged       EventType = "contact.picture_changed"
	EventContactIdentityChanged      EventType = "contact.identity_changed"
	EventGroupJoined                 EventType = "group.joined"
	EventGroupUpdated                EventType = "group.updated"
	EventGroupPictureChanged         EventType = "group.picture_changed"
	EventGroupActivity               EventType = "group.activity"
	EventAccountReachoutTimelock     EventType = "account.reachout_timelock"
	EventAccountNewChatCap           EventType = "account.new_chat_cap"
	EventCallOffer                   EventType = "call.offer"
	EventCallTerminate               EventType = "call.terminate"
	EventHistorySync                 EventType = "history.sync"
	EventRaw                         EventType = "raw"
)

// CommandType is the discriminator of a command frame.
type CommandType string

// Every command type in the contract.
const (
	CommandSessionConnect          CommandType = "session.connect"
	CommandSessionDisconnect       CommandType = "session.disconnect"
	CommandSessionLogout           CommandType = "session.logout"
	CommandSessionDelete           CommandType = "session.delete"
	CommandSessionUpdate           CommandType = "session.update"
	CommandSessionStatus           CommandType = "session.status"
	CommandSessionWake             CommandType = "session.wake"
	CommandAdminPing               CommandType = "admin.ping"
	CommandPairingRequestCode      CommandType = "pairing.request_code"
	CommandPairingPasskeyResponse  CommandType = "pairing.passkey_response"
	CommandPairingPasskeyConfirm   CommandType = "pairing.passkey_confirm"
	CommandMessageSend             CommandType = "message.send"
	CommandMessageEdit             CommandType = "message.edit"
	CommandMessageRevoke           CommandType = "message.revoke"
	CommandMessageReact            CommandType = "message.react"
	CommandMessageMarkRead         CommandType = "message.mark_read"
	CommandMessageMarkUnread       CommandType = "message.mark_unread"
	CommandMessageDownloadMedia    CommandType = "message.download_media"
	CommandHistoryRequest          CommandType = "history.request"
	CommandPresenceSet             CommandType = "presence.set"
	CommandPresenceSubscribe       CommandType = "presence.subscribe"
	CommandChatPresence            CommandType = "chat.presence"
	CommandContactCheck            CommandType = "contact.check"
	CommandContactProfilePicture   CommandType = "contact.profile_picture"
	CommandContactInfo             CommandType = "contact.info"
	CommandContactResolve          CommandType = "contact.resolve"
	CommandGroupCreate             CommandType = "group.create"
	CommandGroupInfo               CommandType = "group.info"
	CommandGroupList               CommandType = "group.list"
	CommandGroupLeave              CommandType = "group.leave"
	CommandGroupParticipantsUpdate CommandType = "group.participants.update"
	CommandGroupNameSet            CommandType = "group.name.set"
	CommandGroupDescriptionSet     CommandType = "group.description.set"
	CommandGroupPhotoSet           CommandType = "group.photo.set"
	CommandGroupSettingsSet        CommandType = "group.settings.set"
	CommandGroupInviteGet          CommandType = "group.invite.get"
	CommandGroupJoinRequestsList   CommandType = "group.join_requests.list"
	CommandGroupJoinRequestsUpdate CommandType = "group.join_requests.update"
	CommandCallReject              CommandType = "call.reject"
)

// AllEventTypes lists every event type this build knows how to produce.
var AllEventTypes = []EventType{
	EventSessionState,
	EventSessionLoggedOut,
	EventSessionStreamReplaced,
	EventSessionTemporaryBan,
	EventSessionClientOutdated,
	EventSessionConnectFailure,
	EventSessionOfflineSyncPreview,
	EventSessionOfflineSyncCompleted,
	EventPairingQR,
	EventPairingCode,
	EventPairingSuccess,
	EventPairingError,
	EventPairingPasskeyRequest,
	EventPairingPasskeyConfirmation,
	EventMessageReceived,
	EventMessageReceipt,
	EventMessageEdited,
	EventMessageRevoked,
	EventMessageReaction,
	EventMediaDownloadFailed,
	EventCommandFailed,
	EventChatPresence,
	EventPresenceUpdate,
	EventContactPictureChanged,
	EventContactIdentityChanged,
	EventGroupJoined,
	EventGroupUpdated,
	EventGroupPictureChanged,
	EventGroupActivity,
	EventAccountReachoutTimelock,
	EventAccountNewChatCap,
	EventCallOffer,
	EventCallTerminate,
	EventHistorySync,
	EventRaw,
}

// AllCommandTypes lists every command type this build knows how to execute.
var AllCommandTypes = []CommandType{
	CommandSessionConnect,
	CommandSessionDisconnect,
	CommandSessionLogout,
	CommandSessionDelete,
	CommandSessionUpdate,
	CommandSessionStatus,
	CommandSessionWake,
	CommandAdminPing,
	CommandPairingRequestCode,
	CommandPairingPasskeyResponse,
	CommandPairingPasskeyConfirm,
	CommandMessageSend,
	CommandMessageEdit,
	CommandMessageRevoke,
	CommandMessageReact,
	CommandMessageMarkRead,
	CommandMessageMarkUnread,
	CommandMessageDownloadMedia,
	CommandHistoryRequest,
	CommandPresenceSet,
	CommandPresenceSubscribe,
	CommandChatPresence,
	CommandContactCheck,
	CommandContactProfilePicture,
	CommandContactInfo,
	CommandContactResolve,
	CommandGroupCreate,
	CommandGroupInfo,
	CommandGroupList,
	CommandGroupLeave,
	CommandGroupParticipantsUpdate,
	CommandGroupNameSet,
	CommandGroupDescriptionSet,
	CommandGroupPhotoSet,
	CommandGroupSettingsSet,
	CommandGroupInviteGet,
	CommandGroupJoinRequestsList,
	CommandGroupJoinRequestsUpdate,
	CommandCallReject,
}

// readOnlyCommands are the commands that ask a question and change nothing. They are
// the ones a redelivery has to carry out again rather than be answered from a record:
// what a session's state was a minute ago is not what it is now, and handing back the
// old answer is worse than doing the work twice.
//
// Membership here is by type, and one command's type is not enough to place it:
// `group.invite.get` reads the invite code with `revoke` unset and rotates it when the
// field is true, so it is listed and then asked about its payload below.
var readOnlyCommands = map[CommandType]bool{
	CommandSessionStatus: true,
	CommandAdminPing:     true,
	// It writes a blob and spends a download, so it is not free -- but carrying it out
	// twice is not different from carrying it out once, and that is what this map is
	// about. Its answer is the one result in the contract that goes stale by
	// construction: a reference to a blob on one instance, good until a TTL. Remembered
	// and handed to a redelivery, it is an address that answers nothing, on the very
	// path that exists to recover an attachment. Doing the work again is cheaper than
	// that, and bounded.
	CommandMessageDownloadMedia:  true,
	CommandContactCheck:          true,
	CommandContactProfilePicture: true,
	CommandContactInfo:           true,
	CommandContactResolve:        true,
	CommandGroupInfo:             true,
	CommandGroupList:             true,
	CommandGroupInviteGet:        true,
	CommandGroupJoinRequestsList: true,
}

// ChangesSomething reports whether carrying this command out twice is different from
// carrying it out once. Everything the contract does not name as a question is assumed
// to change something, so a command added without thinking about it is deduplicated
// rather than repeated.
//
// It reads the payload rather than only the type, because `group.invite.get` is both:
// the read hands back the group's current invite code, and `revoke: true` rotates it
// first, so a redelivery of that one rotates a code nobody has seen yet and answers
// with a different one than the reply that was lost.
func (c *Command) ChangesSomething() bool {
	if !readOnlyCommands[c.Type] {
		return true
	}
	if c.Type == CommandGroupInviteGet {
		var body struct {
			Revoke bool `json:"revoke"`
		}
		// An unreadable payload is not a question. It fails on the way to the engine
		// either way, and the assumption that keeps a redelivery from repeating work
		// is the one that costs nothing when it is wrong.
		if err := json.Unmarshal(c.Payload, &body); err != nil {
			return true
		}
		return body.Revoke
	}
	return false
}

// messageIDKeyed are the commands whose `message_id` names the message the command
// itself puts on the wire. Those are the ones the contract remembers as
// `msg:<message_id>`, and the id being the caller's own is what makes the key hold
// across frames rather than only across redeliveries of one.
//
// `message.download_media` is deliberately absent even though its payload is required
// to carry a `message_id`: there the field names a message that already exists, so
// keying by it borrows the namespace of the send that created it, and a client asking
// for the media of a message it sent would be answered with that send's result.
var messageIDKeyed = map[CommandType]bool{
	CommandMessageSend:  true,
	CommandMessageEdit:  true,
	CommandMessageReact: true,
}

// NamesItsOwnMessage reports whether a `message_id` in this command's payload is the id
// of the message the command creates.
func (t CommandType) NamesItsOwnMessage() bool { return messageIDKeyed[t] }

// rpcCommands are the commands whose caller blocks on a reply. Everything else is
// fire and forget: the caller learns about failures through command.failed.
var rpcCommands = map[CommandType]bool{
	CommandSessionConnect:          true,
	CommandSessionStatus:           true,
	CommandSessionUpdate:           true,
	CommandAdminPing:               true,
	CommandMessageSend:             true,
	CommandMessageEdit:             true,
	CommandMessageRevoke:           true,
	CommandMessageReact:            true,
	CommandMessageDownloadMedia:    true,
	CommandHistoryRequest:          true,
	CommandContactCheck:            true,
	CommandContactProfilePicture:   true,
	CommandContactInfo:             true,
	CommandContactResolve:          true,
	CommandGroupCreate:             true,
	CommandGroupInfo:               true,
	CommandGroupList:               true,
	CommandGroupLeave:              true,
	CommandGroupParticipantsUpdate: true,
	CommandGroupNameSet:            true,
	CommandGroupDescriptionSet:     true,
	CommandGroupPhotoSet:           true,
	CommandGroupSettingsSet:        true,
	CommandGroupInviteGet:          true,
	CommandGroupJoinRequestsList:   true,
	CommandGroupJoinRequestsUpdate: true,
}

// IsRPC reports whether a command expects a reply on wa:reply:<command id>.
func IsRPC(t CommandType) bool { return rpcCommands[t] }

// Valid reports whether the type is part of this build's catalog.
func (t EventType) Valid() bool {
	for _, known := range AllEventTypes {
		if known == t {
			return true
		}
	}
	return false
}

// Valid reports whether the type is part of this build's catalog.
func (t CommandType) Valid() bool {
	for _, known := range AllCommandTypes {
		if known == t {
			return true
		}
	}
	return false
}
