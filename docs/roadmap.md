# Roadmap

## What this build does

> [!IMPORTANT]
> **Status: M0 to M3, M5 and M6 are in; M4 is partial (see the table below).** A session pairs with a real WhatsApp account, resumes
> across restarts, publishes the text messages that arrive on it, and sends text back:
> quotes, mentions and the chat's disappearing-message timer included. `groups` on the
> connect decides whether group chats come with them, and a group is created, read, renamed, given a description, a photo and its settings, left, and its participants and join requests administered; a contact is checked for an account, asked for its profile and its picture; a call the account is rung for reaches the inbox, and is refused as it arrives when the connect asked for that, or answered with its voice carried to the client's browser when the connect asked for `calls.answer`; a call is placed to a phone number the same way. An inbound image, video, audio,
> document or sticker is downloaded as it arrives, kept in this instance's blob cache and
> published as a reference the client fetches over HTTP; a file WhatsApp will not serve
> again is announced with `media.download_failed` so the bubble says the attachment is
> unavailable rather than loading forever. A blob lives on the instance that downloaded it
> and for a bounded time, and `message.download_media` fetches the file again from the
> coordinates kept beside the message, so an attachment survives the instance being
> replaced between the event and the client's fetch. When WhatsApp has dropped the file
> but the sender's phone may still hold it, that same command asks the phone to upload it
> again, and the failure that announced it says `recoverable` so a client knows to ask.
>
> Outbound, a media message names a URL this connector fetches, with whatever headers open
> it, and streams to WhatsApp without holding the file in memory; a location goes out as a
> pin with the name and the street beside it, and contacts as a card or a stack of them,
> with the vCard written here when the caller only has a name and a number. Anything the
> caller's own address answers is separated into what it has to fix and what is worth
> another go, because a client told the wrong one either retries forever or gives up on a
> file that would have arrived.
>
> A file sent to be seen once is announced as unavailable and never kept: a blob is served
> for as long as anybody keeps asking for it, so storing one would turn something the
> sender expected to disappear into something the account holds indefinitely. WhatsApp
> usually does not hand one to a linked device at all, and what it sends instead reaches
> the inbox as a placeholder, asked of the phone first and published on its own if the
> phone never answers -- how long to wait before giving up on it is still a guess
> ([#51](https://github.com/fazer-ai/whatsapp-connector/issues/51)). What this build
> cannot render is left unacknowledged on WhatsApp's side, with its plaintext buffered so
> the redelivery can still be read: the
> account keeps the message and delivers it again once there is somewhere to put it. A
> number paired on this build therefore still accumulates a backlog of everything it cannot
> render (see [Milestones](#milestones)).
>
> Around the messages, what a conversation looks like while nobody is writing one: the
> ticks a message collects on their way to being read, a read mark this account can set
> on somebody else's, the typing and recording indicators both ways, and whether a
> contact is at their phone. A `composing` or a `recording` is the one shape here that
> describes a moment rather than a fact, and it is the only thing this connector drops
> when it goes stale instead of retrying it: delivered a minute late it is somebody shown
> typing who stopped long ago, and the state that would have corrected it went out while
> the stale one was still on its way. The stop that ends a burst is not like that, and
> neither is an availability -- both hold until something says otherwise. WhatsApp forgets
> both on a reconnect, so the session remembers the availability it was asked for and puts
> it back as soon as a connection comes up. Subscriptions are deliberately not reapplied:
> which parties are worth watching is the client's to know, and `session.state: open` is
> where it re-establishes them. The availability is kept next to the account rather than
> in the session, so an instance that takes the account over puts back the state its
> client asked for without having heard the command.
>
> A message is acknowledged to WhatsApp only after its event reaches the stream, so
> losing Redis costs a redelivery and never a message. The client deduplicates on the
> message id, which is what makes that trade safe. A send is answered from the other
> end of the same trade: what a command did is remembered under
> `wa:idem:<sid>:<key>`, so a redelivery is answered with the first run's result
> instead of being carried out again.

## Milestones

| Milestone | Scope |
|---|---|
| **M0** ✅ | Skeleton, Redis Streams transport, lease/ownership port, fake engine, health and metrics, Docker image, publish pipeline |
| **M1** ✅ | whatsmeow engine: QR and code pairing, session state, logout/ban/outdated handling, the device store. A session that has been handed on writes nothing more: every write a device can make is refused from the moment this instance stops owning it, whichever context it arrives with, and the fence asks the lease rather than this instance's own belief -- so a claim that ran out while nobody was looking stops the writes too. Reconnect backoff is still open |
| **M2** ✅ | Messages in and out (text, media, location, contact, reaction, edit, revoke, quoted, mentions), receipts, read marks, chat presence, account presence, idempotent sends. All of them are in both ways, and a body this build has no arm for arrives as a placeholder rather than disappearing, and one WhatsApp masked from every linked device says so rather than reading as a type this build cannot render. What it leaves behind is in the issues rather than here: a presence state is dropped when the publisher has stopped answering and the queue is full ([#47](https://github.com/fazer-ai/whatsapp-connector/issues/47)), and one delayed across a reconnect is published as if it were fresh ([#49](https://github.com/fazer-ai/whatsapp-connector/issues/49)) |
| **M3** ✅ | Groups, contacts and calls. A group is created, read, listed and left, renamed, given a description, a photo and its settings (`announce`, `locked`, `join_approval`, `member_add_mode`), and its participants added, removed, promoted and demoted; its invite link is served and its join requests are listed and answered. What changes about a group while the connection is up is published rather than waiting for the next reconnect. A contact is checked for an account, asked for its profile and its picture, and resolved to the address the wire uses. A call is published as it arrives and again when it ends, announced once however many times WhatsApp announces it, and `calls.auto_reject` on the connect has the connector refuse it without the account ever ringing -- the offer still goes out, because somebody rang either way. With `calls.answer` the voice goes both ways: a `call.offer` carries the SDP a browser answers with `call.accept`, `call.start` places a call from the browser's offer, and either side ends it with `call.terminate`; the media runs between WhatsApp's relays and the browser through this instance, over the UDP port it opens for calls ([limitations](limitations.md#calls-the-connector-does-not-carry)). What a connect asks for is remembered beside the account, so an instance that brings it back puts back the group traffic and the call policy its client asked for rather than half of them |
| **M4** | Multi-instance under load, quarantine, metrics/lag/DLQ, operations docs. Quarantine is in: a session that keeps failing to start is held out of the retry cadence instead of being dialled at the same rate forever. The six metrics this exposes count what one instance did (sessions running, events published, command duration, leases lost, and whether commands are still being read) and none of them measure the fleet: how far behind a client's consumer group is, how many commands are pending, and what happened to one that no instance could carry out. Operations docs are [operations.md](operations.md) and nothing else |
| **M5** ✅ | Pairing code ✅, the passkey relay ✅ (WhatsApp's challenge is handed to the operator's client, the assertion it signs is handed back, and the confirmation code is published for the operator to read off their phone) and a **per-session proxy** ✅ ([#217](https://github.com/fazer-ai/whatsapp-connector/issues/217)): a connect naming `http`, `https` or `socks5` sends that session's traffic with WhatsApp through it, remembers it for the resume, and never falls back to connecting directly |
| **M6** ✅ | Opt-in history sync. A connect with `history_sync` publishes the phone's dumps as `history.sync`, one chat per event and at most 100 messages each, oldest first, media without a file; the dump is receipted to the phone only after it was published. `history.request` asks the phone for what came before a message the client names. Whether the phone sends its whole history is decided per session at pairing, by the same flag |

## Out of scope

**WhatsApp channels (newsletters) and status are not part of the product.** Decided on 2026-10-05. Chatwoot opens no conversation for either (`Address#ignorable?` covers `status`, `broadcast` and `newsletter`), so a capability built here for them would have no consumer. What this connector already does with them stays as it is: a send to a channel goes out as text, and media to one is refused rather than sent broken. Four issues were closed as not planned for this reason, each with its design still written down in case the scope changes: media to a channel ([#28](https://github.com/fazer-ai/whatsapp-connector/issues/28)), a reaction to a channel post ([#34](https://github.com/fazer-ai/whatsapp-connector/issues/34)) or to a status ([#36](https://github.com/fazer-ai/whatsapp-connector/issues/36)), and who deleted a channel post ([#39](https://github.com/fazer-ai/whatsapp-connector/issues/39)). This changes when Chatwoot starts opening conversations for channels or status, and not before.
