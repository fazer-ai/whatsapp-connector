# Browser call console

This example pairs a whatsmeow client by QR code and exposes meowcaller's audio,
H.264 video, independent camera controls, orientation, and call reactions in a
localhost browser UI.

When idle, the console can start a direct call, an ad-hoc multi-person call, or
a group-JID-bound call. Enter WhatsApp numbers or LIDs in the **People** box,
separated by commas or newlines, or enter a `@g.us` group ID in the group field.
Audio and video starts remain separate controls.

During an established call, use the People box and choose **Add people**. The
event log reports each invite independently, then waits for the authoritative
WhatsApp roster. `group_state` shows each transaction, and `participant_join` is
emitted only when the target is connected with a selected PID-bearing device.
The persistent participant list also shows invited/disconnected users and offers
a **Ring** action for users already in the roster. Ring is distinct from adding a
new participant.

The call-link controls create server-issued audio/video links with one click.
The generated URL has direct copy and join actions; the separate existing-link
field is only for previewing or joining a token/URL created elsewhere. Admins can
toggle approval and approve or reject each waiting user from the rendered
waiting-room list. A joining client remains in the waiting-room phase without
microphone, speaker, or relay media until an authoritative admission update
arrives.

In an active group call, the console can raise/lower the local hand, display
other participants' hand state, send fixed or arbitrary Unicode emoji reactions,
and start/stop display sharing. Screen sharing requires video signaling first.
Stopping display capture restores the previous camera state and does not end or
downgrade the call. Display frames currently use the established H.264 video
sender. This matches the captured non-dual-stream mode: camera, display, and
restored camera preserve the same SSRC and RTP sequence timeline, with a fresh
IDR required at each source switch.

See [the capture-backed feature notes](../../docs/whatsapp-group-call-features.md)
for API examples, observed protocol boundaries, and remaining live-validation
work.

```sh
go run .
```

Open the printed `http://127.0.0.1:...` URL. On first run, scan the QR code from
WhatsApp under **Linked devices**. The SQLite session stays in this directory for
later runs.

Use `go run . -diagdump ./capture` to record sensitive call diagnostics for local
protocol research. Do not share those captures without reviewing their contents.
