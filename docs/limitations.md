# Known limitations

What WhatsApp does not let this connector do, measured against real accounts.

## What `calls.auto_reject` does not silence

A caller on WhatsApp Web ignores the refusal. The connector writes the same node it writes
for anybody else, WhatsApp routes it, and the browser on the other end goes on ringing.

Twenty-seven calls to a paired account, with every node in both directions captured, divide
on one attribute of the offer and on nothing else -- the caller's `platform`:

| caller | calls | ended by the refusal |
| --- | --- | --- |
| `android` | 22 | every one that had a refusal written into it, in 0.4s to 1.6s |
| `web` | 5 | none |

The two web calls that were left to run ended on their own: one rang its full 89.8s and
ended `timeout`, the other at 11s when the caller hung up. The `call-creator` on a web
offer carries a device suffix and on an Android offer does not, which is how the two tell
apart in a capture, but nothing this connector writes changes the outcome either way.

It matters to whoever reads the inbox rather than to the code: with the policy on, the
operator's phone still rings for somebody who dialled from a browser, and nothing in the
events tells that case apart. Most callers to an inbox dial from a phone, so the policy
does what it says nearly always, and "nearly" is the part worth knowing before somebody
reports it as a bug.

## What an account under Coexistence does not have

A number running [Coexistence](https://docs.360dialog.com/docs/resources/phone-numbers/coexistence),
which is the WhatsApp Business app and the Cloud API on the same number, does not do voice
or video calls, group chats, broadcast lists, disappearing messages, view-once messages,
live location or the catalog. That is the platform's rule and not this connector's: the
call never reaches any linked device, including WhatsApp's own web client, so `call.offer`
is never published and `calls.auto_reject` has nothing to refuse.

Measured on a paired Coexistence account with every node dumped: not one `<offer>` arrived,
across every call placed to it. What did arrive was the other half of the exchange -- two
`<reject>` broadcasts naming calls this device was never offered, one of them
`reason="busy"` -- which is WhatsApp telling a linked device about a call refused somewhere
it could not see. Somebody reading that capture will find call traffic and should not read
it as the offer having been missed.

Nothing on the wire tells such a number apart. Its `<pair-success>` carries the same
`platform` as any other business account and a `<biz>` with only a name, so the connector
cannot detect it and does not guess: inferring it from an absence would mark a healthy new
inbox as limited. A client that onboarded the number knows which path it came in by, and
that is where the warning belongs.

## Calls the connector does not carry

A session connected with `proxy` does not carry the voice of calls, whatever its `calls.answer` says: the call commands are answered `unsupported` and `call.offer` comes without `sdp`. The voice of a call goes to WhatsApp's relays over UDP sockets meowcaller opens itself, which no HTTP or SOCKS5 proxy here carries, so the call would leave from the connector's own address, the one the proxy exists to keep out of it.

Another linked device of the same account can end a call before anybody answers it. Measured on the live bench of #383: a linked device of the called account declined every call 175 ms after the offer, and WhatsApp then ended the call for every device, the caller's phone included, with a terminate whose reason is `rejected_elsewhere`, 0.4 s after the offer. The connector publishes that `call.terminate`, and there is nothing for it to answer. An account whose calls an agent is meant to take should not have a linked device that refuses calls on its own, such as another integration with its own auto-reject.

A call the connector places rings the contact's phone and none of the contact's linked devices: WhatsApp Desktop and WhatsApp Web stay silent, and only the phone can answer. The offer is addressed to the phone alone on purpose (meowcaller patch 0003). With the offer sent to every device, a Desktop or Web left open preaccepts first, and after the phone answers none of the phone's audio reaches the connector. Measured in #380 on 2026-10-07, and again on the bench of #383 on 2026-10-10: a contact with WhatsApp Web open saw the connector's calls on the phone only, while an ordinary call from another phone rang that same Web client. Calls the contact places are a different path: one placed from the phone of a contact with WhatsApp Web open was answered with voice both ways on the same bench.

## Voice notes that are not opus

A voice note goes out as one only when it is opus in an Ogg container. Anything else sent with `voice_note` (an MP3, the AAC a Safari recording produces) goes out as an ordinary audio file, without the voice bubble and its waveform. WhatsApp's Android client refuses to play any other format as a voice note ("something is wrong with the audio file") while WhatsApp Web plays it, and the connector does not transcode: it is a static binary with no audio encoder, and transcoding on every send is a per-message cost the Chatwoot side already turned down. The previous generation, baileys-api, ran every voice note through ffmpeg instead. Measured in #396 on 2026-10-12 with an MP3 recorded in Chatwoot's dashboard.

## A media download that keeps failing for an unknown reason

A media message whose file fails to download with an error the connector does not recognise as permanent is not acknowledged, so WhatsApp delivers it again and the download is retried then. The failures known to be final (the file expired, the file is off the CDN, the bytes do not match the message, the message names no file) publish the message with the file marked unavailable instead. An unrecognised error that repeats on every delivery keeps the message unacknowledged for as long as it repeats, and WhatsApp drops the socket over an unacknowledged message about fifty minutes after delivering it, which redelivers it and starts the cycle again. Making such a failure final needs a count of attempts per message that survives a reconnect, which the connector does not keep, and no such case has been seen. Messages this build cannot address at all (a chat or sender on a server the contract does not name, a stanza with no id) are acknowledged and dropped with a warning instead, since no redelivery changes that answer (#403).
