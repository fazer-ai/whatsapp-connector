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
