# WhatsApp session protocol (v1)

Canonical contract between the Go connector and any client of it. It is the single
source of truth for two consumers:

- `whatsapp-connector` (this repo): produces events, consumes commands.
- `fazer-ai/chatwoot` (`app/services/whatsapp/session/`): consumes events, produces
  commands, and translates hosted APIs (Uazapi today) into these same shapes.

The Chatwoot side vendors a copy under `spec/fixtures/whatsapp/session/contract/`,
pinned by `CONTRACT_REF`. CI on both sides fails when the copies drift.

## Files

| Path | Content |
|---|---|
| `PROTOCOL.md` | The protocol itself: transport, compatibility, conventions, RPC results. Vendored and counted in the checksum |
| `PROTOCOL_VERSION` | Current major. Bumped only on a breaking change |
| `schema/protocol.schema.json` | JSON Schema (draft-07) for every frame: `#/definitions/event`, `#/definitions/command`, `#/definitions/reply` |
| `fixtures/events/*.json` | One golden frame per event type (plus variants that exercise tricky shapes) |
| `fixtures/commands/*.json` | One golden frame per command type |

## The normative half

`PROTOCOL.md`, beside this file, holds the protocol itself: the transport, the
compatibility rule, the conventions and the RPC result shapes. It is there and not here
because a client vendors this directory and the sync drops this file: anything written
here is invisible to the side the contract is addressed to, which is how a client
obligation once shipped where no client could read it (#222, #223).

So the split is not cosmetic. **This file is for orientation; every obligation the
contract places on the consuming side belongs in `PROTOCOL.md`**, and
`internal/protocol/contract_prose_test.go` fails the suite when one lands here instead.
