<!-- What changes for the operator or for the Chatwoot side, in one short paragraph. -->

## Closes

<!-- Issue / Linear links, or "n/a". -->

## What changed

<!-- Implementation highlights: new events or commands, Redis keys touched, config added. -->

## Protocol impact

<!-- Delete the lines that do not apply. Anything but "none" needs the client side lined up. -->

- [ ] None: no frame changed shape.
- [ ] Additive: new event/command type or new optional field (no PROTOCOL_VERSION bump).
- [ ] Breaking: an existing frame changed meaning or shape (PROTOCOL_VERSION bumped, and the previous major stays supported for one release).
- [ ] `contract/` changed, so clients must re-run their sync task (`rails whatsapp:contract:sync[<ref>]` on the Chatwoot side).

## How to test
