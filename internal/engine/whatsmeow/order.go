package whatsmeow

import "time"

// orderDetailsWait bounds how long a live catalog order waits for its items. The query is
// one IQ that WhatsApp answers in well under a second, and a message behind it waits too.
const orderDetailsWait = 5 * time.Second
