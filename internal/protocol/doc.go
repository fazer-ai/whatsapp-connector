// Package protocol implements the wire contract between the connector and its
// clients: the frames that travel on Redis Streams and the closed sets of event
// types, command types and error codes they carry.
//
// The contract itself lives in ../../contract and is language neutral: it is the
// single source of truth, vendored by every client (fazer-ai/chatwoot pins a copy
// under spec/fixtures/whatsapp/session/contract). This package is the Go binding
// for it, and contract_test.go is what keeps the two from drifting apart.
package protocol
