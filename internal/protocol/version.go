package protocol

// Version is the major version of the protocol this build speaks. It mirrors
// contract/PROTOCOL_VERSION, which contract_test.go asserts.
//
// Additive changes (a new event type, a new optional field) do not bump it. It is
// bumped only when an existing frame changes meaning or shape, and a connector is
// expected to keep serving Version and Version-1 for one release cycle.
const Version = 1

// MinVersion is the oldest protocol version this build still accepts from a peer.
// A connector advertises [MinVersion, Version] on wa:instance:<id> and a client
// refuses to talk to it when the two ranges do not overlap.
const MinVersion = 1
