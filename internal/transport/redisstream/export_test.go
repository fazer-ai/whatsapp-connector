package redisstream

// ReadBackScript is the hash of the script a read runs to read this consumer's history
// back, for the tests that have to slow that one trip and nothing else.
var ReadBackScript = pendingPastScript.Hash()
