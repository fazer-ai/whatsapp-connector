package store

import signallog "go.mau.fi/libsignal/logger"

// libsignal keeps one package-level logger and fills it in lazily, on the first log
// line, with no synchronisation: two goroutines creating a device at the same time
// race on that write, which the race detector catches on the very first parallel test.
//
// Installing one up front closes the race, and installing a silent one is the policy
// we want anyway. The library's own default prints every debug line to stdout with its
// level filter commented out, which would put Signal session internals in the log
// unstructured, past the redacting logger, and against the rule that auth state is
// never logged.
func init() {
	var silent signallog.Loggable = quietSignalLogger{}
	signallog.Setup(&silent)
}

// quietSignalLogger drops everything libsignal has to say. What matters to an operator
// reaches the log through whatsmeow and this connector, both of which redact.
type quietSignalLogger struct{}

func (quietSignalLogger) Debug(_, _ string)   {}
func (quietSignalLogger) Info(_, _ string)    {}
func (quietSignalLogger) Warning(_, _ string) {}
func (quietSignalLogger) Error(_, _ string)   {}
func (quietSignalLogger) Configure(_ string)  {}
