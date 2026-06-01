package main

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"

	"github.com/dnstapir/edm/pkg/cmd"
)

// version set at build time with -ldflags="-X main.version=v0.0.1"
var version = "undefined"

// defaultHostname is used when the real hostname cannot be determined.
const defaultHostname = "dnstapir-edm-hostname-unknown"

// osHostname is a seam so tests can exercise the hostname fallback path.
var osHostname = os.Hostname

// resolveHostname returns the host's name, falling back to defaultHostname
// (and writing a warning to warnW) when the hostname cannot be determined.
func resolveHostname(warnW io.Writer) string {
	hostname, err := osHostname()
	if err != nil {
		fmt.Fprintf(warnW, "unable to get hostname (%v), using '%s'\n", err, defaultHostname)
		return defaultHostname
	}
	return hostname
}

// buildLogger constructs the application logger. It writes JSON to w at the
// level controlled by loggerLevel and tags every record with the service
// name, hostname, Go version and build version. It is factored out of main so
// the logger wiring can be tested without invoking cmd.Execute.
func buildLogger(w io.Writer, loggerLevel *slog.LevelVar, version, hostname string) *slog.Logger {
	logger := slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: loggerLevel}))
	logger = logger.With("service", "dnstapir-edm")
	logger = logger.With("hostname", hostname)
	logger = logger.With("go_version", runtime.Version())
	logger = logger.With("version", version)
	return logger
}

// main wires the hostname and logger helpers together, installs the logger as
// the slog/log default, and hands control to the cobra command tree.
func main() {
	// loggerLevel controls the global logging level for the application
	loggerLevel := new(slog.LevelVar) // Info by default

	// Logger used for all output
	logger := buildLogger(os.Stderr, loggerLevel, version, resolveHostname(os.Stderr))

	// This makes any calls to the standard "log" package to use slog as
	// well
	slog.SetDefault(logger)

	cmd.Execute(logger, loggerLevel)
}
