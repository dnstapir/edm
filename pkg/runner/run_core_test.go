package runner

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/spf13/viper"
)

// runCoreTC returns a testConfiger with the bare minimum to walk through
// run() far enough to exercise the next-stage seam. Every error-path test
// starts from this and tweaks one field (or one seam) to break a specific
// stage. Senders are disabled so the test doesn't open MQTT/HTTP cert
// files unless the test explicitly opts in.
func runCoreTC(t *testing.T) testConfiger {
	t.Helper()
	tc := defaultTC
	tc.DataDir = t.TempDir()
	tc.WellKnownDomainsFile = testDawgFile(t, "example.com.")
	tc.InputUnix = filepath.Join(t.TempDir(), "dnstap.sock")
	tc.MinimiserWorkers = 1
	tc.QnameSeenEntries = 1
	tc.CryptopanAddressEntries = 10
	tc.NewQnameBuffer = 1
	tc.DisableHistogramSender = true
	tc.DisableMQTT = true
	tc.DisableSessionFiles = true
	return tc
}

// runCoreCleanup releases the global viper state and ephemeral HTTP-server
// seam each error-path test installs, so subtests do not leak into one
// another. callers register this via t.Cleanup before invoking run().
func runCoreCleanup(t *testing.T) {
	t.Helper()
	t.Cleanup(viper.Reset)
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// pinHTTPServersToEphemeral overrides the pprof/metrics listen-addr seams
// to ephemeral ports plus a listenAndServeHTTP that exits immediately, so
// parallel tests do not collide on :6060/:2112 and the two server
// goroutines never actually bind a port.
func pinHTTPServersToEphemeral(t *testing.T) {
	t.Helper()
	swapSeam(t, &pprofListenAddr, "127.0.0.1:0")
	swapSeam(t, &metricsListenAddr, "127.0.0.1:0")
	swapSeam(t, &listenAndServeHTTP, func(*http.Server) error {
		return http.ErrServerClosed
	})
}

func TestRunCore_ErrorPaths(t *testing.T) {
	t.Run("setIgnoredClientIPs error", func(t *testing.T) {
		runCoreCleanup(t)
		tc := runCoreTC(t)
		tc.IgnoredClientIPsFile = filepath.Join(t.TempDir(), "missing.txt")
		edm := newTestDnstapMinimiser(t, tc)
		err := run(edm, discardLogger(), new(slog.LevelVar))
		if err == nil || !strings.Contains(err.Error(), "ignored client IPs") {
			t.Fatalf("err = %v, want ignored-client-IPs failure", err)
		}
	})

	t.Run("setIgnoredQuestionNames error", func(t *testing.T) {
		runCoreCleanup(t)
		tc := runCoreTC(t)
		tc.IgnoredQuestionNamesFile = filepath.Join(t.TempDir(), "missing.dawg")
		edm := newTestDnstapMinimiser(t, tc)
		err := run(edm, discardLogger(), new(slog.LevelVar))
		if err == nil || !strings.Contains(err.Error(), "ignored question names") {
			t.Fatalf("err = %v, want ignored-question-names failure", err)
		}
	})

	t.Run("openPebble error", func(t *testing.T) {
		runCoreCleanup(t)
		swapSeam(t, &openPebble, func(string, *pebble.Options) (*pebble.DB, error) {
			return nil, errInjected
		})
		tc := runCoreTC(t)
		edm := newTestDnstapMinimiser(t, tc)
		err := run(edm, discardLogger(), new(slog.LevelVar))
		if !errors.Is(err, errInjected) {
			t.Fatalf("err = %v, want errInjected", err)
		}
	})

	t.Run("setupHistogramSender error", func(t *testing.T) {
		runCoreCleanup(t)
		tc := runCoreTC(t)
		tc.DisableHistogramSender = false
		// Missing cert/key files trip loadHTTPClientCert before
		// setupHistogramSender; either way the error chain wraps "HTTP
		// client cert" or "histogram sender".
		tc.HTTPClientCertFile = filepath.Join(t.TempDir(), "missing.crt")
		tc.HTTPClientKeyFile = filepath.Join(t.TempDir(), "missing.key")
		edm := newTestDnstapMinimiser(t, tc)
		err := run(edm, discardLogger(), new(slog.LevelVar))
		if err == nil || !strings.Contains(err.Error(), "HTTP client cert") {
			t.Fatalf("err = %v, want HTTP-client-cert failure", err)
		}
	})

	t.Run("setupMQTT error", func(t *testing.T) {
		runCoreCleanup(t)
		tc := runCoreTC(t)
		tc.DisableMQTT = false
		// Real matching cert+key so loadMQTTClientCert succeeds and the
		// failure surfaces from setupMQTT's missing-signing-key branch.
		certPath, keyPath, _ := testCertFiles(t)
		tc.MQTTClientCertFile = certPath
		tc.MQTTClientKeyFile = keyPath
		tc.MQTTSigningKeyFile = filepath.Join(t.TempDir(), "missing.jwk")
		edm := newTestDnstapMinimiser(t, tc)
		err := run(edm, discardLogger(), new(slog.LevelVar))
		if err == nil || !strings.Contains(err.Error(), "setup mqtt") {
			t.Fatalf("err = %v, want setup mqtt failure", err)
		}
	})

	t.Run("setupDnstapInput no input", func(t *testing.T) {
		runCoreCleanup(t)
		tc := runCoreTC(t)
		tc.InputUnix = ""
		edm := newTestDnstapMinimiser(t, tc)
		err := run(edm, discardLogger(), new(slog.LevelVar))
		if !errors.Is(err, errNoInputConfigured) {
			t.Fatalf("err = %v, want errNoInputConfigured", err)
		}
	})

	t.Run("seen-qname LRU error", func(t *testing.T) {
		runCoreCleanup(t)
		tc := runCoreTC(t)
		tc.QnameSeenEntries = 0
		edm := newTestDnstapMinimiser(t, tc)
		err := run(edm, discardLogger(), new(slog.LevelVar))
		if err == nil || !strings.Contains(err.Error(), "seen-qname LRU") {
			t.Fatalf("err = %v, want seen-qname LRU failure", err)
		}
	})

	t.Run("loadDawgFile error", func(t *testing.T) {
		runCoreCleanup(t)
		pinHTTPServersToEphemeral(t)
		tc := runCoreTC(t)
		tc.WellKnownDomainsFile = filepath.Join(t.TempDir(), "missing.dawg")
		edm := newTestDnstapMinimiser(t, tc)
		err := run(edm, discardLogger(), new(slog.LevelVar))
		if err == nil || !strings.Contains(err.Error(), "loadDawgFile") {
			t.Fatalf("err = %v, want loadDawgFile failure", err)
		}
	})

	t.Run("debug dnstap file open error", func(t *testing.T) {
		runCoreCleanup(t)
		pinHTTPServersToEphemeral(t)
		tc := runCoreTC(t)
		// A path under a regular file (not a directory) makes OpenFile
		// fail with ENOTDIR regardless of the test user's uid.
		blocker := writeTempFile(t, "blocker", []byte("x"))
		tc.DebugDnstapFilename = filepath.Join(blocker, "debug.dnstap")
		edm := newTestDnstapMinimiser(t, tc)
		err := run(edm, discardLogger(), new(slog.LevelVar))
		if err == nil || !strings.Contains(err.Error(), "debug dnstap file") {
			t.Fatalf("err = %v, want debug dnstap file failure", err)
		}
	})
}
