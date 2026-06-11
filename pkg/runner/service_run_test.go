package runner

import (
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
)

// runCoreTC returns a testConfiger with the bare minimum to walk through
// Run far enough to exercise the next-stage dependency. Every error-path test
// starts from this and tweaks one field or dependency to break a specific
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

// runCoreCleanup releases the global viper state each error-path test touches,
// so subtests do not leak into one another. Callers register this via t.Cleanup
// before invoking Run.
func runCoreCleanup(t *testing.T) {
	t.Helper()
	t.Cleanup(viper.Reset)
}

type httpServerRunnerFunc func(*http.Server) error

func (fn httpServerRunnerFunc) ListenAndServeHTTP(server *http.Server) error {
	return fn(server)
}

type seenQnameStoreFactoryFunc func(string) (seenQnameStore, error)

func (fn seenQnameStoreFactoryFunc) OpenSeenQnameStore(path string) (seenQnameStore, error) {
	return fn(path)
}

// pinHTTPServersToEphemeral overrides the pprof/metrics listen addresses
// to ephemeral ports plus an HTTP runner stub that exits immediately,
// so parallel tests do not collide on :6060/:2112 and the two server
// goroutines never actually bind a port.
//
// The stub also signals via a buffered channel each time it is called so
// the test can wait for both spawned goroutines to call the injected runner.
func pinHTTPServersToEphemeral(t *testing.T, edm *DnstapMinimiser) {
	t.Helper()
	edm.deps.PprofListenAddr = "127.0.0.1:0"
	edm.deps.MetricsListenAddr = "127.0.0.1:0"
	exited := make(chan struct{}, 2)
	edm.deps.HTTPServerRunner = httpServerRunnerFunc(func(*http.Server) error {
		exited <- struct{}{}
		return http.ErrServerClosed
	})
	t.Cleanup(func() {
		// A subtest that returned before pprof/metrics spawned sends nothing;
		// the short timeout keeps the cleanup snappy in that case.
		for range 2 {
			select {
			case <-exited:
			case <-time.After(time.Second):
				return
			}
		}
	})
}

func TestRunCore_ErrorPaths(t *testing.T) {
	t.Run("setIgnoredClientIPs error", func(t *testing.T) {
		runCoreCleanup(t)
		tc := runCoreTC(t)
		tc.IgnoredClientIPsFile = filepath.Join(t.TempDir(), "missing.txt")
		edm := newTestDnstapMinimiser(t, tc)
		err := edm.Run(t.Context())
		if err == nil || !strings.Contains(err.Error(), "ignored client IPs") {
			t.Fatalf("err = %v, want ignored-client-IPs failure", err)
		}
	})

	t.Run("setIgnoredQuestionNames error", func(t *testing.T) {
		runCoreCleanup(t)
		tc := runCoreTC(t)
		tc.IgnoredQuestionNamesFile = filepath.Join(t.TempDir(), "missing.dawg")
		edm := newTestDnstapMinimiser(t, tc)
		err := edm.Run(t.Context())
		if err == nil || !strings.Contains(err.Error(), "ignored question names") {
			t.Fatalf("err = %v, want ignored-question-names failure", err)
		}
	})

	t.Run("seen-qname store open error", func(t *testing.T) {
		runCoreCleanup(t)
		tc := runCoreTC(t)
		edm := newTestDnstapMinimiser(t, tc)
		edm.deps.SeenQnameStoreFactory = seenQnameStoreFactoryFunc(func(string) (seenQnameStore, error) {
			return nil, errInjected
		})
		err := edm.Run(t.Context())
		if !errors.Is(err, errInjected) {
			t.Fatalf("err = %v, want errInjected", err)
		}
	})

	t.Run("histogram sender enabled: loadHTTPClientCert error", func(t *testing.T) {
		// This test exercises the histogram-sender-enabled prefix: with
		// DisableHistogramSender=false, Run calls loadHTTPClientCert
		// before setupHistogramSender. Missing cert/key paths trip
		// loadHTTPClientCert, so the returned error is the
		// "HTTP client cert" wrap — setupHistogramSender itself never
		// runs. The test name and assertion reflect that.
		runCoreCleanup(t)
		tc := runCoreTC(t)
		tc.DisableHistogramSender = false
		tc.HTTPClientCertFile = filepath.Join(t.TempDir(), "missing.crt")
		tc.HTTPClientKeyFile = filepath.Join(t.TempDir(), "missing.key")
		edm := newTestDnstapMinimiser(t, tc)
		err := edm.Run(t.Context())
		if err == nil || !strings.Contains(err.Error(), "HTTP client cert") {
			t.Fatalf("err = %v, want HTTP-client-cert failure", err)
		}
	})

	t.Run("setupHistogramSender error", func(t *testing.T) {
		// Real matching cert+key so loadHTTPClientCert succeeds and the
		// failure surfaces from setupHistogramSender itself (here via an
		// unparseable HTTPURL).
		runCoreCleanup(t)
		tc := runCoreTC(t)
		tc.DisableHistogramSender = false
		certPath, keyPath, _ := testCertFiles(t)
		tc.HTTPClientCertFile = certPath
		tc.HTTPClientKeyFile = keyPath
		tc.HTTPURL = "://bad-url"
		edm := newTestDnstapMinimiser(t, tc)
		err := edm.Run(t.Context())
		if err == nil ||
			!strings.Contains(err.Error(), "histogram sender") {
			t.Fatalf("err = %v, want histogram-sender failure", err)
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
		err := edm.Run(t.Context())
		if err == nil || !strings.Contains(err.Error(), "setup mqtt") {
			t.Fatalf("err = %v, want setup mqtt failure", err)
		}
	})

	t.Run("setupDnstapInput no input", func(t *testing.T) {
		runCoreCleanup(t)
		tc := runCoreTC(t)
		tc.InputUnix = ""
		edm := newTestDnstapMinimiser(t, tc)
		err := edm.Run(t.Context())
		if !errors.Is(err, errNoInputConfigured) {
			t.Fatalf("err = %v, want errNoInputConfigured", err)
		}
	})

	t.Run("seen-qname LRU error", func(t *testing.T) {
		runCoreCleanup(t)
		tc := runCoreTC(t)
		tc.QnameSeenEntries = 0
		edm := newTestDnstapMinimiser(t, tc)
		err := edm.Run(t.Context())
		if err == nil || !strings.Contains(err.Error(), "seen-qname LRU") {
			t.Fatalf("err = %v, want seen-qname LRU failure", err)
		}
	})

	t.Run("DawgLoader.LoadDawgFile error", func(t *testing.T) {
		runCoreCleanup(t)
		tc := runCoreTC(t)
		tc.WellKnownDomainsFile = filepath.Join(t.TempDir(), "missing.dawg")
		edm := newTestDnstapMinimiser(t, tc)
		pinHTTPServersToEphemeral(t, edm)
		err := edm.Run(t.Context())
		if err == nil || !strings.Contains(err.Error(), "DawgLoader.LoadDawgFile") {
			t.Fatalf("err = %v, want DawgLoader.LoadDawgFile failure", err)
		}
	})

	t.Run("cryptopan cache creation error", func(t *testing.T) {
		runCoreCleanup(t)
		tc := runCoreTC(t)
		edm := newTestDnstapMinimiser(t, tc)
		pinHTTPServersToEphemeral(t, edm)
		// A negative entry count cannot pass NewDnstapMinimiser, so mutate
		// the stored config to emulate a bad runtime reload landing before
		// the workers start. Run must fail instead of silently running
		// without minimiser workers.
		edm.conf.CryptopanAddressEntries = -1
		err := edm.Run(t.Context())
		if err == nil || !strings.Contains(err.Error(), "cryptopan cache") {
			t.Fatalf("err = %v, want cryptopan cache failure", err)
		}
	})

	t.Run("debug dnstap file open error", func(t *testing.T) {
		runCoreCleanup(t)
		tc := runCoreTC(t)
		// A path under a regular file (not a directory) makes OpenFile
		// fail with ENOTDIR regardless of the test user's uid.
		blocker := writeTempFile(t, "blocker", []byte("x"))
		tc.DebugDnstapFilename = filepath.Join(blocker, "debug.dnstap")
		edm := newTestDnstapMinimiser(t, tc)
		pinHTTPServersToEphemeral(t, edm)
		err := edm.Run(t.Context())
		if err == nil || !strings.Contains(err.Error(), "debug dnstap file") {
			t.Fatalf("err = %v, want debug dnstap file failure", err)
		}
	})
}
