package runner

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"
	"time"
)

func TestConfigUpdaterExitsOnContextCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		edm := newSynctestDnstapMinimiser(t, defaultTC)

		reloadCh := make(chan os.Signal, 1)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			configUpdater(ctx, reloadCh, edm)
		}()

		// Cancelling the context is sticky, so configUpdater observes it via its
		// select regardless of whether the goroutine has reached the select yet.
		cancel()

		wg.Wait()
	})
}

func TestConfigUpdaterExitsOnChannelClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		edm := newSynctestDnstapMinimiser(t, defaultTC)

		reloadCh := make(chan os.Signal, 1)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			configUpdater(t.Context(), reloadCh, edm)
		}()

		// Closing reloadCh makes the receive return ok=false, which
		// configUpdater treats as a shutdown signal and returns.
		close(reloadCh)

		wg.Wait()
	})
}

type sequenceConfiger struct {
	configs []Config
	index   int
	err     error
}

func (sc *sequenceConfiger) GetConfig() (Config, error) {
	if sc.err != nil {
		return Config{}, sc.err
	}
	if sc.index >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1], nil
	}
	conf := sc.configs[sc.index]
	sc.index++
	return conf, nil
}

func TestConfigUpdater(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		edm := newSynctestDnstapMinimiser(t, defaultTC)
		startConf := edm.getConfig()
		nextConf := startConf
		nextConf.CryptopanKey = "key2"
		nextConf.DisableHistogramSender = true
		nextConf.IgnoredClientIPsFile = ""
		nextConf.IgnoredQuestionNamesFile = ""
		sc := &sequenceConfiger{configs: []Config{nextConf}}
		edm.configer = sc
		edm.reloadMinimiserConfigCh = []chan struct{}{make(chan struct{}, 1)}
		edm.reloadHistogramSenderConfigCh = make(chan struct{}, 1)

		reloadCh := make(chan os.Signal)
		done := make(chan struct{})
		go func() {
			defer close(done)
			configUpdater(t.Context(), reloadCh, edm)
		}()
		reloadCh <- syscall.SIGHUP
		synctest.Wait()

		if edm.getConfig().CryptopanKey != "key2" {
			t.Fatalf("config was not updated: %#v", edm.getConfig())
		}
		select {
		case <-edm.reloadMinimiserConfigCh[0]:
		default:
			t.Fatal("minimiser reload notification not queued")
		}
		select {
		case <-edm.reloadHistogramSenderConfigCh:
		default:
			t.Fatal("histogram reload notification not queued")
		}
		close(reloadCh)
		<-done
	})
}

// TestConfigUpdaterSIGHUP verifies the OS delivery path: a real SIGHUP sent
// to the process reaches configUpdater through a signal.Notify channel wired
// the same way as in Run and triggers a reload.
func TestConfigUpdaterSIGHUP(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	startConf := edm.getConfig()
	nextConf := startConf
	nextConf.CryptopanKey = "key-sighup"
	nextConf.IgnoredClientIPsFile = ""
	nextConf.IgnoredQuestionNamesFile = ""
	nextConf.DisableHistogramSender = true
	edm.configer = &sequenceConfiger{configs: []Config{nextConf}}
	edm.reloadMinimiserConfigCh = []chan struct{}{make(chan struct{}, 1)}
	edm.reloadHistogramSenderConfigCh = make(chan struct{}, 1)

	hupCh := make(chan os.Signal, 1)
	signal.Notify(hupCh, syscall.SIGHUP)
	defer signal.Stop(hupCh)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		configUpdater(ctx, hupCh, edm)
	}()
	defer func() {
		cancel()
		<-done
	}()

	if err := syscall.Kill(os.Getpid(), syscall.SIGHUP); err != nil {
		t.Fatalf("unable to send SIGHUP: %s", err)
	}

	// The histogram sender notification is queued by applyUpdate once the
	// reload has been applied, so waiting on it avoids polling getConfig.
	select {
	case <-edm.reloadHistogramSenderConfigCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for SIGHUP-triggered reload")
	}

	if edm.getConfig().CryptopanKey != "key-sighup" {
		t.Fatalf("config was not updated: %#v", edm.getConfig())
	}
}

// runConfigUpdaterUntil drives a single reload request through configUpdater,
// waits for it to be applied, then shuts the goroutine down. log is wired to
// a syncBuf so subtests can assert on the reload paths that have no other
// observable side-effect without racing the worker on the log write.
func runConfigUpdaterUntil(t *testing.T, edm *DnstapMinimiser, sc *sequenceConfiger, expect func() bool) {
	t.Helper()

	edm.configer = sc
	edm.reloadMinimiserConfigCh = []chan struct{}{make(chan struct{}, 1)}
	edm.reloadHistogramSenderConfigCh = make(chan struct{}, 1)

	reloadCh := make(chan os.Signal)
	ctx := t.Context()
	done := make(chan struct{})
	go func() {
		defer close(done)
		configUpdater(ctx, reloadCh, edm)
	}()
	reloadCh <- syscall.SIGHUP
	synctest.Wait()
	close(reloadCh)
	<-done
	if !expect() {
		t.Fatal("configUpdater did not reach the expected state")
	}
}

// TestConfigUpdaterBranches covers reload arms that TestConfigUpdater
// (cryptopan key + disable-histogram-sender + ignored-files clear) does
// not reach.
func TestConfigUpdaterBranches(t *testing.T) {
	t.Run("non-reload-tagged field warns", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			buf := &syncBuf{}
			edm := newSynctestDnstapMinimiser(t, defaultTC)
			edm.log = slog.New(slog.NewJSONHandler(buf, nil))

			startConf := edm.getConfig()
			next := startConf
			// DataDir has no reload:"true" tag, so changing it triggers the
			// "requires restart" warning. The warning names the changed key
			// from its toml tag, so the message must carry "data-dir" (an
			// empty config_key would mean the wrong struct tag was read).
			next.DataDir = "/tmp/edm-changed"
			runConfigUpdaterUntil(t, edm, &sequenceConfiger{configs: []Config{next}}, func() bool {
				return strings.Contains(buf.String(), "requires restart") &&
					strings.Contains(buf.String(), `"config_key":"data-dir"`)
			})
		})
	})

	t.Run("reloadable fields do not warn", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			buf := &syncBuf{}
			edm := newSynctestDnstapMinimiser(t, defaultTC)
			edm.log = slog.New(slog.NewJSONHandler(buf, nil))

			// well-known-domains-file (the DAWG, re-read at the next
			// rotation) and disable-session-files (the minimiser worker
			// re-reads it live) both carry reload:"true", so changing them
			// must not log the "requires restart" warning.
			startConf := edm.getConfig()
			next := startConf
			next.WellKnownDomainsFile = "other-well-known.dawg"
			next.DisableSessionFiles = !startConf.DisableSessionFiles
			runConfigUpdaterUntil(t, edm, &sequenceConfiger{configs: []Config{next}}, func() bool {
				got := edm.getConfig()
				return got.WellKnownDomainsFile == next.WellKnownDomainsFile &&
					got.DisableSessionFiles == next.DisableSessionFiles
			})
			if strings.Contains(buf.String(), "requires restart") {
				t.Fatalf("toggling reloadable fields logged a restart warning: %s", buf.String())
			}
		})
	})

	t.Run("config provider error keeps old config", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			buf := &syncBuf{}
			edm := newSynctestDnstapMinimiser(t, defaultTC)
			edm.log = slog.New(slog.NewJSONHandler(buf, nil))

			startConf := edm.getConfig()
			runConfigUpdaterUntil(t, edm, &sequenceConfiger{err: errors.New("boom")}, func() bool {
				return strings.Contains(buf.String(), "unable to update edm config")
			})
			if edm.getConfig() != startConf {
				t.Fatal("config changed even though the provider returned an error")
			}
		})
	})

	t.Run("broken ignore list file keeps old state and logs", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			buf := &syncBuf{}
			edm := newSynctestDnstapMinimiser(t, defaultTC)
			edm.log = slog.New(slog.NewJSONHandler(buf, nil))

			// A reload where the configured ignore list file has gone
			// missing (or is unreadable) must keep the previous ignore
			// state and log the loader error.
			startConf := edm.getConfig()
			next := startConf
			next.IgnoredClientIPsFile = filepath.Join(t.TempDir(), "missing-ips.txt")
			next.IgnoredQuestionNamesFile = ""
			before := edm.ignoredClientsIPSet.Load()
			runConfigUpdaterUntil(t, edm, &sequenceConfiger{configs: []Config{next}}, func() bool {
				return strings.Contains(buf.String(), "setIgnoredClientIPs")
			})
			if edm.ignoredClientsIPSet.Load() != before {
				t.Fatal("ignored client IP set changed even though the loader failed")
			}
		})
	})

	t.Run("HTTP cert reloads on reload request", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			buf := &syncBuf{}
			edm := newSynctestDnstapMinimiser(t, defaultTC)
			edm.log = slog.New(slog.NewJSONHandler(buf, nil))

			// Start with the histogram sender enabled and a valid cert so
			// the late-init branch does not also fire and obscure the
			// cert-reload assertion.
			startConf := edm.getConfig()
			startConf.DisableHistogramSender = false
			startConf.HTTPClientCertFile = filepath.Join(t.TempDir(), "missing.crt")
			edm.conf = startConf

			next := startConf
			runConfigUpdaterUntil(t, edm, &sequenceConfiger{configs: []Config{next}}, func() bool {
				return strings.Contains(buf.String(), "loadHTTPClientCert")
			})
		})
	})

	t.Run("MQTT cert reloads on reload request", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			buf := &syncBuf{}
			edm := newSynctestDnstapMinimiser(t, defaultTC)
			edm.log = slog.New(slog.NewJSONHandler(buf, nil))

			startConf := edm.getConfig()
			startConf.DisableMQTT = false
			startConf.MQTTClientCertFile = filepath.Join(t.TempDir(), "missing.crt")
			edm.conf = startConf

			next := startConf
			runConfigUpdaterUntil(t, edm, &sequenceConfiger{configs: []Config{next}}, func() bool {
				return strings.Contains(buf.String(), "loadMQTTClientCert")
			})
		})
	})
}

// TestWritableDataDirSurvivesReload pins that the writable data-dir installed
// by the test constructors lives in the config provider, not only in the
// cached Config. A SIGHUP reload re-reads Config from the provider through
// updateConfig, so a data-dir held only in the cache would revert to the
// read-only placeholder and break DAWG staging on the next rotation.
func TestWritableDataDirSurvivesReload(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)

	dir := edm.getConfig().DataDir
	if dir == placeholderDataDir {
		t.Fatalf("constructor left the placeholder data-dir %q in place", dir)
	}

	// updateConfig re-reads Config from the provider exactly as an SIGHUP
	// reload does; the writable path must survive the round-trip.
	if err := edm.updateConfig(); err != nil {
		t.Fatalf("updateConfig: %s", err)
	}
	if got := edm.getConfig().DataDir; got != dir {
		t.Fatalf("data-dir after reload = %q, want %q", got, dir)
	}
}
