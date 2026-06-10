package runner

import (
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

func TestConfigUpdaterExitsOnContextCancel(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)

	viperNotifyCh := make(chan fsnotify.Event, 1)
	ctx, cancel := testRunContext(t)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		configUpdater(ctx, viperNotifyCh, edm)
	}()

	// Cancelling the context is sticky, so configUpdater observes it via its
	// select regardless of whether the goroutine has reached the select yet.
	cancel()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("configUpdater did not exit after context cancel")
	}
}

func TestConfigUpdaterExitsOnChannelClose(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)

	viperNotifyCh := make(chan fsnotify.Event, 1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		configUpdater(t.Context(), viperNotifyCh, edm)
	}()

	// Closing viperNotifyCh makes the receive return ok=false, which
	// configUpdater treats as a shutdown signal and returns.
	close(viperNotifyCh)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("configUpdater did not exit after viperNotifyCh close")
	}
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
	edm := newTestDnstapMinimiser(t, defaultTC)
	edm.deps.ConfigUpdateDebounce = 10 * time.Millisecond
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

	events := make(chan fsnotify.Event)
	ctx := t.Context()
	done := make(chan struct{})
	go func() {
		defer close(done)
		configUpdater(ctx, events, edm)
	}()
	events <- fsnotify.Event{Name: "config.toml", Op: fsnotify.Write}
	for range 100 {
		if edm.getConfig().CryptopanKey == "key2" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if edm.getConfig().CryptopanKey != "key2" {
		t.Fatalf("config was not updated: %#v", edm.getConfig())
	}
	select {
	case <-edm.reloadMinimiserConfigCh[0]:
	case <-time.After(time.Second):
		t.Fatal("minimiser reload notification not queued")
	}
	select {
	case <-edm.reloadHistogramSenderConfigCh:
	case <-time.After(time.Second):
		t.Fatal("histogram reload notification not queued")
	}
	close(events)
	<-done
}

// runConfigUpdaterUntil drives a single fsnotify event through configUpdater
// and waits for the debounce timer + processing to apply nextConf, then
// shuts the goroutine down. log is wired to a syncBuf so subtests can
// assert on the reload paths that have no other observable side-effect
// without racing the worker on the log write.
func runConfigUpdaterUntil(t *testing.T, edm *DnstapMinimiser, sc *sequenceConfiger, expect func() bool) {
	t.Helper()
	edm.deps.ConfigUpdateDebounce = 5 * time.Millisecond

	edm.configer = sc
	edm.reloadMinimiserConfigCh = []chan struct{}{make(chan struct{}, 1)}
	edm.reloadHistogramSenderConfigCh = make(chan struct{}, 1)

	events := make(chan fsnotify.Event)
	ctx := t.Context()
	done := make(chan struct{})
	go func() {
		defer close(done)
		configUpdater(ctx, events, edm)
	}()
	events <- fsnotify.Event{Name: "config.toml", Op: fsnotify.Write}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if expect() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(events)
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
		buf := &syncBuf{}
		edm := newTestDnstapMinimiser(t, defaultTC)
		edm.log = slog.New(slog.NewJSONHandler(buf, nil))

		startConf := edm.getConfig()
		next := startConf
		// DataDir has no reload:"true" tag, so changing it triggers the
		// "requires restart" warning.
		next.DataDir = "/tmp/edm-changed"
		runConfigUpdaterUntil(t, edm, &sequenceConfiger{configs: []Config{next}}, func() bool {
			return strings.Contains(buf.String(), "requires restart")
		})
	})

	t.Run("HTTP cert path change reloads cert", func(t *testing.T) {
		buf := &syncBuf{}
		edm := newTestDnstapMinimiser(t, defaultTC)
		edm.log = slog.New(slog.NewJSONHandler(buf, nil))

		// Start with the histogram sender enabled and a valid cert so
		// the late-init branch does not also fire and obscure the
		// cert-change assertion.
		certPath, keyPath, _ := testCertFiles(t)
		startConf := edm.getConfig()
		startConf.DisableHistogramSender = false
		startConf.HTTPClientCertFile = certPath
		startConf.HTTPClientKeyFile = keyPath
		edm.conf = startConf

		next := startConf
		next.HTTPClientCertFile = filepath.Join(t.TempDir(), "missing.crt")
		runConfigUpdaterUntil(t, edm, &sequenceConfiger{configs: []Config{next}}, func() bool {
			return strings.Contains(buf.String(), "loadHTTPClientCert")
		})
	})

	t.Run("MQTT cert path change reloads cert", func(t *testing.T) {
		buf := &syncBuf{}
		edm := newTestDnstapMinimiser(t, defaultTC)
		edm.log = slog.New(slog.NewJSONHandler(buf, nil))

		certPath, keyPath, _ := testCertFiles(t)
		startConf := edm.getConfig()
		startConf.DisableMQTT = false
		startConf.MQTTClientCertFile = certPath
		startConf.MQTTClientKeyFile = keyPath
		edm.conf = startConf

		next := startConf
		next.MQTTClientCertFile = filepath.Join(t.TempDir(), "missing.crt")
		runConfigUpdaterUntil(t, edm, &sequenceConfiger{configs: []Config{next}}, func() bool {
			return strings.Contains(buf.String(), "loadMQTTClientCert")
		})
	})
}
