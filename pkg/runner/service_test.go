package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/viper"
)

func TestNewDnstapMinimiserAPI(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	if _, err := NewDnstapMinimiser(nil, logger); !errors.Is(err, ErrNilConfigProvider) {
		t.Fatalf("nil provider err = %v, want %v", err, ErrNilConfigProvider)
	}
	if _, err := NewDnstapMinimiser(defaultTC, nil); !errors.Is(err, ErrNilLogger) {
		t.Fatalf("nil logger err = %v, want %v", err, ErrNilLogger)
	}

	loggerLevel := new(slog.LevelVar)
	edm, err := NewDnstapMinimiser(
		defaultTC, logger,
		WithLoggerLevel(loggerLevel),
		WithDependencies(Dependencies{PprofListenAddr: "127.0.0.1:0", CryptopanFactory: fastTestCryptopanFactory{}}),
	)
	if err != nil {
		t.Fatalf("NewDnstapMinimiser: %s", err)
	}
	t.Cleanup(func() { cleanupTestMinimiser(edm) })

	if edm.loggerLevel != loggerLevel {
		t.Fatal("WithLoggerLevel did not install the supplied level var")
	}
	if edm.deps.PprofListenAddr != "127.0.0.1:0" {
		t.Fatalf("PprofListenAddr = %q, want custom value", edm.deps.PprofListenAddr)
	}
	if edm.deps.FileSystem == nil || edm.deps.Clock == nil || edm.deps.HTTPServerRunner == nil || edm.deps.CryptopanFactory == nil {
		t.Fatal("WithDependencies did not fill nil dependency fields")
	}
}

func TestDnstapMinimiserRunGuards(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)

	//lint:ignore SA1012 this guard verifies Run rejects a nil context
	if err := edm.Run(nil); !errors.Is(err, ErrNilRunContext) { //nolint:staticcheck
		t.Fatalf("Run(nil) err = %v, want %v", err, ErrNilRunContext)
	}

	input := newBlockingTestDnstapInput()
	edm = newRunLifecycleTestMinimiser(t, input)
	ctx, cancel := context.WithCancel(t.Context())
	runErr := make(chan error, 1)
	go func() {
		runErr <- edm.Run(ctx)
	}()

	<-input.ready
	if err := edm.Run(t.Context()); !errors.Is(err, ErrDnstapMinimiserRunning) {
		t.Fatalf("concurrent Run err = %v, want %v", err, ErrDnstapMinimiserRunning)
	}
	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("first Run err = %v, want nil", err)
	}
	if err := edm.Run(t.Context()); !errors.Is(err, ErrDnstapMinimiserAlreadyRun) {
		t.Fatalf("second Run err = %v, want %v", err, ErrDnstapMinimiserAlreadyRun)
	}
	select {
	case <-input.done:
	default:
		t.Fatal("Run returned before DNSTAP input exited")
	}
}

func TestRunWithDisabledSenders(t *testing.T) {
	t.Cleanup(viper.Reset)

	ctx, cancel := context.WithCancel(t.Context())

	dir := t.TempDir()
	configFile := filepath.Join(dir, "edm.toml")
	dawgFile := testDawgFile(t, "example.com.")
	socketPath := filepath.Join(dir, "dnstap.sock")
	configData := fmt.Sprintf(`
config-file = %q
disable-histogram-sender = true
disable-mqtt = true
input-unix = %q
cryptopan-key = "key1"
cryptopan-key-salt = "aabbccddeeffgghh"
well-known-domains-file = %q
histogram-hll-explicit-threshold = 20
data-dir = %q
minimiser-workers = 1
qname-seen-entries = 2
cryptopan-address-entries = 2
newqname-buffer = 1
`, configFile, socketPath, dawgFile, dir)
	if err := os.WriteFile(configFile, []byte(configData), 0o600); err != nil {
		t.Fatal(err)
	}
	viper.SetConfigFile(configFile)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	level := new(slog.LevelVar)
	deps := defaultDependencies()
	deps.HTTPServerRunner = httpServerRunnerFunc(func(*http.Server) error {
		return http.ErrServerClosed
	})
	deps.CryptopanFactory = fastTestCryptopanFactory{}
	input := newBlockingTestDnstapInput()
	listener := newTestNetListener("unix", socketPath)
	listenCall := make(chan [2]string, 1)
	deps.ListenerFactory = testListenerFactory{
		ListenerFactory: deps.ListenerFactory,
		listen: func(network, address string) (net.Listener, error) {
			select {
			case listenCall <- [2]string{network, address}:
			default:
			}
			return listener, nil
		},
	}
	deps.DnstapInputFactory = testDnstapInputFactory{
		DnstapInputFactory: deps.DnstapInputFactory,
		newFromListener: func(net.Listener) DnstapInput {
			return input
		},
	}
	edm, err := NewDnstapMinimiser(ViperConfigProvider{}, logger, WithLoggerLevel(level), WithDependencies(deps))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := edm.Run(ctx); err != nil {
			t.Errorf("Run: %s", err)
		}
	}()

	<-input.ready
	call := <-listenCall
	if call[0] != "unix" || call[1] != socketPath {
		t.Fatalf("Listen(%q, %q), want unix/%q", call[0], call[1], socketPath)
	}
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not exit")
	}
	select {
	case <-input.done:
	default:
		t.Fatal("Run returned before DNSTAP input exited")
	}
}

func TestRunReturnsDnstapInputRuntimeError(t *testing.T) {
	input := newBlockingTestDnstapInput()
	input.err = errInjected
	edm := newRunLifecycleTestMinimiser(t, input)

	err := edm.Run(t.Context())
	if !errors.Is(err, errInjected) {
		t.Fatalf("Run err = %v, want errInjected", err)
	}
}

func newRunLifecycleTestMinimiser(t *testing.T, input *testDnstapInput) *DnstapMinimiser {
	t.Helper()
	runCoreCleanup(t)
	tc := runCoreTC(t)
	deps := newTestDependencies()
	deps.HTTPServerRunner = httpServerRunnerFunc(func(*http.Server) error {
		return http.ErrServerClosed
	})
	listener := newTestNetListener("unix", tc.InputUnix)
	deps.ListenerFactory = testListenerFactory{
		ListenerFactory: deps.ListenerFactory,
		listen: func(_, _ string) (net.Listener, error) {
			return listener, nil
		},
	}
	deps.DnstapInputFactory = testDnstapInputFactory{
		DnstapInputFactory: deps.DnstapInputFactory,
		newFromListener: func(net.Listener) DnstapInput {
			return input
		},
	}
	return newTestDnstapMinimiserWithDependencies(t, tc, deps)
}
