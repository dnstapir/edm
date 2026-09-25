package runner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	dnstap "github.com/dnstap/golang-dnstap"
	"github.com/miekg/dns"
	"github.com/parquet-go/parquet-go"
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
		withDependencies(dependencies{CryptopanFactory: fastTestCryptopanFactory{}}),
	)
	if err != nil {
		t.Fatalf("NewDnstapMinimiser: %s", err)
	}

	if edm.loggerLevel != loggerLevel {
		t.Fatal("WithLoggerLevel did not install the supplied level var")
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
	deps.FileSystem = faultingFileSystem{
		fileSystem: deps.FileSystem,
		chmod: func(string, os.FileMode) error {
			return nil
		},
	}
	deps.ListenerFactory = testListenerFactory{
		listenerFactory: deps.ListenerFactory,
		listen: func(network, address string) (net.Listener, error) {
			select {
			case listenCall <- [2]string{network, address}:
			default:
			}
			return listener, nil
		},
	}
	deps.DnstapInputFactory = testDnstapInputFactory{
		dnstapInputFactory: deps.DnstapInputFactory,
		newFromListener: func(net.Listener) dnstapInput {
			return input
		},
	}
	edm, err := NewDnstapMinimiser(NewFileConfigProvider(configFile), logger, WithLoggerLevel(level), withDependencies(deps))
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

// blockingSeenQnameStore blocks its first lookup until release is closed.
type blockingSeenQnameStore struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (store *blockingSeenQnameStore) Has(string) (bool, error) {
	store.once.Do(func() {
		close(store.entered)
		<-store.release
	})
	return false, nil
}

func (*blockingSeenQnameStore) MarkSeen(string, bool) error { return nil }

func (*blockingSeenQnameStore) Close() error { return nil }

// drainTimeoutClock exposes and controls the shutdown drain timer while
// delegating all other clock operations.
type drainTimeoutClock struct {
	clock
	afterCalled chan time.Duration
	fire        chan time.Time
}

func (c *drainTimeoutClock) After(d time.Duration) <-chan time.Time {
	if d != shutdownDrainTimeout {
		return c.clock.After(d)
	}
	c.afterCalled <- d
	return c.fire
}

// TestRunDrainsAcceptedFramesOnShutdown verifies graceful cancellation
// persists every frame already accepted into the input channel.
func TestRunDrainsAcceptedFramesOnShutdown(t *testing.T) {
	const frameCount = 128
	frame := testPackedDnstapMessage(t, dnstap.Message_CLIENT_RESPONSE, dnstap.SocketFamily_INET, packedDNSMsg(t, "new.example.", dns.TypeA, dns.RcodeSuccess))
	input := newBlockingTestDnstapInput()
	input.frames = slices.Repeat([][]byte{frame}, frameCount)

	edm := newRunLifecycleTestMinimiser(t, input)
	edm.conf.DisableSessionFiles = false
	store := &blockingSeenQnameStore{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	edm.deps.SeenQnameStoreFactory = seenQnameStoreFactoryFunc(func(string) (seenQnameStore, error) {
		return store, nil
	})

	ctx, cancel := context.WithCancel(t.Context())
	runErr := make(chan error, 1)
	go func() {
		runErr <- edm.Run(ctx)
	}()

	<-store.entered
	<-input.ready
	cancel()
	close(store.release)
	if err := <-runErr; err != nil {
		t.Fatalf("Run: %s", err)
	}

	files, err := filepath.Glob(filepath.Join(edm.conf.DataDir, "parquet", "sessions", "*.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("session files = %d, want 1", len(files))
	}
	data, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	rows, err := parquet.Read[sessionData](bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != frameCount {
		t.Fatalf("persisted sessions = %d, want %d", len(rows), frameCount)
	}
}

// TestRunAbortsDrainAfterTimeout verifies cancellation bounds the graceful
// drain and reports the forced abort.
func TestRunAbortsDrainAfterTimeout(t *testing.T) {
	const frameCount = 128
	frame := testPackedDnstapMessage(t, dnstap.Message_CLIENT_RESPONSE, dnstap.SocketFamily_INET, packedDNSMsg(t, "new.example.", dns.TypeA, dns.RcodeSuccess))
	input := newBlockingTestDnstapInput()
	input.cancelSeen = make(chan struct{})
	input.release = make(chan struct{})
	input.frames = slices.Repeat([][]byte{frame}, frameCount)

	edm := newRunLifecycleTestMinimiser(t, input)
	store := &blockingSeenQnameStore{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	edm.deps.SeenQnameStoreFactory = seenQnameStoreFactoryFunc(func(string) (seenQnameStore, error) {
		return store, nil
	})
	testClock := &drainTimeoutClock{
		clock:       edm.deps.Clock,
		afterCalled: make(chan time.Duration, 1),
		fire:        make(chan time.Time, 1),
	}
	edm.deps.Clock = testClock

	ctx, cancel := context.WithCancel(t.Context())
	runErr := make(chan error, 1)
	go func() {
		runErr <- edm.Run(ctx)
	}()

	<-store.entered
	<-input.ready
	cancel()
	<-input.cancelSeen
	select {
	case got := <-testClock.afterCalled:
		t.Fatalf("shutdown drain timer started before input stopped: %s", got)
	default:
	}
	close(input.release)
	if got := <-testClock.afterCalled; got != shutdownDrainTimeout {
		t.Fatalf("shutdown drain timeout = %s, want %s", got, shutdownDrainTimeout)
	}
	testClock.fire <- time.Now()
	close(store.release)

	if err := <-runErr; !errors.Is(err, ErrShutdownDrainTimeout) {
		t.Fatalf("Run error = %v, want %v", err, ErrShutdownDrainTimeout)
	}
}

func BenchmarkRunDrainAcceptedFrames(b *testing.B) {
	const frameCount = 5_000
	frames := make([][]byte, frameCount)
	for i := range frames {
		qname := fmt.Sprintf("name-%d.example.", i)
		frames[i] = testPackedDnstapMessage(b, dnstap.Message_CLIENT_RESPONSE, dnstap.SocketFamily_INET, packedDNSMsg(b, qname, dns.TypeA, dns.RcodeSuccess))
	}

	b.ReportAllocs()
	b.ResetTimer()
	queuedFrames := 0
	for range b.N {
		b.StopTimer()
		input := newBlockingTestDnstapInput()
		input.frames = frames
		edm := newRunLifecycleTestMinimiser(b, input)
		edm.conf.DisableSessionFiles = false
		ctx, cancel := context.WithCancel(b.Context())
		runErr := make(chan error, 1)
		go func() {
			runErr <- edm.Run(ctx)
		}()
		<-input.ready
		queuedFrames += len(edm.inputChannel)

		b.StartTimer()
		cancel()
		if err := <-runErr; err != nil {
			b.Fatalf("Run: %s", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(queuedFrames)/float64(b.N), "queued-frames/op")
}

func newRunLifecycleTestMinimiser(t testing.TB, input dnstapInput) *DnstapMinimiser {
	t.Helper()
	tc := runCoreTC(t)
	deps := newTestDependencies()
	deps.HTTPServerRunner = httpServerRunnerFunc(func(*http.Server) error {
		return http.ErrServerClosed
	})
	listener := newTestNetListener("unix", tc.InputUnix)
	deps.ListenerFactory = testListenerFactory{
		listenerFactory: deps.ListenerFactory,
		listen: func(_, _ string) (net.Listener, error) {
			return listener, nil
		},
	}
	deps.DnstapInputFactory = testDnstapInputFactory{
		dnstapInputFactory: deps.DnstapInputFactory,
		newFromListener: func(net.Listener) dnstapInput {
			return input
		},
	}
	return newTestDnstapMinimiserWithDependencies(t, tc, deps)
}
