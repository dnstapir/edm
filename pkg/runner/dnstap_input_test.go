package runner

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	dnstap "github.com/dnstap/golang-dnstap"
)

type testDnstapInputFactory struct {
	dnstapInputFactory
	newFromListener func(net.Listener) dnstapInput
}

func (tdif testDnstapInputFactory) NewFrameStreamSockInput(listener net.Listener) dnstapInput {
	if tdif.newFromListener != nil {
		return tdif.newFromListener(listener)
	}
	return tdif.dnstapInputFactory.NewFrameStreamSockInput(listener)
}

type testListenerFactory struct {
	listenerFactory
	listen    func(string, string) (net.Listener, error)
	listenTLS func(string, string, *tls.Config) (net.Listener, error)
}

func (tlf testListenerFactory) Listen(network, address string) (net.Listener, error) {
	if tlf.listen != nil {
		return tlf.listen(network, address)
	}
	return tlf.listenerFactory.Listen(network, address)
}

func (tlf testListenerFactory) ListenTLS(network, address string, cfg *tls.Config) (net.Listener, error) {
	if tlf.listenTLS != nil {
		return tlf.listenTLS(network, address, cfg)
	}
	return tlf.listenerFactory.ListenTLS(network, address, cfg)
}

type testNetAddr struct {
	network string
	address string
}

func (addr testNetAddr) Network() string {
	return addr.network
}

func (addr testNetAddr) String() string {
	return addr.address
}

type testNetListener struct {
	addr      net.Addr
	closed    chan struct{}
	closeOnce sync.Once
	accept    func() (net.Conn, error)
	close     func() error
}

func newTestNetListener(network, address string) *testNetListener {
	return &testNetListener{
		addr:   testNetAddr{network: network, address: address},
		closed: make(chan struct{}),
	}
}

func (listener *testNetListener) Accept() (net.Conn, error) {
	if listener.accept != nil {
		return listener.accept()
	}
	<-listener.closed
	return nil, net.ErrClosed
}

func (listener *testNetListener) Close() error {
	var err error
	listener.closeOnce.Do(func() {
		close(listener.closed)
		if listener.close != nil {
			err = listener.close()
		}
	})
	return err
}

func (listener *testNetListener) Addr() net.Addr {
	return listener.addr
}

func (listener *testNetListener) isClosed() bool {
	select {
	case <-listener.closed:
		return true
	default:
		return false
	}
}

type testDnstapInput struct {
	ready chan struct{}
	done  chan struct{}
	err   error
	// cancelSeen, if non-nil, is closed once ReadInto has observed ctx
	// cancellation, letting tests synchronize with Run's shutdown path.
	cancelSeen chan struct{}
	// release, if non-nil, blocks ReadInto's return after ctx cancellation
	// until the test closes it, holding Run at dnstapInputWg.Wait().
	release   chan struct{}
	readyOnce sync.Once
	doneOnce  sync.Once
}

func newBlockingTestDnstapInput() *testDnstapInput {
	return &testDnstapInput{
		ready: make(chan struct{}),
		done:  make(chan struct{}),
	}
}

func (input *testDnstapInput) ReadInto(ctx context.Context, _ chan<- []byte) error {
	input.signalReady()
	defer input.signalDone()
	if input.err != nil {
		return input.err
	}
	<-ctx.Done()
	if input.cancelSeen != nil {
		close(input.cancelSeen)
	}
	if input.release != nil {
		<-input.release
	}
	return nil
}

func (input *testDnstapInput) SetTimeout(time.Duration) {}

func (input *testDnstapInput) SetLogger(dnstap.Logger) {}

func (input *testDnstapInput) Close() error {
	input.signalDone()
	return nil
}

func (input *testDnstapInput) signalReady() {
	if input.ready == nil {
		return
	}
	input.readyOnce.Do(func() {
		close(input.ready)
	})
}

func (input *testDnstapInput) signalDone() {
	if input.done == nil {
		return
	}
	input.doneOnce.Do(func() {
		close(input.done)
	})
}

func TestSetupDnstapInput(t *testing.T) {
	discardLog := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("no input configured", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		_, err := edm.setupDnstapInput(discardLog, Config{})
		if !errors.Is(err, errNoInputConfigured) {
			t.Fatalf("err = %v, want errNoInputConfigured", err)
		}
	})

	t.Run("multiple inputs configured", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		_, err := edm.setupDnstapInput(discardLog, Config{
			InputUnix: filepath.Join(t.TempDir(), "dnstap.sock"),
			InputTCP:  "127.0.0.1:0",
		})
		if !errors.Is(err, errMultipleInputsConfigured) {
			t.Fatalf("err = %v, want errMultipleInputsConfigured", err)
		}
	})

	t.Run("unix happy", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		socketPath := filepath.Join(t.TempDir(), "dnstap.sock")
		listener := newTestNetListener("unix", socketPath)
		var removed string
		var listenedNetwork string
		var listenedAddress string
		var factoryListener net.Listener
		edm.deps.FileSystem = faultingFileSystem{
			fileSystem: edm.deps.FileSystem,
			remove: func(name string) error {
				removed = name
				return fs.ErrNotExist
			},
		}
		edm.deps.ListenerFactory = testListenerFactory{
			listenerFactory: edm.deps.ListenerFactory,
			listen: func(network, address string) (net.Listener, error) {
				listenedNetwork = network
				listenedAddress = address
				return listener, nil
			},
		}
		edm.deps.DnstapInputFactory = testDnstapInputFactory{
			dnstapInputFactory: edm.deps.DnstapInputFactory,
			newFromListener: func(l net.Listener) dnstapInput {
				factoryListener = l
				return &testDnstapInput{}
			},
		}
		dti, err := edm.setupDnstapInput(discardLog, Config{
			InputUnix: socketPath,
		})
		if err != nil {
			t.Fatalf("setupDnstapInput: %v", err)
		}
		if dti == nil {
			t.Fatal("dti is nil on success")
		}
		t.Cleanup(func() {
			if err := dti.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		})
		if removed != socketPath {
			t.Fatalf("removed socket = %q, want %q", removed, socketPath)
		}
		if listenedNetwork != "unix" || listenedAddress != socketPath {
			t.Fatalf("Listen(%q, %q), want unix/%q", listenedNetwork, listenedAddress, socketPath)
		}
		if factoryListener != listener {
			t.Fatal("dnstap input factory did not receive listener returned by ListenerFactory")
		}
	})

	t.Run("unix remove error", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		edm.deps.FileSystem = faultingFileSystem{
			fileSystem: edm.deps.FileSystem,
			remove: func(string) error {
				return errInjected
			},
		}
		_, err := edm.setupDnstapInput(discardLog, Config{InputUnix: "/tmp/x"})
		if !errors.Is(err, errInjected) {
			t.Fatalf("err = %v, want errInjected", err)
		}
	})

	t.Run("unix listen error", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		edm.deps.FileSystem = faultingFileSystem{
			fileSystem: edm.deps.FileSystem,
			remove: func(string) error {
				return nil
			},
		}
		edm.deps.ListenerFactory = testListenerFactory{
			listenerFactory: edm.deps.ListenerFactory,
			listen: func(string, string) (net.Listener, error) {
				return nil, errInjected
			},
		}
		_, err := edm.setupDnstapInput(discardLog, Config{InputUnix: "/tmp/x"})
		if !errors.Is(err, errInjected) {
			t.Fatalf("err = %v, want errInjected", err)
		}
	})

	t.Run("tcp happy", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		dti, err := edm.setupDnstapInput(discardLog, Config{InputTCP: "127.0.0.1:0"})
		if err != nil {
			t.Fatalf("setupDnstapInput: %v", err)
		}
		if dti == nil {
			t.Fatal("dti is nil on success")
		}
		t.Cleanup(func() {
			if err := dti.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		})
	})

	t.Run("tcp listen error", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		edm.deps.ListenerFactory = testListenerFactory{
			listenerFactory: edm.deps.ListenerFactory,
			listen: func(string, string) (net.Listener, error) {
				return nil, errInjected
			},
		}
		_, err := edm.setupDnstapInput(discardLog, Config{InputTCP: "127.0.0.1:0"})
		if !errors.Is(err, errInjected) {
			t.Fatalf("err = %v, want errInjected", err)
		}
	})

	t.Run("tls happy", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		certPath, keyPath, _ := testCertFiles(t)
		dti, err := edm.setupDnstapInput(discardLog, Config{
			InputTLS:         "127.0.0.1:0",
			InputTLSCertFile: certPath,
			InputTLSKeyFile:  keyPath,
		})
		if err != nil {
			t.Fatalf("setupDnstapInput: %v", err)
		}
		if dti == nil {
			t.Fatal("dti is nil on success")
		}
		t.Cleanup(func() {
			if err := dti.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		})
	})

	t.Run("tls happy with client CA", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		certPath, keyPath, caPath := testCertFiles(t)
		dti, err := edm.setupDnstapInput(discardLog, Config{
			InputTLS:             "127.0.0.1:0",
			InputTLSCertFile:     certPath,
			InputTLSKeyFile:      keyPath,
			InputTLSClientCAFile: caPath,
		})
		if err != nil {
			t.Fatalf("setupDnstapInput: %v", err)
		}
		if dti == nil {
			t.Fatal("dti is nil on success")
		}
		t.Cleanup(func() {
			if err := dti.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		})
	})

	t.Run("tls bad cert", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		_, err := edm.setupDnstapInput(discardLog, Config{
			InputTLS:         "127.0.0.1:0",
			InputTLSCertFile: filepath.Join(t.TempDir(), "missing.crt"),
			InputTLSKeyFile:  filepath.Join(t.TempDir(), "missing.key"),
		})
		if err == nil || !strings.Contains(err.Error(), "x509 dnstap listener cert") {
			t.Fatalf("err = %v, want x509 cert load failure", err)
		}
	})

	t.Run("tls bad client CA file", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		certPath, keyPath, _ := testCertFiles(t)
		badCA := writeTempFile(t, "bad-ca.pem", []byte("not a pem"))
		_, err := edm.setupDnstapInput(discardLog, Config{
			InputTLS:             "127.0.0.1:0",
			InputTLSCertFile:     certPath,
			InputTLSKeyFile:      keyPath,
			InputTLSClientCAFile: badCA,
		})
		if err == nil || !strings.Contains(err.Error(), "CA cert pool") {
			t.Fatalf("err = %v, want CA cert pool failure", err)
		}
	})

	t.Run("tls listen error", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		certPath, keyPath, _ := testCertFiles(t)
		edm.deps.ListenerFactory = testListenerFactory{
			listenerFactory: edm.deps.ListenerFactory,
			listenTLS: func(string, string, *tls.Config) (net.Listener, error) {
				return nil, errInjected
			},
		}
		_, err := edm.setupDnstapInput(discardLog, Config{
			InputTLS:         "127.0.0.1:0",
			InputTLSCertFile: certPath,
			InputTLSKeyFile:  keyPath,
		})
		if !errors.Is(err, errInjected) {
			t.Fatalf("err = %v, want errInjected", err)
		}
	})
}

func TestSocketDnstapInputReadIntoClosesListenerOnCancel(t *testing.T) {
	listener := newTestNetListener("tcp", "127.0.0.1:0")
	input := newSocketDnstapInput(listener)

	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() {
		errCh <- input.ReadInto(ctx, make(chan []byte))
	}()

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ReadInto err = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ReadInto did not exit after context cancellation")
	}
	if !listener.isClosed() {
		t.Fatal("listener was not closed after context cancellation")
	}
}

func TestSocketDnstapInputReadIntoReturnsAcceptError(t *testing.T) {
	listener := newTestNetListener("tcp", "127.0.0.1:0")
	listener.accept = func() (net.Conn, error) {
		return nil, errInjected
	}
	input := newSocketDnstapInput(listener)

	err := input.ReadInto(t.Context(), make(chan []byte))
	if !errors.Is(err, errInjected) {
		t.Fatalf("ReadInto err = %v, want errInjected", err)
	}
}

func TestSocketDnstapInputReadIntoRetriesTransientAcceptError(t *testing.T) {
	listener := newTestNetListener("tcp", "127.0.0.1:0")
	var acceptCalls atomic.Int64
	retried := make(chan struct{})
	listener.accept = func() (net.Conn, error) {
		switch acceptCalls.Add(1) {
		case 1:
			return nil, &net.OpError{Op: "accept", Net: "tcp", Err: syscall.EMFILE}
		case 2:
			close(retried)
		}
		<-listener.closed
		return nil, net.ErrClosed
	}
	input := newSocketDnstapInput(listener)

	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() {
		errCh <- input.ReadInto(ctx, make(chan []byte))
	}()

	select {
	case <-retried:
	case <-time.After(2 * time.Second):
		t.Fatal("ReadInto did not keep accepting after a transient accept error")
	}
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ReadInto err = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ReadInto did not exit after context cancellation")
	}
}

type timeoutNetError struct{}

func (timeoutNetError) Error() string   { return "i/o timeout" }
func (timeoutNetError) Timeout() bool   { return true }
func (timeoutNetError) Temporary() bool { return false }

func TestIsTransientAcceptError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"net.Error timeout", &net.OpError{Op: "accept", Net: "tcp", Err: timeoutNetError{}}, true},
		{"ECONNABORTED", &net.OpError{Op: "accept", Net: "tcp", Err: syscall.ECONNABORTED}, true},
		{"EMFILE", &net.OpError{Op: "accept", Net: "tcp", Err: syscall.EMFILE}, true},
		{"ENFILE", &net.OpError{Op: "accept", Net: "tcp", Err: syscall.ENFILE}, true},
		{"closed listener", net.ErrClosed, false},
		{"unrecognized", errInjected, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isTransientAcceptError(tt.err); got != tt.want {
				t.Fatalf("isTransientAcceptError(%v) = %t, want %t", tt.err, got, tt.want)
			}
		})
	}
}
