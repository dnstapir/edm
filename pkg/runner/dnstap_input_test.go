package runner

import (
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"testing"
)

type testDnstapInputFactory struct {
	DnstapInputFactory
	newFromPath func(string) (DnstapInput, error)
}

func (tdif testDnstapInputFactory) NewFrameStreamSockInputFromPath(path string) (DnstapInput, error) {
	if tdif.newFromPath != nil {
		return tdif.newFromPath(path)
	}
	return tdif.DnstapInputFactory.NewFrameStreamSockInputFromPath(path)
}

type testListenerFactory struct {
	ListenerFactory
	listen    func(string, string) (net.Listener, error)
	listenTLS func(string, string, *tls.Config) (net.Listener, error)
}

func (tlf testListenerFactory) Listen(network, address string) (net.Listener, error) {
	if tlf.listen != nil {
		return tlf.listen(network, address)
	}
	return tlf.ListenerFactory.Listen(network, address)
}

func (tlf testListenerFactory) ListenTLS(network, address string, cfg *tls.Config) (net.Listener, error) {
	if tlf.listenTLS != nil {
		return tlf.listenTLS(network, address, cfg)
	}
	return tlf.ListenerFactory.ListenTLS(network, address, cfg)
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

	t.Run("unix happy", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		dti, err := edm.setupDnstapInput(discardLog, Config{
			InputUnix: filepath.Join(t.TempDir(), "dnstap.sock"),
		})
		if err != nil {
			t.Fatalf("setupDnstapInput: %v", err)
		}
		if dti == nil {
			t.Fatal("dti is nil on success")
		}
	})

	t.Run("unix error", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		edm.deps.DnstapInputFactory = testDnstapInputFactory{
			DnstapInputFactory: edm.deps.DnstapInputFactory,
			newFromPath: func(string) (DnstapInput, error) {
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
	})

	t.Run("tcp listen error", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		edm.deps.ListenerFactory = testListenerFactory{
			ListenerFactory: edm.deps.ListenerFactory,
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
			ListenerFactory: edm.deps.ListenerFactory,
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
