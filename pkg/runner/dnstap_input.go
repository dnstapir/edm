package runner

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"log/slog"
	"net"
	"sync"
	"time"

	dnstap "github.com/dnstap/golang-dnstap"
)

// setupDnstapInput constructs the dnstap socket input selected by startConf.
// Exactly one of InputUnix/InputTCP/InputTLS must be set; with none configured
// it returns errNoInputConfigured rather than letting Run dereference a nil
// *FrameStreamSockInput. On TLS, InputTLSClientCAFile (when set) enables
// required-and-verify client mTLS via tls.RequireAndVerifyClientCert.
func (edm *DnstapMinimiser) setupDnstapInput(logger *slog.Logger, startConf Config) (DnstapInput, error) {
	var dti DnstapInput
	switch {
	case startConf.InputUnix != "":
		logger.Info("creating dnstap unix socket", "socket", startConf.InputUnix)
		if err := edm.deps.FileSystem.Remove(startConf.InputUnix); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("unable to remove stale dnstap unix socket: %w", err)
		}
		l, err := edm.deps.ListenerFactory.Listen("unix", startConf.InputUnix)
		if err != nil {
			return nil, fmt.Errorf("unable to create dnstap unix socket: %w", err)
		}
		dti = edm.deps.DnstapInputFactory.NewFrameStreamSockInput(l)
	case startConf.InputTCP != "":
		logger.Info("creating plaintext dnstap TCP socket", "socket", startConf.InputTCP)
		l, err := edm.deps.ListenerFactory.Listen("tcp", startConf.InputTCP)
		if err != nil {
			return nil, fmt.Errorf("unable to create plaintext dnstap TCP socket: %w", err)
		}
		dti = edm.deps.DnstapInputFactory.NewFrameStreamSockInput(l)
	case startConf.InputTLS != "":
		logger.Info("creating encrypted dnstap TLS socket", "socket", startConf.InputTLS)
		dnstapInputCert, err := edm.deps.KeyMaterialLoader.LoadKeyPair(startConf.InputTLSCertFile, startConf.InputTLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("unable to load x509 dnstap listener cert: %w", err)
		}
		dnstapTLSConfig := &tls.Config{
			Certificates: []tls.Certificate{dnstapInputCert},
			MinVersion:   tls.VersionTLS13,
		}

		// Enable client mTLS (client cert auth) if a CA file was passed.
		if startConf.InputTLSClientCAFile != "" {
			logger.Info("dnstap socket requiring valid client certs", "ca-file", startConf.InputTLSClientCAFile)
			inputTLSClientCACertPool, err := edm.deps.KeyMaterialLoader.LoadCertPool(startConf.InputTLSClientCAFile)
			if err != nil {
				return nil, fmt.Errorf("failed to create CA cert pool for '-input-tls-client-ca-file': %w", err)
			}
			dnstapTLSConfig.ClientAuth = tls.RequireAndVerifyClientCert
			dnstapTLSConfig.ClientCAs = inputTLSClientCACertPool
		}

		l, err := edm.deps.ListenerFactory.ListenTLS("tcp", startConf.InputTLS, dnstapTLSConfig)
		if err != nil {
			return nil, fmt.Errorf("unable to create TCP listener: %w", err)
		}
		dti = edm.deps.DnstapInputFactory.NewFrameStreamSockInput(l)
	default:
		return nil, errNoInputConfigured
	}
	dti.SetTimeout(time.Second * 5)
	dti.SetLogger(log.Default())
	return dti, nil
}

type socketDnstapInput struct {
	listener net.Listener
	timeout  time.Duration
	log      dnstap.Logger

	closeOnce sync.Once
	closeErr  error
	connMutex sync.Mutex
	conns     map[net.Conn]struct{}
}

func newSocketDnstapInput(listener net.Listener) *socketDnstapInput {
	return &socketDnstapInput{
		listener: listener,
		log:      noOpDnstapLogger{},
		conns:    map[net.Conn]struct{}{},
	}
}

func (input *socketDnstapInput) SetTimeout(timeout time.Duration) {
	input.timeout = timeout
}

func (input *socketDnstapInput) SetLogger(logger dnstap.Logger) {
	if logger == nil {
		input.log = noOpDnstapLogger{}
		return
	}
	input.log = logger
}

func (input *socketDnstapInput) Close() error {
	input.closeOnce.Do(func() {
		input.closeErr = input.listener.Close()

		input.connMutex.Lock()
		defer input.connMutex.Unlock()
		for conn := range input.conns {
			if err := conn.Close(); err != nil {
				input.log.Printf("%s: close connection failed: %v", conn.LocalAddr(), err)
			}
		}
	})
	return input.closeErr
}

func (input *socketDnstapInput) ReadInto(ctx context.Context, output chan<- []byte) error {
	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-ctx.Done():
			if err := input.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				input.log.Printf("%s: close listener failed: %v", input.listener.Addr(), err)
			}
		case <-done:
		}
	}()

	var wg sync.WaitGroup
	var connID uint64
	for {
		conn, err := input.listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				wg.Wait()
				return nil
			}
			if closeErr := input.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
				input.log.Printf("%s: close listener failed: %v", input.listener.Addr(), closeErr)
			}
			wg.Wait()
			return fmt.Errorf("%s: accept dnstap connection: %w", input.listener.Addr(), err)
		}

		input.trackConn(conn)
		if ctx.Err() != nil {
			input.untrackConn(conn)
			if err := conn.Close(); err != nil {
				input.log.Printf("%s: close canceled connection failed: %v", conn.LocalAddr(), err)
			}
			continue
		}

		connID++
		id := connID
		origin := ""
		switch conn.RemoteAddr().Network() {
		case "tcp", "tcp4", "tcp6":
			origin = fmt.Sprintf(" from %s", conn.RemoteAddr())
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer input.untrackConn(conn)
			defer func() {
				if err := conn.Close(); err != nil && ctx.Err() == nil {
					input.log.Printf("%s: close connection %d%s failed: %v", conn.LocalAddr(), id, origin, err)
				}
			}()

			input.log.Printf("%s: accepted connection %d%s", conn.LocalAddr(), id, origin)
			if err := input.readConn(ctx, conn, output); err != nil && ctx.Err() == nil {
				input.log.Printf("%s: connection %d%s read failed: %v", conn.LocalAddr(), id, origin, err)
			}
			input.log.Printf("%s: closed connection %d%s", conn.LocalAddr(), id, origin)
		}()
	}
}

func (input *socketDnstapInput) trackConn(conn net.Conn) {
	input.connMutex.Lock()
	defer input.connMutex.Unlock()
	input.conns[conn] = struct{}{}
}

func (input *socketDnstapInput) untrackConn(conn net.Conn) {
	input.connMutex.Lock()
	defer input.connMutex.Unlock()
	delete(input.conns, conn)
}

func (input *socketDnstapInput) readConn(ctx context.Context, conn net.Conn, output chan<- []byte) error {
	reader, err := dnstap.NewReader(conn, &dnstap.ReaderOptions{
		Bidirectional: true,
		Timeout:       input.timeout,
	})
	if err != nil {
		return fmt.Errorf("open framestream reader: %w", err)
	}

	buf := make([]byte, dnstap.MaxPayloadSize)
	for {
		n, err := reader.ReadFrame(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("read frame: %w", err)
		}

		frame := make([]byte, n)
		copy(frame, buf[:n])
		select {
		case output <- frame:
		case <-ctx.Done():
			return nil
		}
	}
}

type noOpDnstapLogger struct{}

func (noOpDnstapLogger) Printf(string, ...interface{}) {}
