package runner

import (
	"crypto/tls"
	"fmt"
	"log"
	"log/slog"
	"time"
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
		d, err := edm.deps.DnstapInputFactory.NewFrameStreamSockInputFromPath(startConf.InputUnix)
		if err != nil {
			return nil, fmt.Errorf("unable to create dnstap unix socket: %w", err)
		}
		dti = d
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
