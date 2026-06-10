package runner

import (
	"crypto/tls"
	"fmt"
	"sync"
)

type certStore struct {
	cert *tls.Certificate
	mtx  sync.RWMutex
}

// Implements tls.Config.GetClientCertificate
func (cs *certStore) getClientCertificate(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()

	if cs.cert == nil {
		return nil, fmt.Errorf("getClientCertificate: %w", errNoClientCertificate)
	}

	return cs.cert, nil
}

func (cs *certStore) loadCert(loader KeyMaterialLoader, certPath string, keyPath string) error {
	cert, err := loader.LoadKeyPair(certPath, keyPath)
	if err != nil {
		return fmt.Errorf("unable to load x509 cert: %w", err)
	}
	cs.mtx.Lock()
	cs.cert = &cert
	cs.mtx.Unlock()

	return nil
}

func newCertStore() *certStore {
	return &certStore{}
}

func (edm *DnstapMinimiser) loadHTTPClientCert() error {
	conf := edm.getConfig()

	edm.log.Info("loadHTTPClientCert: loading cert into HTTP cert store", "cert_file", conf.HTTPClientCertFile, "key_file", conf.HTTPClientKeyFile)
	err := edm.httpClientCertStore.loadCert(edm.deps.KeyMaterialLoader, conf.HTTPClientCertFile, conf.HTTPClientKeyFile)
	return err
}

func (edm *DnstapMinimiser) loadMQTTClientCert() error {
	conf := edm.getConfig()
	edm.log.Info("loadMQTTClientCert: loading cert into MQTT cert store", "cert_file", conf.MQTTClientCertFile, "key_file", conf.MQTTClientKeyFile)
	err := edm.mqttClientCertStore.loadCert(edm.deps.KeyMaterialLoader, conf.MQTTClientCertFile, conf.MQTTClientKeyFile)
	return err
}
