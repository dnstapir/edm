package runner

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble"
	dnstap "github.com/dnstap/golang-dnstap"
	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/autopaho/queue/file"
	"github.com/eclipse/paho.golang/paho"
	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/smhanov/dawg"
	"github.com/yawning/cryptopan"
)

// fsFile is the file surface the runner needs. It is intentionally smaller
// than [*os.File] so tests can use lightweight fakes.
type fsFile interface {
	io.Reader
	io.Writer
	io.Closer
	Stat() (os.FileInfo, error)
	Name() string
}

// fileSystem contains filesystem operations used by the runner.
type fileSystem interface {
	Open(name string) (fsFile, error)
	OpenFile(name string, flag int, perm os.FileMode) (fsFile, error)
	Create(name string) (fsFile, error)
	ReadFile(name string) ([]byte, error)
	ReadDir(name string) ([]os.DirEntry, error)
	Stat(name string) (os.FileInfo, error)
	MkdirAll(path string, perm os.FileMode) error
	Rename(oldpath, newpath string) error
	Remove(name string) error
}

// ticker is the clock ticker surface used by background workers.
type ticker interface {
	C() <-chan time.Time
	Stop()
	Reset(time.Duration)
}

// clock contains time operations used by the runner.
type clock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
	NewTicker(time.Duration) ticker
}

// listenerFactory creates plaintext and TLS listeners.
type listenerFactory interface {
	Listen(network, address string) (net.Listener, error)
	ListenTLS(network, address string, config *tls.Config) (net.Listener, error)
}

// dnstapInput is the DNSTAP input surface used by Run.
type dnstapInput interface {
	ReadInto(context.Context, chan<- []byte) error
	SetTimeout(time.Duration)
	SetLogger(dnstap.Logger)
	Close() error
}

// dnstapInputFactory creates DNSTAP frame stream inputs.
type dnstapInputFactory interface {
	NewFrameStreamSockInput(listener net.Listener) dnstapInput
}

// seenQnameStore stores the persistent set of previously observed qnames.
type seenQnameStore interface {
	// Has reports whether qname is recorded in the store. It may report
	// true together with a non-nil error when the value was found but
	// releasing lookup resources failed; callers should trust the bool.
	Has(qname string) (bool, error)
	MarkSeen(qname string, sync bool) error
	Close() error
}

// seenQnameStoreFactory opens SeenQnameStore instances.
type seenQnameStoreFactory interface {
	OpenSeenQnameStore(path string) (seenQnameStore, error)
}

// httpServerRunner starts an HTTP server.
type httpServerRunner interface {
	ListenAndServeHTTP(server *http.Server) error
}

// aggregateSender sends histogram parquet files to aggregate-receiver.
type aggregateSender interface {
	Send(ctx context.Context, fileName string, ts time.Time, duration time.Duration) error
	CloseIdleConnections()
}

// aggregateSenderFactory creates AggregateSender instances.
type aggregateSenderFactory interface {
	NewAggregateSender(log *slog.Logger, aggrecURL *url.URL, signingJWK jwk.Key, caCertPool *x509.CertPool, getClientCertificate func(*tls.CertificateRequestInfo) (*tls.Certificate, error), fs fileSystem, clock clock) (aggregateSender, error)
}

// mqttConnectionManager is the MQTT connection surface used by the publisher.
type mqttConnectionManager interface {
	AwaitConnection(context.Context) error
	PublishViaQueue(context.Context, *autopaho.QueuePublish) error
	Publish(context.Context, *paho.Publish) (*paho.PublishResponse, error)
}

// mqttFactory creates MQTT file queues and connection managers.
type mqttFactory interface {
	NewFileQueue(path, prefix, extension string) (*file.Queue, error)
	NewConnection(ctx context.Context, cfg autopaho.ClientConfig) (mqttConnectionManager, error)
}

// keyMaterialLoader loads certificates, CA pools and signing keys.
type keyMaterialLoader interface {
	LoadKeyPair(certPath, keyPath string) (tls.Certificate, error)
	LoadEdDSAJWK(fileName string) (jwk.Key, error)
	LoadCertPool(fileName string) (*x509.CertPool, error)
}

// dawgLoader loads DAWG files.
type dawgLoader interface {
	LoadDawgFile(fileName string) (dawg.Finder, time.Time, error)
}

// cryptopanFactory creates Crypto-PAn instances from configured key material.
type cryptopanFactory interface {
	NewCryptopan(key, salt string) (*cryptopan.Cryptopan, error)
}

// dependencies holds all external functionality used by DnstapMinimiser.
// Zero-valued fields are filled with production implementations.
type dependencies struct {
	FileSystem             fileSystem
	Clock                  clock
	ListenerFactory        listenerFactory
	DnstapInputFactory     dnstapInputFactory
	SeenQnameStoreFactory  seenQnameStoreFactory
	HTTPServerRunner       httpServerRunner
	AggregateSenderFactory aggregateSenderFactory
	MQTTFactory            mqttFactory
	KeyMaterialLoader      keyMaterialLoader
	DawgLoader             dawgLoader
	CryptopanFactory       cryptopanFactory

	DiskCleanerInterval     time.Duration
	MonitorChannelInterval  time.Duration
	HistogramSenderInterval time.Duration
	HistogramSenderBackoff  time.Duration
	PprofListenAddr         string
	MetricsListenAddr       string
}

func defaultDependencies() dependencies {
	return fillDependencies(dependencies{})
}

func fillDependencies(deps dependencies) dependencies {
	if deps.FileSystem == nil {
		deps.FileSystem = osFileSystem{}
	}
	if deps.Clock == nil {
		deps.Clock = realClock{}
	}
	if deps.ListenerFactory == nil {
		deps.ListenerFactory = netListenerFactory{}
	}
	if deps.DnstapInputFactory == nil {
		deps.DnstapInputFactory = realDnstapInputFactory{}
	}
	if deps.SeenQnameStoreFactory == nil {
		deps.SeenQnameStoreFactory = pebbleSeenQnameStoreFactory{}
	}
	if deps.HTTPServerRunner == nil {
		deps.HTTPServerRunner = realHTTPServerRunner{}
	}
	if deps.AggregateSenderFactory == nil {
		deps.AggregateSenderFactory = realAggregateSenderFactory{}
	}
	if deps.MQTTFactory == nil {
		deps.MQTTFactory = realMQTTFactory{}
	}
	if deps.KeyMaterialLoader == nil {
		deps.KeyMaterialLoader = realKeyMaterialLoader{fs: deps.FileSystem}
	}
	if deps.DawgLoader == nil {
		deps.DawgLoader = realDawgLoader{fs: deps.FileSystem}
	}
	if deps.CryptopanFactory == nil {
		deps.CryptopanFactory = realCryptopanFactory{}
	}
	if deps.DiskCleanerInterval == 0 {
		deps.DiskCleanerInterval = time.Minute
	}
	if deps.MonitorChannelInterval == 0 {
		deps.MonitorChannelInterval = time.Second
	}
	if deps.HistogramSenderInterval == 0 {
		deps.HistogramSenderInterval = 10 * time.Second
	}
	if deps.HistogramSenderBackoff == 0 {
		deps.HistogramSenderBackoff = 15 * time.Second
	}
	if deps.PprofListenAddr == "" {
		deps.PprofListenAddr = "127.0.0.1:6060"
	}
	if deps.MetricsListenAddr == "" {
		deps.MetricsListenAddr = "127.0.0.1:2112"
	}
	return deps
}

type osFileSystem struct{}

func (osFileSystem) Open(name string) (fsFile, error) {
	return os.Open(name) // #nosec G304 -- production adapter intentionally opens configured runtime paths.
}

func (osFileSystem) OpenFile(name string, flag int, perm os.FileMode) (fsFile, error) {
	return os.OpenFile(name, flag, perm) // #nosec G304 -- production adapter intentionally opens configured runtime paths.
}

func (osFileSystem) Create(name string) (fsFile, error) {
	return os.Create(name) // #nosec G304 -- production adapter intentionally creates configured runtime paths.
}

func (osFileSystem) ReadFile(name string) ([]byte, error) {
	return os.ReadFile(name) // #nosec G304 -- production adapter intentionally reads configured runtime paths.
}

func (osFileSystem) ReadDir(name string) ([]os.DirEntry, error) {
	return os.ReadDir(name)
}

func (osFileSystem) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

func (osFileSystem) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (osFileSystem) Rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

func (osFileSystem) Remove(name string) error {
	return os.Remove(name)
}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

func (realClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

func (realClock) NewTicker(d time.Duration) ticker {
	return realTicker{Ticker: time.NewTicker(d)}
}

type realTicker struct {
	*time.Ticker
}

func (rt realTicker) C() <-chan time.Time {
	return rt.Ticker.C
}

type netListenerFactory struct{}

func (netListenerFactory) Listen(network, address string) (net.Listener, error) {
	return net.Listen(network, address)
}

func (netListenerFactory) ListenTLS(network, address string, config *tls.Config) (net.Listener, error) {
	return tls.Listen(network, address, config)
}

type realDnstapInputFactory struct{}

// NewFrameStreamSockInput mirrors the name of the dnstap library API it
// supersedes but returns the runner's own [socketDnstapInput], which adds
// context cancellation and graceful close on top of the library behavior.
func (realDnstapInputFactory) NewFrameStreamSockInput(listener net.Listener) dnstapInput {
	return newSocketDnstapInput(listener)
}

type pebbleSeenQnameStoreFactory struct{}

func (pebbleSeenQnameStoreFactory) OpenSeenQnameStore(path string) (seenQnameStore, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &pebbleSeenQnameStore{db: db}, nil
}

type pebbleSeenQnameStore struct {
	db *pebble.DB
}

func (ps *pebbleSeenQnameStore) Has(qname string) (bool, error) {
	_, closer, err := ps.db.Get([]byte(qname))
	if err == nil {
		if err := closer.Close(); err != nil {
			return true, err
		}
		return true, nil
	}
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	return false, err
}

func (ps *pebbleSeenQnameStore) MarkSeen(qname string, sync bool) error {
	writeOpts := pebble.NoSync
	if sync {
		writeOpts = pebble.Sync
	}
	return ps.db.Set([]byte(qname), []byte{}, writeOpts)
}

func (ps *pebbleSeenQnameStore) Close() error {
	return ps.db.Close()
}

type realHTTPServerRunner struct{}

func (realHTTPServerRunner) ListenAndServeHTTP(server *http.Server) error {
	return server.ListenAndServe()
}

type realAggregateSenderFactory struct{}

func (realAggregateSenderFactory) NewAggregateSender(log *slog.Logger, aggrecURL *url.URL, signingJWK jwk.Key, caCertPool *x509.CertPool, getClientCertificate func(*tls.CertificateRequestInfo) (*tls.Certificate, error), fs fileSystem, clock clock) (aggregateSender, error) {
	return newAggregateSender(log, aggrecURL, signingJWK, caCertPool, getClientCertificate, fs, clock)
}

type realMQTTFactory struct{}

func (realMQTTFactory) NewFileQueue(path, prefix, extension string) (*file.Queue, error) {
	return file.New(path, prefix, extension)
}

func (realMQTTFactory) NewConnection(ctx context.Context, cfg autopaho.ClientConfig) (mqttConnectionManager, error) {
	return autopaho.NewConnection(ctx, cfg)
}

type realKeyMaterialLoader struct {
	fs fileSystem
}

func (rkl realKeyMaterialLoader) LoadKeyPair(certPath, keyPath string) (tls.Certificate, error) {
	certPath = filepath.Clean(certPath)
	keyPath = filepath.Clean(keyPath)

	certPEMBlock, err := rkl.fs.ReadFile(certPath)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("read cert file %q: %w", certPath, err)
	}
	keyPEMBlock, err := rkl.fs.ReadFile(keyPath)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("read key file %q: %w", keyPath, err)
	}

	cert, err := tls.X509KeyPair(certPEMBlock, keyPEMBlock)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("parse key pair %q/%q: %w", certPath, keyPath, err)
	}
	return cert, nil
}

// LoadEdDSAJWK loads a JWK from fileName and verifies it is an EdDSA
// (Ed25519/Ed448) key before stamping its algorithm as EdDSA. Other key
// types (including the OKP key-agreement curves X25519/X448) return an
// error wrapping errNotEdDSAJWK, so a mismatched key fails at load time
// instead of during later JWS operations.
//
// The key must also carry a key ID, returning errJWKMissingKeyID otherwise:
// the HTTP signer's keyid and the MQTT topic/client ID are derived from it,
// and a missing value would surface only as a downstream verification failure.
func (rkl realKeyMaterialLoader) LoadEdDSAJWK(fileName string) (jwk.Key, error) {
	fileName = filepath.Clean(fileName)
	keyFile, err := rkl.fs.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	jwkKey, err := jwk.ParseKey(keyFile)
	if err != nil {
		return nil, err
	}

	// jwk.ParseKey rejects an OKP key without a crv, so the ok value is
	// always true here. Even if it were not, Crv reports an invalid curve
	// that fails the Ed25519/Ed448 check below, so the ok value can be
	// discarded.
	var crv jwa.EllipticCurveAlgorithm
	switch key := jwkKey.(type) {
	case jwk.OKPPrivateKey:
		crv, _ = key.Crv()
	case jwk.OKPPublicKey:
		crv, _ = key.Crv()
	default:
		return nil, fmt.Errorf("%w: key type %q", errNotEdDSAJWK, jwkKey.KeyType())
	}
	if crv != jwa.Ed25519() && crv != jwa.Ed448() {
		return nil, fmt.Errorf("%w: curve %q", errNotEdDSAJWK, crv)
	}

	if kid, ok := jwkKey.KeyID(); !ok || kid == "" {
		return nil, errJWKMissingKeyID
	}

	if err := jwkKey.Set(jwk.AlgorithmKey, jwa.EdDSA()); err != nil {
		return nil, err
	}
	return jwkKey, nil
}

func (rkl realKeyMaterialLoader) LoadCertPool(fileName string) (*x509.CertPool, error) {
	fileName = filepath.Clean(fileName)
	cert, err := rkl.fs.ReadFile(fileName)
	if err != nil {
		return nil, err
	}
	certPool := x509.NewCertPool()
	ok := certPool.AppendCertsFromPEM(cert)
	if !ok {
		return nil, errAppendCertsFromPEM
	}
	return certPool, nil
}

type realDawgLoader struct {
	fs fileSystem
}

func (rdl realDawgLoader) LoadDawgFile(dawgFile string) (dawg.Finder, time.Time, error) {
	dawgFileInfo, err := rdl.fs.Stat(dawgFile)
	if err != nil {
		return nil, time.Time{}, err
	}

	if dawgFileInfo.Size() == 0 {
		return nil, time.Time{}, errEmptyDawgFile
	}

	dawgFinder, err := dawg.Load(dawgFile)
	if err != nil {
		return nil, time.Time{}, err
	}

	return dawgFinder, dawgFileInfo.ModTime(), nil
}
