package runner

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
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
	"github.com/fsnotify/fsnotify"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/smhanov/dawg"
	"github.com/yawning/cryptopan"
)

// File is the file surface the runner needs. It is intentionally smaller than
// *os.File so tests can use lightweight fakes.
type File interface {
	io.Reader
	io.Writer
	io.Closer
	Stat() (os.FileInfo, error)
	Name() string
}

// FileSystem contains filesystem operations used by the runner.
type FileSystem interface {
	Open(name string) (File, error)
	OpenFile(name string, flag int, perm os.FileMode) (File, error)
	Create(name string) (File, error)
	ReadFile(name string) ([]byte, error)
	ReadDir(name string) ([]os.DirEntry, error)
	Stat(name string) (os.FileInfo, error)
	MkdirAll(path string, perm os.FileMode) error
	Rename(oldpath, newpath string) error
	Remove(name string) error
}

// Timer is the clock timer surface used by debounce code.
type Timer interface {
	Stop() bool
	Reset(time.Duration) bool
}

// Ticker is the clock ticker surface used by background workers.
type Ticker interface {
	C() <-chan time.Time
	Stop()
	Reset(time.Duration)
}

// Clock contains time operations used by the runner.
type Clock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
	AfterFunc(time.Duration, func()) Timer
	NewTicker(time.Duration) Ticker
}

// FileWatcher is the fsnotify surface used by the runner.
type FileWatcher interface {
	Add(name string) error
	Remove(name string) error
	Close() error
	WatchList() []string
	Events() <-chan fsnotify.Event
	Errors() <-chan error
}

// WatcherFactory creates file watchers.
type WatcherFactory interface {
	NewWatcher() (FileWatcher, error)
}

// ListenerFactory creates plaintext and TLS listeners.
type ListenerFactory interface {
	Listen(network, address string) (net.Listener, error)
	ListenTLS(network, address string, config *tls.Config) (net.Listener, error)
}

// DnstapInput is the DNSTAP input surface used by Run.
type DnstapInput interface {
	ReadInto(chan []byte)
	SetTimeout(time.Duration)
	SetLogger(dnstap.Logger)
}

// DnstapInputFactory creates DNSTAP frame stream inputs.
type DnstapInputFactory interface {
	NewFrameStreamSockInputFromPath(path string) (DnstapInput, error)
	NewFrameStreamSockInput(listener net.Listener) DnstapInput
}

// SeenQnameStore stores the persistent set of previously observed qnames.
type SeenQnameStore interface {
	Has(qname string) (bool, error)
	MarkSeen(qname string, sync bool) error
	Close() error
}

// SeenQnameStoreFactory opens SeenQnameStore instances.
type SeenQnameStoreFactory interface {
	OpenSeenQnameStore(path string) (SeenQnameStore, error)
}

// HTTPServerRunner starts an HTTP server.
type HTTPServerRunner interface {
	ListenAndServeHTTP(server *http.Server) error
}

// AggregateSender sends histogram parquet files to aggregate-receiver.
type AggregateSender interface {
	Send(ctx context.Context, fileName string, ts time.Time, duration time.Duration) error
	CloseIdleConnections()
}

// AggregateSenderFactory creates AggregateSender instances.
type AggregateSenderFactory interface {
	NewAggregateSender(log *slog.Logger, aggrecURL *url.URL, signingJWK jwk.Key, caCertPool *x509.CertPool, getClientCertificate func(*tls.CertificateRequestInfo) (*tls.Certificate, error), fs FileSystem, clock Clock) (AggregateSender, error)
}

// MQTTConnectionManager is the MQTT connection surface used by the publisher.
type MQTTConnectionManager interface {
	AwaitConnection(context.Context) error
	PublishViaQueue(context.Context, *autopaho.QueuePublish) error
	Publish(context.Context, *paho.Publish) (*paho.PublishResponse, error)
}

// MQTTFactory creates MQTT file queues and connection managers.
type MQTTFactory interface {
	NewFileQueue(path, prefix, extension string) (*file.Queue, error)
	NewConnection(ctx context.Context, cfg autopaho.ClientConfig) (MQTTConnectionManager, error)
}

// KeyMaterialLoader loads certificates, CA pools and signing keys.
type KeyMaterialLoader interface {
	LoadKeyPair(certPath, keyPath string) (tls.Certificate, error)
	LoadEdDSAJWK(fileName string) (jwk.Key, error)
	LoadCertPool(fileName string) (*x509.CertPool, error)
}

// DawgLoader loads DAWG files.
type DawgLoader interface {
	LoadDawgFile(fileName string) (dawg.Finder, time.Time, error)
}

// CryptopanFactory creates Crypto-PAn instances from configured key material.
type CryptopanFactory interface {
	NewCryptopan(key, salt string) (*cryptopan.Cryptopan, error)
}

// Dependencies holds all external functionality used by DnstapMinimiser.
// Zero-valued fields are filled with production implementations.
type Dependencies struct {
	FileSystem             FileSystem
	Clock                  Clock
	WatcherFactory         WatcherFactory
	ListenerFactory        ListenerFactory
	DnstapInputFactory     DnstapInputFactory
	SeenQnameStoreFactory  SeenQnameStoreFactory
	HTTPServerRunner       HTTPServerRunner
	AggregateSenderFactory AggregateSenderFactory
	MQTTFactory            MQTTFactory
	KeyMaterialLoader      KeyMaterialLoader
	DawgLoader             DawgLoader
	CryptopanFactory       CryptopanFactory

	ConfigUpdateDebounce    time.Duration
	FSEventDebounce         time.Duration
	DiskCleanerInterval     time.Duration
	MonitorChannelInterval  time.Duration
	HistogramSenderInterval time.Duration
	HistogramSenderBackoff  time.Duration
	PprofListenAddr         string
	MetricsListenAddr       string
}

func defaultDependencies() Dependencies {
	return fillDependencies(Dependencies{})
}

func fillDependencies(deps Dependencies) Dependencies {
	if deps.FileSystem == nil {
		deps.FileSystem = osFileSystem{}
	}
	if deps.Clock == nil {
		deps.Clock = realClock{}
	}
	if deps.WatcherFactory == nil {
		deps.WatcherFactory = fsnotifyWatcherFactory{}
	}
	if deps.ListenerFactory == nil {
		deps.ListenerFactory = netListenerFactory{}
	}
	if deps.DnstapInputFactory == nil {
		deps.DnstapInputFactory = dnstapInputFactory{}
	}
	if deps.SeenQnameStoreFactory == nil {
		deps.SeenQnameStoreFactory = pebbleSeenQnameStoreFactory{}
	}
	if deps.HTTPServerRunner == nil {
		deps.HTTPServerRunner = httpServerRunner{}
	}
	if deps.AggregateSenderFactory == nil {
		deps.AggregateSenderFactory = aggregateSenderFactory{}
	}
	if deps.MQTTFactory == nil {
		deps.MQTTFactory = mqttFactory{}
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
	if deps.ConfigUpdateDebounce == 0 {
		deps.ConfigUpdateDebounce = 100 * time.Millisecond
	}
	if deps.FSEventDebounce == 0 {
		deps.FSEventDebounce = 100 * time.Millisecond
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

func (osFileSystem) Open(name string) (File, error) {
	return os.Open(name)
}

func (osFileSystem) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	return os.OpenFile(name, flag, perm)
}

func (osFileSystem) Create(name string) (File, error) {
	return os.Create(name)
}

func (osFileSystem) ReadFile(name string) ([]byte, error) {
	return os.ReadFile(name)
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

func (realClock) AfterFunc(d time.Duration, f func()) Timer {
	return time.AfterFunc(d, f)
}

func (realClock) NewTicker(d time.Duration) Ticker {
	return realTicker{Ticker: time.NewTicker(d)}
}

type realTicker struct {
	*time.Ticker
}

func (rt realTicker) C() <-chan time.Time {
	return rt.Ticker.C
}

type fsnotifyWatcherFactory struct{}

func (fsnotifyWatcherFactory) NewWatcher() (FileWatcher, error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	return fsnotifyFileWatcher{Watcher: w}, nil
}

type fsnotifyFileWatcher struct {
	*fsnotify.Watcher
}

func (fw fsnotifyFileWatcher) Events() <-chan fsnotify.Event {
	return fw.Watcher.Events
}

func (fw fsnotifyFileWatcher) Errors() <-chan error {
	return fw.Watcher.Errors
}

type netListenerFactory struct{}

func (netListenerFactory) Listen(network, address string) (net.Listener, error) {
	return net.Listen(network, address)
}

func (netListenerFactory) ListenTLS(network, address string, config *tls.Config) (net.Listener, error) {
	return tls.Listen(network, address, config)
}

type dnstapInputFactory struct{}

func (dnstapInputFactory) NewFrameStreamSockInputFromPath(path string) (DnstapInput, error) {
	return dnstap.NewFrameStreamSockInputFromPath(path)
}

func (dnstapInputFactory) NewFrameStreamSockInput(listener net.Listener) DnstapInput {
	return dnstap.NewFrameStreamSockInput(listener)
}

type pebbleSeenQnameStoreFactory struct{}

func (pebbleSeenQnameStoreFactory) OpenSeenQnameStore(path string) (SeenQnameStore, error) {
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

type httpServerRunner struct{}

func (httpServerRunner) ListenAndServeHTTP(server *http.Server) error {
	return server.ListenAndServe()
}

type aggregateSenderFactory struct{}

func (aggregateSenderFactory) NewAggregateSender(log *slog.Logger, aggrecURL *url.URL, signingJWK jwk.Key, caCertPool *x509.CertPool, getClientCertificate func(*tls.CertificateRequestInfo) (*tls.Certificate, error), fs FileSystem, clock Clock) (AggregateSender, error) {
	return newAggregateSender(log, aggrecURL, signingJWK, caCertPool, getClientCertificate, fs, clock)
}

type mqttFactory struct{}

func (mqttFactory) NewFileQueue(path, prefix, extension string) (*file.Queue, error) {
	return file.New(path, prefix, extension)
}

func (mqttFactory) NewConnection(ctx context.Context, cfg autopaho.ClientConfig) (MQTTConnectionManager, error) {
	return autopaho.NewConnection(ctx, cfg)
}

type realKeyMaterialLoader struct {
	fs FileSystem
}

func (rkl realKeyMaterialLoader) LoadKeyPair(certPath, keyPath string) (tls.Certificate, error) {
	return tls.LoadX509KeyPair(certPath, keyPath)
}

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

	if err := jwkKey.Set(jwk.AlgorithmKey, jwa.EdDSA); err != nil {
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
	fs FileSystem
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
