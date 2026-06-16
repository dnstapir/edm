package runner

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"io"
	"log/slog"
	"math/big"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	dnstap "github.com/dnstap/golang-dnstap"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/miekg/dns"
	"github.com/smhanov/dawg"
	"github.com/yawning/cryptopan"
	"google.golang.org/protobuf/proto"
)

// testPseudonymiseDnstap is the test-side equivalent of the producer
// hot path. pseudonymiseDnstap takes the per-worker cache + cryptopan
// snapshot as parameters; tests don't run inside a real worker so they
// need their own cache. We give each *DnstapMinimiser instance one shared
// cache via a sync.Map keyed by the minimiser pointer, so repeated test
// calls accumulate hits like a single worker would.
//
// This is purely a test convenience - production code does not use it.
var testCryptopanCaches sync.Map // map[*DnstapMinimiser]*lru.Cache[netip.Addr, netip.Addr]

func (edm *DnstapMinimiser) testPseudonymiseDnstap(dt *dnstap.Dnstap) {
	cache := edm.testCryptopanCache()
	edm.pseudonymiseDnstap(dt, edm.cryptopan.Load(), cache)
}

// testCryptopanCache returns the shared per-edm-instance cache, creating
// it lazily so callers don't have to set it up. cacheEntries is read from
// the current config; 0 disables caching, mirroring production behaviour.
func (edm *DnstapMinimiser) testCryptopanCache() *lru.Cache[netip.Addr, netip.Addr] {
	conf := edm.getConfig()
	if conf.CryptopanAddressEntries == 0 {
		return nil
	}
	if c, ok := testCryptopanCaches.Load(edm); ok {
		return c.(*lru.Cache[netip.Addr, netip.Addr])
	}
	c, err := lru.New[netip.Addr, netip.Addr](conf.CryptopanAddressEntries)
	if err != nil {
		panic(err)
	}
	actual, _ := testCryptopanCaches.LoadOrStore(edm, c)
	return actual.(*lru.Cache[netip.Addr, netip.Addr])
}

// testResetCryptopanCache drops the test-side cache for edm. Used by
// tests after setCryptopan to mirror the per-worker Purge that
// runMinimiser does on cryptopanGen change.
func (edm *DnstapMinimiser) testResetCryptopanCache() {
	testCryptopanCaches.Delete(edm)
}

func testRunContext(t testing.TB) (context.Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	return ctx, cancel
}

var (
	testDawg     = flag.Bool("test-dawg", false, "perform tests requiring a well-known-domains.dawg file")
	writeParquet = flag.Bool("write-parquet", false, "make parquet tests write out files in a temporary directory")
	defaultTC    = newDefaultTC()

	testJWKJSONOnce sync.Once
	testJWKJSONData []byte
	testJWKJSONErr  error

	testCertMaterialOnce sync.Once
	testCertPEM          []byte
	testCertKeyPEM       []byte
	testCertMaterialErr  error
)

// testConfiger is a ConfigProvider backed by a complete Config value. Tests
// build on defaultTestConfig (which mirrors DefaultConfig) and override
// individual fields rather than going through the config file decoding
// pipeline. Embedding Config promotes every field so a test can do
// `tc := defaultTC; tc.MQTTServer = "..."` directly.
//
// testConfiger is test-only; production wires FileConfigProvider.
type testConfiger struct {
	Config
}

// GetConfig implements ConfigProvider.
func (tc testConfiger) GetConfig() (Config, error) {
	return tc.Config, nil
}

// defaultTestConfig returns a config populated with pkg/cmd/run.go's flag
// defaults for the scalar/path fields. URL- and address-typed fields
// (MQTTServer, HTTPURL) are deliberately left empty because run.go's
// defaults (`127.0.0.1:8883`, etc.) are bare host:port values that fail
// `url.Parse` for autopaho/HTTP — those defaults are only useful once a
// scheme is added at the CLI layer, so tests must opt in by setting them.
//
// testConfiger does not call [Config.Validate], so the unset required
// fields do not block construction.

// placeholderDataDir is the non-writable data-dir defaultTestConfig uses as a
// stand-in. The test minimiser constructors swap it for a writable temp dir
// (see useWritableDataDir) so DAWG staging, which copies into data-dir, works.
const placeholderDataDir = "/var/lib/dnstapir/edm"

func defaultTestConfig() Config {
	return Config{
		ConfigFile:                    "edm.toml",
		WellKnownDomainsFile:          "well-known-domains.dawg",
		DataDir:                       placeholderDataDir,
		MinimiserWorkers:              1,
		CryptopanKeySalt:              "edm-kdf-salt-val",
		QnameSeenEntries:              10_000_000,
		CryptopanAddressEntries:       10_000_000,
		NewQnameBuffer:                1000,
		HistogramHLLExplicitThreshold: 20,
		MQTTSigningKeyFile:            "edm-mqtt-signer-key.pem",
		MQTTClientKeyFile:             "edm-mqtt-client-key.pem",
		MQTTClientCertFile:            "edm-mqtt-client.pem",
		MQTTKeepalive:                 30,
		HTTPSigningKeyFile:            "edm-http-signer-key.pem",
		HTTPClientKeyFile:             "edm-http-client-key.pem",
		HTTPClientCertFile:            "edm-http-client.pem",
	}
}

// newDefaultTC builds the testConfiger used by most tests. It starts from
// defaultTestConfig and overrides the cryptopan fields with values the
// surrounding tests assume (a short key and a tiny LRU so cache-eviction
// branches are reachable without millions of entries).
func newDefaultTC() testConfiger {
	c := defaultTestConfig()
	c.CryptopanKey = "key1"
	c.CryptopanKeySalt = "aabbccddeeffgghh"
	c.CryptopanAddressEntries = 10
	return testConfiger{Config: c}
}

// useWritableDataDir replaces the default placeholder data-dir with a writable
// temp dir so DAWG staging (which copies into data-dir) works. Tests that set
// their own DataDir before or after construction are left untouched.
func useWritableDataDir(t testing.TB, edm *DnstapMinimiser) {
	t.Helper()
	if edm.conf.DataDir == placeholderDataDir {
		edm.conf.DataDir = t.TempDir()
	}
}

func newTestDnstapMinimiser(t testing.TB, tc testConfiger) *DnstapMinimiser {
	t.Helper()

	return newTestDnstapMinimiserWithDependencies(t, tc, newTestDependencies())
}

func newTestDnstapMinimiserWithDependencies(t testing.TB, tc testConfiger, deps dependencies) *DnstapMinimiser {
	t.Helper()

	discardLogger := slog.NewTextHandler(io.Discard, nil)
	logger := slog.New(discardLogger)

	edm, err := NewDnstapMinimiser(tc, logger, withDependencies(deps))
	if err != nil {
		t.Fatalf("unable to setup edm: %s", err)
	}
	useWritableDataDir(t, edm)

	return edm
}

func newRealCryptopanTestDnstapMinimiser(t testing.TB, tc testConfiger) *DnstapMinimiser {
	t.Helper()

	discardLogger := slog.NewTextHandler(io.Discard, nil)
	logger := slog.New(discardLogger)

	edm, err := NewDnstapMinimiser(tc, logger)
	if err != nil {
		t.Fatalf("unable to setup edm: %s", err)
	}
	useWritableDataDir(t, edm)

	return edm
}

func newTestDependencies() dependencies {
	deps := defaultDependencies()
	deps.CryptopanFactory = fastTestCryptopanFactory{}
	return deps
}

type fastTestCryptopanFactory struct{}

func (fastTestCryptopanFactory) NewCryptopan(key, salt string) (*cryptopan.Cryptopan, error) {
	sum := sha256.Sum256([]byte(key + "\x00" + salt))
	return cryptopan.New(sum[:])
}

func newSynctestDnstapMinimiser(t testing.TB, tc testConfiger) *DnstapMinimiser {
	t.Helper()

	discardLogger := slog.NewTextHandler(io.Discard, nil)
	logger := slog.New(discardLogger)

	return newSynctestDnstapMinimiserWithLogger(t, tc, logger)
}

func newSynctestDnstapMinimiserWithLogger(t testing.TB, tc testConfiger, logger *slog.Logger) *DnstapMinimiser {
	t.Helper()

	deps := newTestDependencies()

	edm, err := NewDnstapMinimiser(tc, logger, withDependencies(deps))
	if err != nil {
		t.Fatalf("unable to setup edm: %s", err)
	}
	useWritableDataDir(t, edm)

	return edm
}

func writeTempFile(t testing.TB, name string, data []byte) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func testDawgFinder(t testing.TB, domains ...string) dawg.Finder {
	t.Helper()

	slices.Sort(domains)
	builder := dawg.New()
	for _, domain := range domains {
		builder.Add(domain)
	}
	return builder.Finish()
}

func testDawgFile(t testing.TB, domains ...string) string {
	t.Helper()

	finder := testDawgFinder(t, domains...)
	t.Cleanup(func() {
		if err := finder.Close(); err != nil {
			t.Fatalf("unable to close test DAWG: %s", err)
		}
	})

	path := filepath.Join(t.TempDir(), "domains.dawg")
	if _, err := finder.Save(path); err != nil {
		t.Fatal(err)
	}
	return path
}

func packedDNSMsg(t testing.TB, name string, qtype uint16, rcode int) []byte {
	t.Helper()

	msg := new(dns.Msg)
	msg.SetQuestion(name, qtype)
	msg.Response = true
	msg.Rcode = rcode
	packed, err := msg.Pack()
	if err != nil {
		t.Fatal(err)
	}
	return packed
}

func testDnstapMessage(t testing.TB, msgType dnstap.Message_Type, family dnstap.SocketFamily, packed []byte) *dnstap.Dnstap {
	t.Helper()

	queryPort := uint32(12345)
	responsePort := uint32(53)
	querySec := uint64(1_700_000_000)
	queryNSec := uint32(123)
	responseSec := uint64(1_700_000_001)
	responseNSec := uint32(456)
	protoUDP := dnstap.SocketProtocol_UDP
	topType := dnstap.Dnstap_MESSAGE
	dt := &dnstap.Dnstap{
		Type:     &topType,
		Identity: []byte("server-1"),
		Message: &dnstap.Message{
			Type:             &msgType,
			SocketFamily:     &family,
			SocketProtocol:   &protoUDP,
			QueryPort:        &queryPort,
			ResponsePort:     &responsePort,
			QueryTimeSec:     &querySec,
			QueryTimeNsec:    &queryNSec,
			ResponseTimeSec:  &responseSec,
			ResponseTimeNsec: &responseNSec,
		},
	}

	switch family {
	case dnstap.SocketFamily_INET:
		dt.Message.QueryAddress = netip.MustParseAddr("198.51.100.20").AsSlice()
		dt.Message.ResponseAddress = netip.MustParseAddr("198.51.100.53").AsSlice()
	case dnstap.SocketFamily_INET6:
		dt.Message.QueryAddress = netip.MustParseAddr("2001:db8::20").AsSlice()
		dt.Message.ResponseAddress = netip.MustParseAddr("2001:db8::53").AsSlice()
	}

	if strings.HasSuffix(dnstap.Message_Type_name[int32(msgType)], "_QUERY") {
		dt.Message.QueryMessage = packed
	} else {
		dt.Message.ResponseMessage = packed
	}
	return dt
}

func marshaledDnstap(t testing.TB, dt *dnstap.Dnstap) []byte {
	t.Helper()

	data, err := proto.Marshal(dt)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func cacheTestJWKJSON() {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		testJWKJSONErr = err
		return
	}
	key, err := jwk.FromRaw(priv)
	if err != nil {
		testJWKJSONErr = err
		return
	}
	if err := key.Set(jwk.KeyIDKey, "test-key"); err != nil {
		testJWKJSONErr = err
		return
	}
	if err := key.Set(jwk.AlgorithmKey, jwa.EdDSA); err != nil {
		testJWKJSONErr = err
		return
	}
	testJWKJSONData, testJWKJSONErr = json.Marshal(key)
}

func testJWKJSON(t testing.TB) []byte {
	t.Helper()

	testJWKJSONOnce.Do(func() {
		cacheTestJWKJSON()
	})
	if testJWKJSONErr != nil {
		t.Fatal(testJWKJSONErr)
	}
	return bytes.Clone(testJWKJSONData)
}

func testJWK(t testing.TB) jwk.Key {
	t.Helper()

	key, err := jwk.ParseKey(testJWKJSON(t))
	if err != nil {
		t.Fatal(err)
	}
	return key
}

// testJWKPair returns the shared test signing key together with its public
// half, for tests that verify signed messages.
func testJWKPair(t testing.TB) (priv, pub jwk.Key) {
	t.Helper()

	var err error
	priv = testJWK(t)
	pub, err = priv.PublicKey()
	if err != nil {
		t.Fatalf("derive public JWK: %s", err)
	}
	return priv, pub
}

func testJWKFile(t testing.TB) string {
	t.Helper()

	return writeTempFile(t, "key.jwk", testJWKJSON(t))
}

func cachedTestCertMaterial() {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		testCertMaterialErr = err
		return
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		NotAfter:     time.Date(2120, 1, 1, 0, 0, 0, 0, time.UTC),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		IsCA:         true,
		DNSNames:     []string{"localhost"},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		testCertMaterialErr = err
		return
	}
	testCertPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	testCertKeyPEM = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}

func testCertMaterial(t testing.TB) (certPEM []byte, keyPEM []byte) {
	t.Helper()

	testCertMaterialOnce.Do(cachedTestCertMaterial)
	if testCertMaterialErr != nil {
		t.Fatal(testCertMaterialErr)
	}
	return bytes.Clone(testCertPEM), bytes.Clone(testCertKeyPEM)
}

func testCertFiles(t testing.TB) (string, string, string) {
	t.Helper()

	certPEM, keyPEM := testCertMaterial(t)

	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")
	caPath := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(caPath, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath, caPath
}

func newTestPebble(t testing.TB) *pebble.DB {
	t.Helper()

	db, err := pebble.Open(filepath.Join(t.TempDir(), "pebble"), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("unable to close pebble: %s", err)
		}
	})
	return db
}

// syncBuf is a thread-safe bytes.Buffer wrapper for slog handlers whose
// output is consumed concurrently by both the worker under test and the
// goroutine polling for assertions.
type syncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// errInjected is the sentinel failure injected through fake dependencies so
// the error-path assertions can confirm (via errors.Is) that the injected
// failure is the one surfaced, rather than some unrelated error.
var errInjected = errors.New("injected failure")

// discardEDM returns a minimal minimiser with a no-op logger. It intentionally
// only sets the logger, which is all the file-operation helpers under test
// touch; it deliberately does not go through newTestDnstapMinimiser.
func discardEDM() *DnstapMinimiser {
	return &DnstapMinimiser{
		log:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		deps: defaultDependencies(),
	}
}

type faultingFileSystem struct {
	fileSystem
	readFile func(string) ([]byte, error)
	create   func(string) (fsFile, error)
	rename   func(string, string) error
	remove   func(string) error
	mkdirAll func(string, os.FileMode) error
	stat     func(string) (os.FileInfo, error)
	readDir  func(string) ([]os.DirEntry, error)
}

func (ffs faultingFileSystem) ReadFile(name string) ([]byte, error) {
	if ffs.readFile != nil {
		return ffs.readFile(name)
	}
	return ffs.fileSystem.ReadFile(name)
}

func (ffs faultingFileSystem) Create(name string) (fsFile, error) {
	if ffs.create != nil {
		return ffs.create(name)
	}
	return ffs.fileSystem.Create(name)
}

func (ffs faultingFileSystem) Rename(oldpath, newpath string) error {
	if ffs.rename != nil {
		return ffs.rename(oldpath, newpath)
	}
	return ffs.fileSystem.Rename(oldpath, newpath)
}

func (ffs faultingFileSystem) Remove(name string) error {
	if ffs.remove != nil {
		return ffs.remove(name)
	}
	return ffs.fileSystem.Remove(name)
}

func (ffs faultingFileSystem) MkdirAll(path string, perm os.FileMode) error {
	if ffs.mkdirAll != nil {
		return ffs.mkdirAll(path, perm)
	}
	return ffs.fileSystem.MkdirAll(path, perm)
}

func (ffs faultingFileSystem) Stat(name string) (os.FileInfo, error) {
	if ffs.stat != nil {
		return ffs.stat(name)
	}
	return ffs.fileSystem.Stat(name)
}

func (ffs faultingFileSystem) ReadDir(name string) ([]os.DirEntry, error) {
	if ffs.readDir != nil {
		return ffs.readDir(name)
	}
	return ffs.fileSystem.ReadDir(name)
}
