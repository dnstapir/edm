package runner

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cockroachdb/pebble"
	dnstap "github.com/dnstap/golang-dnstap"
	"github.com/dnstapir/edm/pkg/protocols"
	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/fsnotify/fsnotify"
	"github.com/hashicorp/golang-lru/v2"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/miekg/dns"
	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/go-hll"
	"github.com/smhanov/dawg"
	"github.com/spaolacci/murmur3"
	"github.com/spf13/viper"
	"go4.org/netipx"
	"google.golang.org/protobuf/proto"
)

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

func testJWK(t testing.TB) jwk.Key {
	t.Helper()

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	key, err := jwk.FromRaw(priv)
	if err != nil {
		t.Fatal(err)
	}
	if err := key.Set(jwk.KeyIDKey, "test-key"); err != nil {
		t.Fatal(err)
	}
	if err := key.Set(jwk.AlgorithmKey, jwa.EdDSA); err != nil {
		t.Fatal(err)
	}
	return key
}

func testJWKFile(t testing.TB) string {
	t.Helper()

	data, err := json.Marshal(testJWK(t))
	if err != nil {
		t.Fatal(err)
	}
	return writeTempFile(t, "key.jwk", data)
}

func testCertFiles(t testing.TB) (string, string, string) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		IsCA:         true,
		DNSNames:     []string{"localhost"},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})

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

func TestCertStore(t *testing.T) {
	store := newCertStore()
	if _, err := store.getClientCertificate(nil); !errors.Is(err, errNoClientCertificate) {
		t.Fatalf("empty getClientCertificate error = %v", err)
	}

	certPath, keyPath, _ := testCertFiles(t)
	if err := store.loadCert(certPath, keyPath); err != nil {
		t.Fatal(err)
	}
	cert, err := store.getClientCertificate(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(cert.Certificate) == 0 {
		t.Fatal("loaded certificate is empty")
	}

	if err := store.loadCert(certPath, keyPath+".missing"); err == nil {
		t.Fatal("loadCert with missing key succeeded")
	}
}

func TestSetLabelsNilAndBoundedReverse(t *testing.T) {
	edm := &dnstapMinimiser{}

	labels := edm.reverseLabelsBounded(nil, 10)
	if labels != nil {
		t.Fatalf("nil labels = %#v", labels)
	}

	dl := &dnsLabels{}
	edm.setLabels(nil, 10, dl)
	if dl.Label0 != nil {
		t.Fatalf("nil labels set Label0 = %q", *dl.Label0)
	}

	got := edm.reverseLabelsBounded([]string{"a", "b", "c"}, 10)
	want := []string{"c", "b", "a"}
	if !slices.Equal(got, want) {
		t.Fatalf("reverseLabelsBounded = %#v, want %#v", got, want)
	}
}

func TestSetCryptopanInvalidCacheSize(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	if err := edm.setCryptopan("key", "salt", -1); err == nil {
		t.Fatal("setCryptopan accepted negative cache size")
	}
}

func TestCertPoolAndJWKFiles(t *testing.T) {
	_, _, caPath := testCertFiles(t)
	pool, err := certPoolFromFile(caPath)
	if err != nil {
		t.Fatal(err)
	}
	if pool.Equal(x509.NewCertPool()) {
		t.Fatal("cert pool has no certificates")
	}

	if _, err := certPoolFromFile(writeTempFile(t, "bad-ca.pem", []byte("not pem"))); err == nil {
		t.Fatal("bad CA file succeeded")
	}
	if _, err := certPoolFromFile(filepath.Join(t.TempDir(), "missing.pem")); err == nil {
		t.Fatal("missing CA file succeeded")
	}

	keyPath := testJWKFile(t)
	key, err := edDsaJWKFromFile(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	if key.Algorithm() != jwa.EdDSA {
		t.Fatalf("algorithm = %v", key.Algorithm())
	}
	if _, err := edDsaJWKFromFile(writeTempFile(t, "bad.jwk", []byte("{"))); err == nil {
		t.Fatal("bad JWK succeeded")
	}
	if _, err := edDsaJWKFromFile(filepath.Join(t.TempDir(), "missing.jwk")); err == nil {
		t.Fatal("missing JWK succeeded")
	}
}

func TestLoadDawgFileErrors(t *testing.T) {
	if _, _, err := loadDawgFile(filepath.Join(t.TempDir(), "missing.dawg")); err == nil {
		t.Fatal("missing DAWG succeeded")
	}
	if _, _, err := loadDawgFile(writeTempFile(t, "empty.dawg", nil)); !errors.Is(err, errEmptyDawgFile) {
		t.Fatalf("empty DAWG error = %v", err)
	}
	recovered := func() (recovered any) {
		defer func() {
			recovered = recover()
		}()
		if _, _, err := loadDawgFile(writeTempFile(t, "invalid.dawg", []byte("bad"))); err != nil {
			t.Fatalf("invalid DAWG returned error instead of panic: %s", err)
		}
		return nil
	}()
	if recovered == nil {
		t.Fatal("invalid DAWG did not panic")
	}

	finder, _, err := loadDawgFile(testDawgFile(t, "example.com."))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := finder.Close(); err != nil {
			t.Fatalf("close loaded DAWG: %s", err)
		}
	})
	if finder.NumAdded() != 1 {
		t.Fatalf("NumAdded = %d", finder.NumAdded())
	}
}

func TestIgnoredFileErrors(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)

	edm.conf.IgnoredClientIPsFile = filepath.Join(t.TempDir(), "missing")
	if err := edm.setIgnoredClientIPs(); err == nil {
		t.Fatal("missing ignored-client file succeeded")
	}

	edm.conf.IgnoredClientIPsFile = writeTempFile(t, "bad-cidr", []byte("not-a-prefix\n"))
	if err := edm.setIgnoredClientIPs(); err == nil {
		t.Fatal("bad CIDR succeeded")
	}

	edm.conf.IgnoredQuestionNamesFile = filepath.Join(t.TempDir(), "missing.dawg")
	if err := edm.setIgnoredQuestionNames(); err == nil {
		t.Fatal("missing ignored-question file succeeded")
	}
}

// TestSetIgnoredQuestionNamesBranches drives the three code paths in
// setIgnoredQuestionNames that the basic missing-file test in
// TestIgnoredFileErrors does not reach.
func TestSetIgnoredQuestionNamesBranches(t *testing.T) {
	t.Run("empty filename closes existing list", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		// Pre-load a finder so the empty-filename branch has something
		// to close on replace.
		edm.conf.IgnoredQuestionNamesFile = testDawgFile(t, "ignore.example.")
		if err := edm.setIgnoredQuestionNames(); err != nil {
			t.Fatalf("initial load: %v", err)
		}
		edm.ignoredQuestionsMutex.RLock()
		if edm.ignoredQuestions == nil {
			edm.ignoredQuestionsMutex.RUnlock()
			t.Fatal("expected finder loaded; got nil")
		}
		edm.ignoredQuestionsMutex.RUnlock()

		// Unset the filename and reload — the close-on-replace branch
		// fires and ignoredQuestions returns to nil.
		edm.conf.IgnoredQuestionNamesFile = ""
		if err := edm.setIgnoredQuestionNames(); err != nil {
			t.Fatalf("unset reload: %v", err)
		}
		edm.ignoredQuestionsMutex.RLock()
		defer edm.ignoredQuestionsMutex.RUnlock()
		if edm.ignoredQuestions != nil {
			t.Fatal("expected finder cleared after unset; got non-nil")
		}
	})

	t.Run("empty-byte dawg file treated as unset", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		// Pre-load to verify the close-on-replace branch inside the
		// errEmptyDawgFile arm fires.
		edm.conf.IgnoredQuestionNamesFile = testDawgFile(t, "ignore.example.")
		if err := edm.setIgnoredQuestionNames(); err != nil {
			t.Fatalf("initial load: %v", err)
		}

		// loadDawgFile returns errEmptyDawgFile for a zero-byte file
		// (dawg.Load would panic), which setIgnoredQuestionNames treats
		// as "unset". Expect nil error and a cleared finder.
		edm.conf.IgnoredQuestionNamesFile = writeTempFile(t, "empty.dawg", nil)
		if err := edm.setIgnoredQuestionNames(); err != nil {
			t.Fatalf("empty file: %v", err)
		}
		edm.ignoredQuestionsMutex.RLock()
		defer edm.ignoredQuestionsMutex.RUnlock()
		if edm.ignoredQuestions != nil {
			t.Fatal("expected finder cleared for empty file; got non-nil")
		}
	})

	t.Run("dawg with zero names clears finder", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		// A dawg file that's non-empty on disk but has NumAdded()==0
		// goes through dawg.Load successfully and then takes the
		// "else: ignoredQuestions = nil" arm.
		edm.conf.IgnoredQuestionNamesFile = testDawgFile(t)
		if err := edm.setIgnoredQuestionNames(); err != nil {
			t.Fatalf("zero-name dawg: %v", err)
		}
		edm.ignoredQuestionsMutex.RLock()
		defer edm.ignoredQuestionsMutex.RUnlock()
		if edm.ignoredQuestions != nil {
			t.Fatal("expected nil finder for zero-name dawg; got non-nil")
		}
	})
}

// TestPseudonymiseIPCacheBranches covers the three pseudonymiseIP cache
// branches that TestIPConversionErrorsAndPseudonymiseInvalid (bad-slice)
// and TestPseudonymiseDnstap (one-shot success) do not reach: cache hit,
// cache eviction at the LRU size limit, and the cache-disabled path
// reached via cryptopanCache == nil.
func TestPseudonymiseIPCacheBranches(t *testing.T) {
	addrA := netip.MustParseAddr("198.51.100.20").AsSlice()
	addrB := netip.MustParseAddr("198.51.100.30").AsSlice()

	t.Run("cache hit on repeat", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		// First call populates the cache, second returns the cached
		// value via cryptopanCache.Get — exercising the cacheHit arm.
		first, err := edm.pseudonymiseIP(addrA)
		if err != nil {
			t.Fatalf("first: %v", err)
		}
		second, err := edm.pseudonymiseIP(addrA)
		if err != nil {
			t.Fatalf("second: %v", err)
		}
		if !bytes.Equal(first, second) {
			t.Fatalf("cache hit produced different result: %v vs %v", first, second)
		}
		// pseudonymiseIP is deterministic, so first==second holds even
		// if caching were silently bypassed. Pin the assertion to the
		// observable side-effect of the hit arm: exactly one entry in
		// the LRU, keyed by addrA.
		if edm.cryptopanCache == nil {
			t.Fatal("cryptopanCache is nil")
		}
		if got := edm.cryptopanCache.Len(); got != 1 {
			t.Fatalf("cache len = %d, want 1", got)
		}
		if !edm.cryptopanCache.Contains(netip.MustParseAddr("198.51.100.20")) {
			t.Fatal("cache does not contain addrA")
		}
	})

	t.Run("cache eviction at size limit", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		// Shrink the LRU to a single entry so the second distinct
		// address evicts the first — exercising the evicted arm and
		// the promCryptopanCacheEvicted.Inc() call.
		if err := edm.setCryptopan("key1", "aabbccddeeffgghh", 1); err != nil {
			t.Fatalf("setCryptopan size 1: %v", err)
		}
		if _, err := edm.pseudonymiseIP(addrA); err != nil {
			t.Fatalf("populate: %v", err)
		}
		if _, err := edm.pseudonymiseIP(addrB); err != nil {
			t.Fatalf("evict: %v", err)
		}
		if edm.cryptopanCache.Len() != 1 {
			t.Fatalf("cache len = %d, want 1 after eviction", edm.cryptopanCache.Len())
		}
	})

	t.Run("cache disabled bypasses cache logic", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		// cacheEntries=0 leaves cryptopanCache nil so the cache-Get
		// and cache-Add branches are skipped entirely.
		if err := edm.setCryptopan("key1", "aabbccddeeffgghh", 0); err != nil {
			t.Fatalf("setCryptopan disabled: %v", err)
		}
		if edm.cryptopanCache != nil {
			t.Fatal("cryptopanCache should be nil with cacheEntries=0")
		}
		if _, err := edm.pseudonymiseIP(addrA); err != nil {
			t.Fatalf("pseudonymiseIP with disabled cache: %v", err)
		}
	})
}

func TestFileAndFilenameHelpers(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	base := t.TempDir()
	start := time.Date(2026, 5, 28, 12, 0, 0, 0, time.FixedZone("test", 2*60*60))
	stop := start.Add(time.Minute)

	tmpName, finalName := buildParquetFilenames(base, "dns_histogram", start, stop)
	if !strings.HasSuffix(tmpName, ".parquet.tmp") || !strings.HasSuffix(finalName, ".parquet") {
		t.Fatalf("unexpected filenames: %q %q", tmpName, finalName)
	}
	if timestampToFileString(start.UTC()) != "2026-05-28T10-00-00Z" {
		t.Fatalf("unexpected timestamp string: %s", timestampToFileString(start.UTC()))
	}
	if got := getStartTimeFromRotationTime(stop); !got.Equal(start) {
		t.Fatalf("start time = %v, want %v", got, start)
	}

	parsedStart, parsedStop, err := timestampsFromFilename(filepath.Base(finalName))
	if err != nil {
		t.Fatal(err)
	}
	if !parsedStart.Equal(start.UTC()) || !parsedStop.Equal(stop.UTC()) {
		t.Fatalf("parsed times = %v %v", parsedStart, parsedStop)
	}
	if _, _, err := timestampsFromFilename("dns_histogram-bad_bad.parquet"); err == nil {
		t.Fatal("bad timestamp filename succeeded")
	}
	if _, _, err := timestampsFromFilename("dns_histogram-2026-05-28T10-00-00Z_bad.parquet"); err == nil {
		t.Fatal("bad stop timestamp filename succeeded")
	}

	out, err := edm.createFile(filepath.Join(base, "missing", "created.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	if err := edm.renameFile(out.Name(), filepath.Join(base, "sent", "created.txt")); err != nil {
		t.Fatal(err)
	}
	if _, err := edm.createFile(base); err == nil {
		t.Fatal("createFile on directory succeeded")
	}
	if err := edm.renameFile(filepath.Join(base, "nope"), filepath.Join(base, "dst")); err == nil {
		t.Fatal("rename missing source succeeded")
	}
}

func TestIPConversionErrorsAndPseudonymiseInvalid(t *testing.T) {
	if _, err := ipBytesToInt([]byte{1, 2, 3}); err == nil {
		t.Fatal("short IPv4 bytes succeeded")
	}
	if _, _, err := ip6BytesToInt([]byte{1, 2, 3}); err == nil {
		t.Fatal("short IPv6 bytes succeeded")
	}

	edm := newTestDnstapMinimiser(t, defaultTC)
	got, err := edm.pseudonymiseIP([]byte{1, 2, 3})
	if err == nil {
		t.Fatal("invalid pseudonymiseIP succeeded")
	}
	if !bytes.Equal(got, []byte{0, 0, 0}) {
		t.Fatalf("invalid pseudonymiseIP returned %v", got)
	}

	dt := &dnstap.Dnstap{Message: &dnstap.Message{QueryAddress: []byte{1, 2, 3}, ResponseAddress: []byte{4, 5, 6}}}
	edm.pseudonymiseDnstap(dt)
	if !bytes.Equal(dt.Message.QueryAddress, []byte{0, 0, 0}) || !bytes.Equal(dt.Message.ResponseAddress, []byte{0, 0, 0}) {
		t.Fatalf("invalid dnstap addresses were not zeroed: %#v", dt.Message)
	}
}

func TestParseHLLStorageTypeErrors(t *testing.T) {
	if _, err := parseHllStorageType(nil); err == nil {
		t.Fatal("empty HLL bytes succeeded")
	}
	if _, err := parseHllStorageType([]byte{0x20}); err == nil {
		t.Fatal("unsupported HLL version succeeded")
	}

	h, err := hll.NewHll(getHllDefaults(10))
	if err != nil {
		t.Fatal(err)
	}
	storageType, err := parseHllStorageType(h.ToBytes())
	if err != nil {
		t.Fatal(err)
	}
	if storageType != hllEmpty {
		t.Fatalf("storage type = %v, want hllEmpty", storageType)
	}
}

func TestNewHistogramDataAndWriteParquet(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)

	exact := edm.newHistogramData(getHllDefaults(0), false)
	if exact.EDMStatusBits != uint64(edmStatusWellKnownExact) {
		t.Fatalf("exact status = %d", exact.EDMStatusBits)
	}
	wildcard := edm.newHistogramData(getHllDefaults(0), true)
	if wildcard.EDMStatusBits != uint64(edmStatusWellKnownWildcard) {
		t.Fatalf("wildcard status = %d", wildcard.EDMStatusBits)
	}

	finder := testDawgFinder(t, "example.com.")
	wkd := &wellKnownDomainsData{
		m:          map[int]*histogramData{0: exact},
		dawgFinder: finder,
	}
	exact.ACount = 1
	exact.v4ClientHLL.AddRaw(murmur3.Sum64(netip.MustParseAddr("198.51.100.20").AsSlice()))

	var buf bytes.Buffer
	if err := edm.writeHistogramParquet(&buf, time.Unix(10, 0), wkd, defaultLabelLimit); err != nil {
		t.Fatal(err)
	}
	rows, err := parquet.Read[histogramData](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ACount != 1 || rows[0].V4ClientCount == 0 {
		t.Fatalf("unexpected rows: %#v", rows)
	}

	badWKD := &wellKnownDomainsData{m: map[int]*histogramData{99: exact}, dawgFinder: finder}
	if err := edm.writeHistogramParquet(io.Discard, time.Time{}, badWKD, defaultLabelLimit); err == nil {
		t.Fatal("writeHistogramParquet with bad DAWG index succeeded")
	}
}

func TestSessionParquetAndSessionConstruction(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	packed := packedDNSMsg(t, "www.example.com.", dns.TypeA, dns.RcodeSuccess)
	dt := testDnstapMessage(t, dnstap.Message_CLIENT_RESPONSE, dnstap.SocketFamily_INET, packed)
	msg, ts := edm.parsePacket(dt, false)
	if msg == nil {
		t.Fatal("parsePacket returned nil msg")
	}
	if !ts.Equal(time.Unix(1_700_000_001, 456).UTC()) {
		t.Fatalf("response timestamp = %v", ts)
	}
	sd := edm.newSession(dt, msg, false, defaultLabelLimit, ts)
	if sd.ResponseTime == nil || sd.ResponseMessage == nil || sd.ServerID == nil {
		t.Fatalf("session missing response fields: %#v", sd)
	}
	if sd.SourceIPv4 == nil || sd.DestIPv4 == nil || sd.DNSProtocol == nil {
		t.Fatalf("session missing network fields: %#v", sd)
	}

	var buf bytes.Buffer
	if err := edm.writeSessionParquet(&buf, &prevSessions{sessions: []*sessionData{sd}}); err != nil {
		t.Fatal(err)
	}
	rows, err := parquet.Read[sessionData](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ServerID == nil || *rows[0].ServerID != "server-1" {
		t.Fatalf("unexpected session rows: %#v", rows)
	}

	queryDT := testDnstapMessage(t, dnstap.Message_CLIENT_QUERY, dnstap.SocketFamily_INET6, packed)
	queryMsg, queryTS := edm.parsePacket(queryDT, true)
	querySession := edm.newSession(queryDT, queryMsg, true, defaultLabelLimit, queryTS)
	if querySession.QueryTime == nil || querySession.QueryMessage == nil || querySession.SourceIPv6Network == nil || querySession.DestIPv6Host == nil {
		t.Fatalf("query session missing fields: %#v", querySession)
	}

	huge := uint64(math.MaxInt64) + 1
	queryDT.Message.QueryTimeSec = &huge
	if _, zeroTS := edm.parsePacket(queryDT, true); !zeroTS.Equal(time.Unix(0, 0).UTC()) {
		t.Fatalf("overflow query timestamp = %v, want Unix zero", zeroTS)
	}
	responseDT := testDnstapMessage(t, dnstap.Message_CLIENT_RESPONSE, dnstap.SocketFamily_INET, packed)
	huge = uint64(math.MaxInt64) + 1
	responseDT.Message.ResponseTimeSec = &huge
	if _, zeroTS := edm.parsePacket(responseDT, false); !zeroTS.Equal(time.Unix(0, 0).UTC()) {
		t.Fatalf("overflow response timestamp = %v, want Unix zero", zeroTS)
	}

	badMsg, _ := edm.parsePacket(&dnstap.Dnstap{Message: &dnstap.Message{QueryMessage: []byte{1}, QueryTimeSec: ptr(uint64(0)), QueryTimeNsec: ptr(uint32(0))}}, true)
	if badMsg != nil {
		t.Fatal("bad query packet returned non-nil message")
	}
}

// TestParsePacketAddressFormattingBranches drives the address-formatting
// arms of parsePacket that the addr+port-present canary in
// TestSessionParquetAndSessionConstruction does not reach: the
// addr-without-port and all-nil fallbacks, plus the response-unpack-error
// path. The port-without-address branch is covered directly by
// TestFormatDnstapEndpoint.
func TestParsePacketAddressFormattingBranches(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	packed := packedDNSMsg(t, "www.example.com.", dns.TypeA, dns.RcodeSuccess)

	t.Run("addr without ports", func(t *testing.T) {
		dt := testDnstapMessage(t, dnstap.Message_CLIENT_RESPONSE, dnstap.SocketFamily_INET, packed)
		dt.Message.QueryPort = nil
		dt.Message.ResponsePort = nil
		if msg, _ := edm.parsePacket(dt, false); msg == nil {
			t.Fatal("parsePacket returned nil msg")
		}
	})

	t.Run("no addresses at all", func(t *testing.T) {
		dt := testDnstapMessage(t, dnstap.Message_CLIENT_RESPONSE, dnstap.SocketFamily_INET, packed)
		dt.Message.QueryAddress = nil
		dt.Message.ResponseAddress = nil
		dt.Message.QueryPort = nil
		dt.Message.ResponsePort = nil
		if msg, _ := edm.parsePacket(dt, false); msg == nil {
			t.Fatal("parsePacket returned nil msg")
		}
	})

	t.Run("response unpack error", func(t *testing.T) {
		dt := &dnstap.Dnstap{
			Message: &dnstap.Message{
				ResponseMessage:  []byte{1, 2, 3},
				ResponseTimeSec:  ptr(uint64(0)),
				ResponseTimeNsec: ptr(uint32(0)),
			},
		}
		badMsg, _ := edm.parsePacket(dt, false)
		if badMsg != nil {
			t.Fatal("bad response packet returned non-nil message")
		}
	})
}

// TestNewSessionBranches covers newSession arms that
// TestSessionParquetAndSessionConstruction (basic INET/INET6 happy paths)
// does not reach: port overflow, ipBytesToInt error from bad address
// bytes, ipBytesToInt error from IPv6 bytes carried on an INET family,
// ip6BytesToInt error from bad address bytes, and the unknown
// SocketFamily default arm.
func TestNewSessionBranches(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	packed := packedDNSMsg(t, "www.example.com.", dns.TypeA, dns.RcodeSuccess)

	t.Run("port overflow zeroes ports", func(t *testing.T) {
		dt := testDnstapMessage(t, dnstap.Message_CLIENT_RESPONSE, dnstap.SocketFamily_INET, packed)
		big := uint32(math.MaxInt32) + 1
		dt.Message.QueryPort = &big
		dt.Message.ResponsePort = &big
		msg, ts := edm.parsePacket(dt, false)
		sd := edm.newSession(dt, msg, false, defaultLabelLimit, ts)
		if sd.SourcePort == nil || *sd.SourcePort != 0 {
			t.Fatalf("SourcePort = %v, want 0", sd.SourcePort)
		}
		if sd.DestPort == nil || *sd.DestPort != 0 {
			t.Fatalf("DestPort = %v, want 0", sd.DestPort)
		}
	})

	t.Run("bad INET address bytes logs but does not panic", func(t *testing.T) {
		dt := testDnstapMessage(t, dnstap.Message_CLIENT_RESPONSE, dnstap.SocketFamily_INET, packed)
		dt.Message.QueryAddress = []byte{1, 2, 3}
		dt.Message.ResponseAddress = []byte{4, 5, 6}
		msg, ts := edm.parsePacket(dt, false)
		sd := edm.newSession(dt, msg, false, defaultLabelLimit, ts)
		if sd.SourceIPv4 != nil {
			t.Fatalf("SourceIPv4 should be nil for bad addr bytes, got %v", *sd.SourceIPv4)
		}
		if sd.DestIPv4 != nil {
			t.Fatalf("DestIPv4 should be nil for bad addr bytes, got %v", *sd.DestIPv4)
		}
	})

	t.Run("mismatched IPv6 address bytes with INET family leaves IPv4 nil", func(t *testing.T) {
		dt := testDnstapMessage(t, dnstap.Message_CLIENT_RESPONSE, dnstap.SocketFamily_INET, packed)
		dt.Message.QueryAddress = netip.MustParseAddr("2001:db8::20").AsSlice()
		dt.Message.ResponseAddress = netip.MustParseAddr("2001:db8::53").AsSlice()
		msg, ts := edm.parsePacket(dt, false)
		sd := edm.newSession(dt, msg, false, defaultLabelLimit, ts)
		if sd.SourceIPv4 != nil {
			t.Fatalf("SourceIPv4 should be nil for IPv6 bytes with INET family, got %d", *sd.SourceIPv4)
		}
		if sd.DestIPv4 != nil {
			t.Fatalf("DestIPv4 should be nil for IPv6 bytes with INET family, got %d", *sd.DestIPv4)
		}
	})

	t.Run("bad INET6 address bytes logs but does not panic", func(t *testing.T) {
		dt := testDnstapMessage(t, dnstap.Message_CLIENT_RESPONSE, dnstap.SocketFamily_INET6, packed)
		dt.Message.QueryAddress = []byte{1, 2, 3}
		dt.Message.ResponseAddress = []byte{4, 5, 6}
		msg, ts := edm.parsePacket(dt, false)
		sd := edm.newSession(dt, msg, false, defaultLabelLimit, ts)
		if sd.SourceIPv6Network != nil {
			t.Fatalf("SourceIPv6Network should be nil for bad addr bytes")
		}
		if sd.DestIPv6Network != nil {
			t.Fatalf("DestIPv6Network should be nil for bad addr bytes")
		}
	})

	t.Run("unknown socket family logs and leaves IPs nil", func(t *testing.T) {
		dt := testDnstapMessage(t, dnstap.Message_CLIENT_RESPONSE, dnstap.SocketFamily_INET, packed)
		unknown := dnstap.SocketFamily(99)
		dt.Message.SocketFamily = &unknown
		msg, ts := edm.parsePacket(dt, false)
		sd := edm.newSession(dt, msg, false, defaultLabelLimit, ts)
		if sd.SourceIPv4 != nil || sd.SourceIPv6Network != nil {
			t.Fatal("expected no IP fields populated for unknown family")
		}
	})

	t.Run("empty identity leaves ServerID nil", func(t *testing.T) {
		dt := testDnstapMessage(t, dnstap.Message_CLIENT_RESPONSE, dnstap.SocketFamily_INET, packed)
		dt.Identity = nil
		msg, ts := edm.parsePacket(dt, false)
		sd := edm.newSession(dt, msg, false, defaultLabelLimit, ts)
		if sd.ServerID != nil {
			t.Fatalf("ServerID should be nil for empty identity, got %q", *sd.ServerID)
		}
	})
}

func TestCreateSessionAndHistogramFiles(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	dataDir := t.TempDir()
	rotationTime := time.Date(2026, 5, 28, 12, 1, 0, 0, time.UTC)
	ps := &prevSessions{
		rotationTime: rotationTime,
		sessions: []*sessionData{{
			dnsLabels: dnsLabels{Label0: ptr("com")},
			ServerID:  ptr("server"),
		}},
	}
	sessionFile, err := edm.createSessionFile(ps, dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if strings.HasSuffix(sessionFile, ".tmp") {
		t.Fatalf("session file kept tmp suffix: %s", sessionFile)
	}

	finder := testDawgFinder(t, "example.com.")
	hd := edm.newHistogramData(getHllDefaults(0), false)
	wkd := &wellKnownDomainsData{rotationTime: rotationTime, dawgFinder: finder, m: map[int]*histogramData{0: hd}}
	histFile, err := edm.createHistogramFile(wkd, defaultLabelLimit, filepath.Join(dataDir, "parquet", "histograms", "outbox"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.HasSuffix(histFile, ".tmp") {
		t.Fatalf("histogram file kept tmp suffix: %s", histFile)
	}
}

func TestQnameSeen(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	db := newTestPebble(t)
	cache, err := lru.New[string, struct{}](1)
	if err != nil {
		t.Fatal(err)
	}

	msg := new(dns.Msg)
	msg.SetQuestion("Example.COM.", dns.TypeA)
	if edm.qnameSeen(msg, cache, db) {
		t.Fatal("first qnameSeen call returned true")
	}
	if !edm.qnameSeen(msg, cache, db) {
		t.Fatal("second qnameSeen call returned false")
	}

	cache, err = lru.New[string, struct{}](1)
	if err != nil {
		t.Fatal(err)
	}
	if !edm.qnameSeen(msg, cache, db) {
		t.Fatal("qnameSeen did not find qname in pebble")
	}

	other := new(dns.Msg)
	other.SetQuestion("other.example.", dns.TypeA)
	_ = edm.qnameSeen(other, cache, db)
}

func TestWellKnownDomainUpdatesAndRotation(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	path := testDawgFile(t, "example.com.")
	finder, modTime, err := loadDawgFile(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = finder.Close() })

	wkd, err := newWellKnownDomainsTracker(finder, modTime)
	if err != nil {
		t.Fatal(err)
	}
	msg := new(dns.Msg)
	msg.SetQuestion("example.com.", dns.TypeMX)
	msg.Rcode = dns.RcodeNameError
	wkd.sendUpdate(netip.MustParseAddr("198.51.100.20").AsSlice(), msg, 0, false, modTime)

	select {
	case wu := <-wkd.updateCh:
		if wu.NXCount != 1 || wu.MXCount != 1 || !wu.ip.IsValid() || wu.hllHash == 0 {
			t.Fatalf("unexpected update: %#v", wu)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for update")
	}

	prev, err := wkd.rotateTracker(edm, path, time.Unix(60, 0))
	if err != nil {
		t.Fatal(err)
	}
	if !prev.rotationTime.Equal(time.Unix(60, 0)) || len(wkd.m) != 0 {
		t.Fatalf("unexpected rotation state: %#v", prev)
	}

	if _, err := wkd.rotateTracker(edm, filepath.Join(t.TempDir(), "missing.dawg"), time.Now()); err == nil {
		t.Fatal("rotateTracker with missing file succeeded")
	}
}

func TestUpdateRetryer(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	finder := testDawgFinder(t, "example.com.")
	wkd, err := newWellKnownDomainsTracker(finder, time.Unix(2, 0))
	if err != nil {
		t.Fatal(err)
	}
	msg := new(dns.Msg)
	msg.SetQuestion("example.com.", dns.TypeA)

	var wg sync.WaitGroup
	wg.Add(1)
	go wkd.updateRetryer(edm, &wg)
	wkd.retryCh <- wkdUpdate{msg: msg, dawgModTime: time.Unix(1, 0), retryLimit: 2}
	close(wkd.retryCh)

	select {
	case wu := <-wkd.updateCh:
		if wu.retry != 1 || wu.dawgIndex != 0 || wu.dawgModTime != time.Unix(2, 0) {
			t.Fatalf("unexpected retried update: %#v", wu)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for retried update")
	}
	wg.Wait()
	<-wkd.retryerDone
}

// TestSendUpdateBranches exercises the rcode/qtype switch arms and the
// invalid-IP-slice fallback in sendUpdate. TestWellKnownDomainUpdatesAndRotation
// already covers the RcodeNameError+TypeMX path; this drives the rest.
func TestSendUpdateBranches(t *testing.T) {
	finder := testDawgFinder(t, "example.com.")
	wkd, err := newWellKnownDomainsTracker(finder, time.Unix(2, 0))
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name    string
		ipBytes []byte
		rcode   int
		qtype   uint16
		qclass  uint16
		check   func(t *testing.T, wu wkdUpdate)
	}{
		{
			name:    "ServerFailure rcode AAAA in",
			ipBytes: netip.MustParseAddr("198.51.100.20").AsSlice(),
			rcode:   dns.RcodeServerFailure,
			qtype:   dns.TypeAAAA,
			qclass:  dns.ClassINET,
			check: func(t *testing.T, wu wkdUpdate) {
				if wu.FailCount != 1 || wu.AAAACount != 1 {
					t.Fatalf("FailCount/AAAACount: %#v", wu)
				}
			},
		},
		{
			name:    "Other rcode NS in",
			ipBytes: netip.MustParseAddr("198.51.100.20").AsSlice(),
			rcode:   dns.RcodeRefused,
			qtype:   dns.TypeNS,
			qclass:  dns.ClassINET,
			check: func(t *testing.T, wu wkdUpdate) {
				if wu.OtherRcodeCount != 1 || wu.NSCount != 1 {
					t.Fatalf("OtherRcode/NSCount: %#v", wu)
				}
			},
		},
		{
			name:    "Success Other-type in",
			ipBytes: netip.MustParseAddr("198.51.100.20").AsSlice(),
			rcode:   dns.RcodeSuccess,
			qtype:   dns.TypeSRV,
			qclass:  dns.ClassINET,
			check: func(t *testing.T, wu wkdUpdate) {
				if wu.OKCount != 1 || wu.OtherTypeCount != 1 {
					t.Fatalf("OK/OtherType: %#v", wu)
				}
			},
		},
		{
			name:    "Non-INET class",
			ipBytes: netip.MustParseAddr("198.51.100.20").AsSlice(),
			rcode:   dns.RcodeSuccess,
			qtype:   dns.TypeA,
			qclass:  dns.ClassCHAOS,
			check: func(t *testing.T, wu wkdUpdate) {
				if wu.NonINCount != 1 {
					t.Fatalf("NonINCount: %#v", wu)
				}
			},
		},
		{
			name:    "Bad IP slice leaves ip invalid",
			ipBytes: []byte{1, 2, 3},
			rcode:   dns.RcodeSuccess,
			qtype:   dns.TypeA,
			qclass:  dns.ClassINET,
			check: func(t *testing.T, wu wkdUpdate) {
				if wu.ip.IsValid() {
					t.Fatalf("expected invalid ip from short slice; got %v", wu.ip)
				}
				if wu.hllHash != 0 {
					t.Fatalf("expected zero hllHash from short slice; got %d", wu.hllHash)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg := new(dns.Msg)
			msg.SetQuestion("example.com.", tc.qtype)
			msg.Question[0].Qclass = tc.qclass
			msg.Rcode = tc.rcode
			wkd.sendUpdate(tc.ipBytes, msg, 0, false, time.Unix(2, 0))
			select {
			case wu := <-wkd.updateCh:
				tc.check(t, wu)
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for update")
			}
		})
	}
}

// TestUpdateRetryerBranches drives the two skip arms of updateRetryer
// that TestUpdateRetryer (which covers the happy resend path) does not
// reach: hitting the retry limit and the dawgNotFound case where the
// reloaded tracker no longer recognises the qname.
func TestUpdateRetryerBranches(t *testing.T) {
	t.Run("retry limit reached drops update", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		finder := testDawgFinder(t, "example.com.")
		wkd, err := newWellKnownDomainsTracker(finder, time.Unix(2, 0))
		if err != nil {
			t.Fatal(err)
		}
		msg := new(dns.Msg)
		msg.SetQuestion("example.com.", dns.TypeA)

		var wg sync.WaitGroup
		wg.Add(1)
		go wkd.updateRetryer(edm, &wg)
		// retry is 1 BEFORE the increment, becomes 2 after — equal to
		// retryLimit, so the skip arm fires and no resend reaches updateCh.
		wkd.retryCh <- wkdUpdate{msg: msg, dawgModTime: time.Unix(1, 0), retry: 1, retryLimit: 2}
		close(wkd.retryCh)
		wg.Wait()
		<-wkd.retryerDone

		select {
		case wu := <-wkd.updateCh:
			t.Fatalf("expected no resend, got %#v", wu)
		default:
		}
	})

	t.Run("dawgNotFound drops update", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		// Tracker only knows example.com; the retry will look up a
		// different qname so wkd.lookup returns dawgNotFound and the
		// retryer drops the update.
		finder := testDawgFinder(t, "example.com.")
		wkd, err := newWellKnownDomainsTracker(finder, time.Unix(2, 0))
		if err != nil {
			t.Fatal(err)
		}
		msg := new(dns.Msg)
		msg.SetQuestion("unknown.example.", dns.TypeA)

		var wg sync.WaitGroup
		wg.Add(1)
		go wkd.updateRetryer(edm, &wg)
		wkd.retryCh <- wkdUpdate{msg: msg, dawgModTime: time.Unix(1, 0), retryLimit: 5}
		close(wkd.retryCh)
		wg.Wait()
		<-wkd.retryerDone

		select {
		case wu := <-wkd.updateCh:
			t.Fatalf("expected no resend on dawgNotFound, got %#v", wu)
		default:
		}
	})
}

// TestQnameSeenLRUEviction verifies the LRU-evicted bookkeeping arm of
// qnameSeen: when the cache is full and a fresh qname is added, the
// previously-cached qname is evicted and promSeenQnameLRUEvicted is
// incremented.
func TestQnameSeenLRUEviction(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	db := newTestPebble(t)
	cache, err := lru.New[string, struct{}](1)
	if err != nil {
		t.Fatal(err)
	}

	first := new(dns.Msg)
	first.SetQuestion("a.example.", dns.TypeA)
	if edm.qnameSeen(first, cache, db) {
		t.Fatal("first qname unexpectedly already-seen")
	}

	second := new(dns.Msg)
	second.SetQuestion("b.example.", dns.TypeA)
	// Adding the second distinct qname evicts the first from the LRU,
	// exercising the evicted/promSeenQnameLRUEvicted.Inc() arm.
	_ = edm.qnameSeen(second, cache, db)
	if cache.Len() != 1 {
		t.Fatalf("cache len = %d, want 1 after eviction", cache.Len())
	}
	if cache.Contains("a.example.") {
		t.Fatal("a.example. should have been evicted")
	}
}

func TestFSWatchersAndEventWatcher(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	dir := t.TempDir()
	watched := filepath.Join(dir, "watched.txt")
	var calls atomic.Int32
	callbackDone := make(chan struct{}, 1)
	edm.fsWatcherFuncs = map[string][]func() error{
		watched: {
			func() error {
				calls.Add(1)
				select {
				case callbackDone <- struct{}{}:
				default:
				}
				return errors.New("logged")
			},
		},
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go edm.fsEventWatcher(&wg)
	edm.fsWatcher.Events <- fsnotifyEvent(watched)
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for watcher callback")
	}
	if calls.Load() != 1 {
		t.Fatalf("watcher callbacks = %d, want 1", calls.Load())
	}
	if err := edm.fsWatcher.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}

func fsnotifyEvent(name string) fsnotify.Event {
	return fsnotify.Event{Name: name, Op: fsnotify.Write}
}

func TestConfigureFSWatchers(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	dir := t.TempDir()
	edm.conf.IgnoredClientIPsFile = filepath.Join(dir, "ignored-ips")
	edm.conf.IgnoredQuestionNamesFile = filepath.Join(dir, "ignored-names")
	edm.conf.HTTPClientCertFile = filepath.Join(dir, "http-cert")
	edm.conf.MQTTClientCertFile = filepath.Join(dir, "mqtt-cert")
	edm.conf.DisableHistogramSender = false
	startConf := edm.conf
	startConf.DisableMQTT = false

	if err := edm.configureFSWatchers(startConf); err != nil {
		t.Fatal(err)
	}
	if len(edm.fsWatcherFuncs) != 4 {
		t.Fatalf("fsWatcherFuncs = %d, want 4", len(edm.fsWatcherFuncs))
	}

	edm.conf.IgnoredClientIPsFile = ""
	edm.conf.IgnoredQuestionNamesFile = ""
	edm.conf.HTTPClientCertFile = ""
	edm.conf.MQTTClientCertFile = ""
	if err := edm.configureFSWatchers(startConf); err != nil {
		t.Fatal(err)
	}
	if len(edm.fsWatcherFuncs) != 0 {
		t.Fatalf("fsWatcherFuncs after cleanup = %d", len(edm.fsWatcherFuncs))
	}
}

func TestAggregateSender(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	payload := []byte("parquet-ish")
	fileName := writeTempFile(t, "hist.parquet", payload)

	var sawRequest bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawRequest = true
		if r.URL.Path != "/api/v1/aggregate/histogram" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/vnd.apache.parquet" {
			t.Fatalf("content type = %q", r.Header.Get("Content-Type"))
		}
		if r.Header.Get("Aggregate-Interval") != "2026-05-28T12:34:56Z/PT2M" {
			t.Fatalf("aggregate interval = %q", r.Header.Get("Aggregate-Interval"))
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(body, payload) {
			t.Fatalf("body = %q", body)
		}
		w.Header().Set("Location", "/uploaded/hist.parquet")
		w.WriteHeader(http.StatusCreated)
	}))
	t.Cleanup(server.Close)

	u, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	as, err := edm.newAggregateSender(u, testJWK(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := as.send(t.Context(), fileName, time.Date(2026, 5, 28, 12, 34, 56, 0, time.UTC), 2*time.Minute); err != nil {
		t.Fatal(err)
	}
	if !sawRequest {
		t.Fatal("server did not receive request")
	}

	if err := as.send(t.Context(), filepath.Join(t.TempDir(), "missing.parquet"), time.Now(), time.Minute); err == nil {
		t.Fatal("sending missing file succeeded")
	}

	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	badKey, err := jwk.FromRaw(rsaKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := edm.newAggregateSender(u, badKey, nil); err == nil {
		t.Fatal("newAggregateSender accepted non-Ed25519 key")
	}
}

func TestAggregateSenderStatusAndLocationErrors(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	fileName := writeTempFile(t, "hist.parquet", []byte("data"))

	statusServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusBadRequest)
	}))
	t.Cleanup(statusServer.Close)
	statusURL, err := url.Parse(statusServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	as, err := edm.newAggregateSender(statusURL, testJWK(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := as.send(t.Context(), fileName, time.Now(), time.Minute); err == nil {
		t.Fatal("unexpected status succeeded")
	}

	locationServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", ":// bad")
		w.WriteHeader(http.StatusCreated)
	}))
	t.Cleanup(locationServer.Close)
	locationURL, err := url.Parse(locationServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	as.aggrecURL = locationURL
	if err := as.send(t.Context(), fileName, time.Now(), time.Minute); err == nil {
		t.Fatal("bad Location succeeded")
	}
}

func TestHistogramSender(t *testing.T) {
	oldInterval := histogramSenderInterval
	oldBackoff := histogramSenderBackoff
	t.Cleanup(func() {
		histogramSenderInterval = oldInterval
		histogramSenderBackoff = oldBackoff
	})
	histogramSenderInterval = time.Millisecond
	histogramSenderBackoff = time.Millisecond

	edm := newTestDnstapMinimiser(t, defaultTC)
	edm.reloadHistogramSenderConfigCh = make(chan struct{}, 1)
	outboxDir := filepath.Join(t.TempDir(), "outbox")
	sentDir := filepath.Join(t.TempDir(), "sent")
	if err := os.MkdirAll(outboxDir, 0o750); err != nil {
		t.Fatal(err)
	}
	name := "dns_histogram-2026-05-28T12-00-00Z_2026-05-28T12-01-00Z.parquet"
	if err := os.WriteFile(filepath.Join(outboxDir, name), []byte("payload"), 0o600); err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "/ok")
		w.WriteHeader(http.StatusCreated)
	}))
	t.Cleanup(server.Close)
	u, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	as, err := edm.newAggregateSender(u, testJWK(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	edm.aggregSender = as

	var wg sync.WaitGroup
	wg.Add(1)
	go edm.histogramSender(outboxDir, sentDir, &wg)
	for range 200 {
		if _, err := os.Stat(filepath.Join(sentDir, name)); err == nil {
			edm.stop()
			wg.Wait()
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	edm.stop()
	wg.Wait()
	t.Fatal("histogramSender did not move sent file")
}

// TestHistogramSenderBranches covers the histogramSender arms that
// TestHistogramSender (the happy send-and-rename path) does not reach:
// disabled-at-startup, parse-error filename, send-error backoff, and
// the reload arm that flips DisableHistogramSender at runtime.
func TestHistogramSenderBranches(t *testing.T) {
	t.Run("disabled at startup skips ticks", func(t *testing.T) {
		oldInterval := histogramSenderInterval
		t.Cleanup(func() { histogramSenderInterval = oldInterval })
		histogramSenderInterval = time.Millisecond

		tc := defaultTC
		tc.DisableHistogramSender = true
		edm := newTestDnstapMinimiser(t, tc)
		edm.reloadHistogramSenderConfigCh = make(chan struct{}, 1)

		buf := &syncBuf{}
		edm.log = slog.New(slog.NewJSONHandler(buf, nil))

		var wg sync.WaitGroup
		wg.Add(1)
		go edm.histogramSender(t.TempDir(), t.TempDir(), &wg)
		// Let several ticks elapse; nothing happens because the
		// DisableHistogramSender guard short-circuits.
		time.Sleep(20 * time.Millisecond)
		edm.stop()
		wg.Wait()

		if !strings.Contains(buf.String(), `"state":"disabled"`) {
			t.Fatalf("expected disabled-state log, got: %q", buf.String())
		}
	})

	t.Run("parse-error filename is logged and skipped", func(t *testing.T) {
		oldInterval := histogramSenderInterval
		t.Cleanup(func() { histogramSenderInterval = oldInterval })
		histogramSenderInterval = time.Millisecond

		edm := newTestDnstapMinimiser(t, defaultTC)
		edm.reloadHistogramSenderConfigCh = make(chan struct{}, 1)
		outboxDir := t.TempDir()
		sentDir := t.TempDir()
		// Filename has the expected prefix/suffix but a malformed
		// timestamp section, so timestampsFromFilename errors out.
		badName := "dns_histogram-not-a-timestamp.parquet"
		if err := os.WriteFile(filepath.Join(outboxDir, badName), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}

		buf := &syncBuf{}
		edm.log = slog.New(slog.NewJSONHandler(buf, nil))

		var wg sync.WaitGroup
		wg.Add(1)
		go edm.histogramSender(outboxDir, sentDir, &wg)
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if strings.Contains(buf.String(), "unable to parse timestamps from histogram filename") {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		edm.stop()
		wg.Wait()

		if !strings.Contains(buf.String(), "unable to parse timestamps from histogram filename") {
			t.Fatalf("expected parse-error log, got: %q", buf.String())
		}
	})

	t.Run("send error triggers backoff log", func(t *testing.T) {
		oldInterval := histogramSenderInterval
		oldBackoff := histogramSenderBackoff
		t.Cleanup(func() {
			histogramSenderInterval = oldInterval
			histogramSenderBackoff = oldBackoff
		})
		histogramSenderInterval = time.Millisecond
		// Keep the backoff short so the test does not wait the real backoff.
		histogramSenderBackoff = time.Millisecond

		edm := newTestDnstapMinimiser(t, defaultTC)
		edm.reloadHistogramSenderConfigCh = make(chan struct{}, 1)
		outboxDir := t.TempDir()
		sentDir := t.TempDir()
		name := "dns_histogram-2026-05-28T12-00-00Z_2026-05-28T12-01-00Z.parquet"
		if err := os.WriteFile(filepath.Join(outboxDir, name), []byte("payload"), 0o600); err != nil {
			t.Fatal(err)
		}

		// Aggregate sender points at an unreachable URL so send fails.
		u, err := url.Parse("http://127.0.0.1:1")
		if err != nil {
			t.Fatal(err)
		}
		as, err := edm.newAggregateSender(u, testJWK(t), nil)
		if err != nil {
			t.Fatal(err)
		}
		edm.aggregSender = as

		buf := &syncBuf{}
		edm.log = slog.New(slog.NewJSONHandler(buf, nil))

		var wg sync.WaitGroup
		wg.Add(1)
		go edm.histogramSender(outboxDir, sentDir, &wg)
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if strings.Contains(buf.String(), "unable to send histogram file") {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		edm.stop()
		wg.Wait()

		if !strings.Contains(buf.String(), "unable to send histogram file") {
			t.Fatalf("expected send-error log, got: %q", buf.String())
		}
	})

	t.Run("backoff interrupted by stop", func(t *testing.T) {
		oldInterval := histogramSenderInterval
		oldBackoff := histogramSenderBackoff
		t.Cleanup(func() {
			histogramSenderInterval = oldInterval
			histogramSenderBackoff = oldBackoff
		})
		histogramSenderInterval = time.Millisecond
		// A long backoff: a non-interruptible wait would block shutdown for the
		// full minute, so exiting promptly proves stop() interrupts the backoff.
		histogramSenderBackoff = time.Minute

		edm := newTestDnstapMinimiser(t, defaultTC)
		edm.reloadHistogramSenderConfigCh = make(chan struct{}, 1)
		outboxDir := t.TempDir()
		sentDir := t.TempDir()
		name := "dns_histogram-2026-05-28T12-00-00Z_2026-05-28T12-01-00Z.parquet"
		if err := os.WriteFile(filepath.Join(outboxDir, name), []byte("payload"), 0o600); err != nil {
			t.Fatal(err)
		}

		// Aggregate sender points at an unreachable URL so the send fails and
		// the sender enters its backoff.
		u, err := url.Parse("http://127.0.0.1:1")
		if err != nil {
			t.Fatal(err)
		}
		as, err := edm.newAggregateSender(u, testJWK(t), nil)
		if err != nil {
			t.Fatal(err)
		}
		edm.aggregSender = as

		buf := &syncBuf{}
		edm.log = slog.New(slog.NewJSONHandler(buf, nil))

		var wg sync.WaitGroup
		wg.Add(1)
		go edm.histogramSender(outboxDir, sentDir, &wg)

		// Wait until the send has failed and the sender is in its backoff.
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if strings.Contains(buf.String(), "unable to send histogram file") {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if !strings.Contains(buf.String(), "unable to send histogram file") {
			t.Fatalf("sender did not reach backoff: %q", buf.String())
		}

		// Cancel during the in-flight one-minute backoff; histogramSender must
		// exit promptly instead of waiting it out.
		edm.stop()
		waitOrFail(t, &wg, 2*time.Second, "histogramSender did not exit when cancelled during backoff")
	})

	t.Run("reload toggles enabled state", func(t *testing.T) {
		oldInterval := histogramSenderInterval
		t.Cleanup(func() { histogramSenderInterval = oldInterval })
		histogramSenderInterval = time.Millisecond

		tc := defaultTC
		tc.DisableHistogramSender = true
		edm := newTestDnstapMinimiser(t, tc)
		edm.reloadHistogramSenderConfigCh = make(chan struct{}, 1)

		buf := &syncBuf{}
		edm.log = slog.New(slog.NewJSONHandler(buf, nil))

		var wg sync.WaitGroup
		wg.Add(1)
		go edm.histogramSender(t.TempDir(), t.TempDir(), &wg)

		// Wait until the worker has read its startup conf before flipping
		// edm.conf — otherwise we race the worker's edm.getConfig() at
		// histogramSender's entry and it may pick up the post-flip value.
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if strings.Contains(buf.String(), `"state":"disabled"`) {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if !strings.Contains(buf.String(), `"state":"disabled"`) {
			t.Fatalf("worker did not log the initial disabled state: %q", buf.String())
		}

		// Flip DisableHistogramSender on edm.conf and signal a reload.
		edm.confMutex.Lock()
		edm.conf.DisableHistogramSender = false
		edm.confMutex.Unlock()
		edm.reloadHistogramSenderConfigCh <- struct{}{}

		deadline = time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if strings.Contains(buf.String(), "enabling histogram sender") {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		edm.stop()
		wg.Wait()

		if !strings.Contains(buf.String(), "enabling histogram sender") {
			t.Fatalf("expected enable log, got: %q", buf.String())
		}
	})
}

func TestMQTTConfigAndPublisher(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	edm.autopahoCtx, edm.autopahoCancel = context.WithCancel(t.Context())
	t.Cleanup(edm.autopahoCancel)

	cfg, err := edm.newAutoPahoClientConfig(nil, "mqtts://example.test:8883", "client-id", 30, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ClientID != "client-id" || cfg.KeepAlive != 30 || cfg.TlsCfg.MinVersion != tls.VersionTLS13 {
		t.Fatalf("unexpected MQTT config: %#v", cfg)
	}
	cfg.OnConnectionUp(nil, nil)
	cfg.OnConnectError(errors.New("connect"))
	cfg.OnClientError(errors.New("client"))
	cfg.OnServerDisconnect(&paho.Disconnect{ReasonCode: 1})
	cfg.OnServerDisconnect(&paho.Disconnect{Properties: &paho.DisconnectProperties{ReasonString: "bye"}})
	if _, err := edm.newAutoPahoClientConfig(nil, "://bad", "client-id", 30, nil); err == nil {
		t.Fatal("bad MQTT URL succeeded")
	}

	jwk := testJWK(t)
	conn := &fakeAutoPahoConnection{}
	edm.startMQTTPipeline(conn, jwk, true, 1)
	edm.mqttPubCh <- []byte(`{"hello":"world"}`)
	close(edm.mqttPubCh)
	edm.autopahoWg.Wait()
	conn.mu.Lock()
	queued := len(conn.queued)
	conn.mu.Unlock()
	if queued != 1 {
		t.Fatalf("queued messages = %d, want 1", queued)
	}

	var buf bytes.Buffer
	pahoDebugLogger{logger: slog.New(slog.NewTextHandler(&buf, nil))}.Printf("hello %s", "debug")
	pahoDebugLogger{logger: slog.New(slog.NewTextHandler(&buf, nil))}.Println("hello", "debug")
	pahoErrorLogger{logger: slog.New(slog.NewTextHandler(&buf, nil))}.Printf("hello %s", "error")
	pahoErrorLogger{logger: slog.New(slog.NewTextHandler(&buf, nil))}.Println("hello", "error")
	if !strings.Contains(buf.String(), "hello") {
		t.Fatalf("logger output = %q", buf.String())
	}
}

func TestMQTTPipelinePublishPath(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	edm.autopahoCtx, edm.autopahoCancel = context.WithCancel(t.Context())
	t.Cleanup(edm.autopahoCancel)
	jwk := testJWK(t)
	conn := &fakeAutoPahoConnection{publishedCh: make(chan struct{}, 1)}

	edm.startMQTTPipeline(conn, jwk, false, 1)
	edm.mqttPubCh <- []byte(`{"publish":"now"}`)
	select {
	case <-conn.publishedCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for publish")
	}
	close(edm.mqttPubCh)
	edm.autopahoWg.Wait()

	conn.mu.Lock()
	published := len(conn.published)
	conn.mu.Unlock()
	if published != 1 {
		t.Fatalf("published messages = %d, want 1", published)
	}
}

func TestMQTTPublishWorkerAwaitError(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	edm.autopahoCtx, edm.autopahoCancel = context.WithCancel(t.Context())
	t.Cleanup(edm.autopahoCancel)
	conn := &fakeAutoPahoConnection{awaitErr: context.Canceled}

	edm.autopahoWg.Add(1)
	go edm.mqttPublishWorker(conn, "events/up/test/new_qname", false)
	waitOrFail(t, &edm.autopahoWg, time.Second, "mqttPublishWorker did not exit after AwaitConnection error")
}

type fakeAutoPahoConnection struct {
	mu          sync.Mutex
	queued      []*autopaho.QueuePublish
	published   []*paho.Publish
	awaitErr    error
	publishedCh chan struct{}
	// publishErr, if non-nil, is returned from Publish so the
	// mqttPublishWorker's error log branch can be exercised.
	publishErr error
	// publishResp, if non-nil, is returned from Publish; otherwise
	// Publish returns &paho.PublishResponse{} (ReasonCode 0).
	publishResp *paho.PublishResponse
}

func (f *fakeAutoPahoConnection) AwaitConnection(context.Context) error {
	return f.awaitErr
}

func (f *fakeAutoPahoConnection) Publish(_ context.Context, p *paho.Publish) (*paho.PublishResponse, error) {
	f.mu.Lock()
	f.published = append(f.published, p)
	f.mu.Unlock()
	if f.publishedCh != nil {
		select {
		case f.publishedCh <- struct{}{}:
		default:
		}
	}
	if f.publishErr != nil {
		return nil, f.publishErr
	}
	if f.publishResp != nil {
		return f.publishResp, nil
	}
	return &paho.PublishResponse{}, nil
}

func (f *fakeAutoPahoConnection) PublishViaQueue(_ context.Context, p *autopaho.QueuePublish) error {
	f.mu.Lock()
	f.queued = append(f.queued, p)
	f.mu.Unlock()
	if f.publishedCh != nil {
		select {
		case f.publishedCh <- struct{}{}:
		default:
		}
	}
	return nil
}

func TestNewQnamePublisher(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	edm.autopahoCtx, edm.autopahoCancel = context.WithCancel(t.Context())
	edm.newQnamePublisherCh = make(chan *protocols.NewQnameJSON, 1)
	edm.mqttPubCh = make(chan []byte, 1)

	var wg sync.WaitGroup
	wg.Add(1)
	go edm.newQnamePublisher(&wg)
	event := protocols.NewQnameJSON{Type: protocols.NewQnameJSONType, Qname: "example.com.", Version: protocols.NewQnameJSONVersion}
	edm.newQnamePublisherCh <- &event
	close(edm.newQnamePublisherCh)
	wg.Wait()

	msg := <-edm.mqttPubCh
	if !strings.Contains(string(msg), "example.com.") {
		t.Fatalf("MQTT payload = %s", msg)
	}
	if _, ok := <-edm.mqttPubCh; ok {
		t.Fatal("mqttPubCh was not closed")
	}
}

func TestSetupHistogramSenderAndCertLoaders(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	certPath, keyPath, caPath := testCertFiles(t)
	httpKeyPath := testJWKFile(t)
	mqttKeyPath := testJWKFile(t)

	edm.conf.HTTPURL = "https://example.test"
	edm.conf.HTTPSigningKeyFile = httpKeyPath
	edm.conf.HTTPCAFile = caPath
	edm.conf.HTTPClientCertFile = certPath
	edm.conf.HTTPClientKeyFile = keyPath
	edm.conf.MQTTClientCertFile = certPath
	edm.conf.MQTTClientKeyFile = keyPath
	if err := edm.loadHTTPClientCert(); err != nil {
		t.Fatal(err)
	}
	if err := edm.loadMQTTClientCert(); err != nil {
		t.Fatal(err)
	}
	if err := edm.setupHistogramSender(); err != nil {
		t.Fatal(err)
	}
	if edm.aggregSender.aggrecURL.String() != "https://example.test" {
		t.Fatalf("aggregate URL = %s", edm.aggregSender.aggrecURL)
	}

	edm.conf.HTTPURL = "://bad"
	if err := edm.setupHistogramSender(); err == nil {
		t.Fatal("bad HTTP URL succeeded")
	}
	edm.conf.HTTPURL = "https://example.test"
	edm.conf.HTTPSigningKeyFile = filepath.Join(t.TempDir(), "missing.jwk")
	if err := edm.setupHistogramSender(); err == nil {
		t.Fatal("missing HTTP signing key succeeded")
	}

	edm.conf.MQTTSigningKeyFile = mqttKeyPath
}

// TestSetupHistogramSenderClosesOldTransport verifies that reloading the
// histogram sender retains the previous aggregate sender's transport and
// closes its idle connections, so reloads do not leak keep-alive connections.
func TestSetupHistogramSenderClosesOldTransport(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)

	connStateCh := make(chan http.ConnState, 16)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "/uploaded/hist.parquet")
		w.WriteHeader(http.StatusCreated)
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		select {
		case connStateCh <- state:
		default:
		}
	}
	server.Start()
	t.Cleanup(server.Close)

	edm.conf.HTTPURL = server.URL
	edm.conf.HTTPSigningKeyFile = testJWKFile(t)

	if err := edm.setupHistogramSender(); err != nil {
		t.Fatal(err)
	}
	oldSender := edm.aggregSender
	if oldSender.httpTransport == nil {
		t.Fatal("httpTransport was not retained on the aggregate sender")
	}

	// Make a request through the old transport so it holds an idle keep-alive
	// connection that the reload is expected to close.
	fileName := writeTempFile(t, "hist.parquet", []byte("payload"))
	if err := oldSender.send(t.Context(), fileName, time.Now(), time.Minute); err != nil {
		t.Fatal(err)
	}
	waitForConnState(t, connStateCh, http.StateIdle)

	// Reloading creates a new sender and must close the old transport's idle
	// connections without touching the live one.
	if err := edm.setupHistogramSender(); err != nil {
		t.Fatal(err)
	}
	if edm.aggregSender.httpTransport == oldSender.httpTransport {
		t.Fatal("reload did not create a new transport")
	}
	waitForConnState(t, connStateCh, http.StateClosed)
}

// waitForConnState blocks until want is observed on ch or the test deadline is
// reached, failing the test if the state never arrives.
func waitForConnState(t testing.TB, ch <-chan http.ConnState, want http.ConnState) {
	t.Helper()

	timeout := time.After(10 * time.Second)
	for {
		select {
		case state := <-ch:
			if state == want {
				return
			}
		case <-timeout:
			t.Fatalf("timed out waiting for connection state %s", want)
		}
	}
}

func TestSetupMQTT(t *testing.T) {
	oldNewAutoPahoConnection := newAutoPahoConnection
	t.Cleanup(func() {
		newAutoPahoConnection = oldNewAutoPahoConnection
	})

	t.Run("success", func(t *testing.T) {
		conn := &fakeAutoPahoConnection{publishedCh: make(chan struct{}, 1)}
		newAutoPahoConnection = func(context.Context, autopaho.ClientConfig) (mqttConnectionManager, error) {
			return conn, nil
		}

		edm := newTestDnstapMinimiser(t, defaultTC)
		edm.conf.DataDir = t.TempDir()
		edm.conf.MQTTSigningKeyFile = testJWKFile(t)
		edm.conf.MQTTServer = "mqtts://example.test:8883"
		edm.conf.MQTTKeepalive = 30
		edm.conf.DisableMQTTFilequeue = false
		edm.conf.MQTTSignWorkers = 0 // exercise the GOMAXPROCS default branch

		if err := edm.setupMQTT(); err != nil {
			t.Fatalf("setupMQTT: %v", err)
		}
		// Drive the publish path so the fake connection manager is actually
		// exercised; otherwise the worker would exit before touching cm.
		edm.mqttPubCh <- []byte(`{"hello":"world"}`)
		select {
		case <-conn.publishedCh:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for publish")
		}
		close(edm.mqttPubCh)
		edm.autopahoWg.Wait()
		if edm.autopahoCancel == nil {
			t.Fatal("autopaho cancel was not set")
		}
		conn.mu.Lock()
		queued := len(conn.queued)
		conn.mu.Unlock()
		if queued != 1 {
			t.Fatalf("queued messages = %d, want 1", queued)
		}
	})

	t.Run("missing signing key", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		edm.conf.DataDir = t.TempDir()
		edm.conf.MQTTSigningKeyFile = filepath.Join(t.TempDir(), "missing.jwk")
		err := edm.setupMQTT()
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("setupMQTT error = %v, want os.ErrNotExist", err)
		}
	})

	t.Run("bad CA file", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		edm.conf.DataDir = t.TempDir()
		edm.conf.MQTTSigningKeyFile = testJWKFile(t)
		edm.conf.MQTTCAFile = writeTempFile(t, "bad-ca.pem", []byte("not a pem"))
		err := edm.setupMQTT()
		if err == nil || !strings.Contains(err.Error(), "CA cert pool") {
			t.Fatalf("setupMQTT error = %v, want CA cert pool failure", err)
		}
	})

	t.Run("queue dir creation failure", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		// Point DataDir below a regular file so MkdirAll fails with ENOTDIR
		// regardless of the uid the tests run as.
		blocker := writeTempFile(t, "blocker", []byte("x"))
		edm.conf.DataDir = filepath.Join(blocker, "datadir")
		edm.conf.MQTTSigningKeyFile = testJWKFile(t)
		edm.conf.DisableMQTTFilequeue = false
		err := edm.setupMQTT()
		if err == nil || !strings.Contains(err.Error(), "queue dir") {
			t.Fatalf("setupMQTT error = %v, want queue dir failure", err)
		}
	})

	t.Run("connection manager failure", func(t *testing.T) {
		oldConn := newAutoPahoConnection
		t.Cleanup(func() { newAutoPahoConnection = oldConn })
		errConnect := errors.New("connect boom")
		newAutoPahoConnection = func(context.Context, autopaho.ClientConfig) (mqttConnectionManager, error) {
			return nil, errConnect
		}
		edm := newTestDnstapMinimiser(t, defaultTC)
		edm.conf.DataDir = t.TempDir()
		edm.conf.MQTTSigningKeyFile = testJWKFile(t)
		edm.conf.MQTTServer = "mqtts://example.test:8883"
		edm.conf.DisableMQTTFilequeue = true
		err := edm.setupMQTT()
		if !errors.Is(err, errConnect) {
			t.Fatalf("setupMQTT error = %v, want %v", err, errConnect)
		}
	})
}

func TestSetupDnstapInput(t *testing.T) {
	discardLog := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("no input configured", func(t *testing.T) {
		_, err := setupDnstapInput(discardLog, config{})
		if !errors.Is(err, errNoInputConfigured) {
			t.Fatalf("err = %v, want errNoInputConfigured", err)
		}
	})

	t.Run("unix happy", func(t *testing.T) {
		dti, err := setupDnstapInput(discardLog, config{
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
		swapSeam(t, &newFrameStreamSockInputFromPath, func(string) (*dnstap.FrameStreamSockInput, error) {
			return nil, errInjected
		})
		_, err := setupDnstapInput(discardLog, config{InputUnix: "/tmp/x"})
		if !errors.Is(err, errInjected) {
			t.Fatalf("err = %v, want errInjected", err)
		}
	})

	t.Run("tcp happy", func(t *testing.T) {
		dti, err := setupDnstapInput(discardLog, config{InputTCP: "127.0.0.1:0"})
		if err != nil {
			t.Fatalf("setupDnstapInput: %v", err)
		}
		if dti == nil {
			t.Fatal("dti is nil on success")
		}
	})

	t.Run("tcp listen error", func(t *testing.T) {
		swapSeam(t, &listenNet, func(string, string) (net.Listener, error) {
			return nil, errInjected
		})
		_, err := setupDnstapInput(discardLog, config{InputTCP: "127.0.0.1:0"})
		if !errors.Is(err, errInjected) {
			t.Fatalf("err = %v, want errInjected", err)
		}
	})

	t.Run("tls happy", func(t *testing.T) {
		certPath, keyPath, _ := testCertFiles(t)
		dti, err := setupDnstapInput(discardLog, config{
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
		certPath, keyPath, caPath := testCertFiles(t)
		dti, err := setupDnstapInput(discardLog, config{
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
		_, err := setupDnstapInput(discardLog, config{
			InputTLS:         "127.0.0.1:0",
			InputTLSCertFile: filepath.Join(t.TempDir(), "missing.crt"),
			InputTLSKeyFile:  filepath.Join(t.TempDir(), "missing.key"),
		})
		if err == nil || !strings.Contains(err.Error(), "x509 dnstap listener cert") {
			t.Fatalf("err = %v, want x509 cert load failure", err)
		}
	})

	t.Run("tls bad client CA file", func(t *testing.T) {
		certPath, keyPath, _ := testCertFiles(t)
		badCA := writeTempFile(t, "bad-ca.pem", []byte("not a pem"))
		_, err := setupDnstapInput(discardLog, config{
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
		certPath, keyPath, _ := testCertFiles(t)
		swapSeam(t, &listenTLS, func(string, string, *tls.Config) (net.Listener, error) {
			return nil, errInjected
		})
		_, err := setupDnstapInput(discardLog, config{
			InputTLS:         "127.0.0.1:0",
			InputTLSCertFile: certPath,
			InputTLSKeyFile:  keyPath,
		})
		if !errors.Is(err, errInjected) {
			t.Fatalf("err = %v, want errInjected", err)
		}
	})
}

type sequenceConfiger struct {
	configs []config
	index   int
	err     error
}

func (sc *sequenceConfiger) getConfig() (config, error) {
	if sc.err != nil {
		return config{}, sc.err
	}
	if sc.index >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1], nil
	}
	conf := sc.configs[sc.index]
	sc.index++
	return conf, nil
}

func TestConfigUpdater(t *testing.T) {
	oldDebounce := configUpdateDebounce
	t.Cleanup(func() {
		configUpdateDebounce = oldDebounce
	})
	configUpdateDebounce = 10 * time.Millisecond

	edm := newTestDnstapMinimiser(t, defaultTC)
	startConf := edm.getConfig()
	nextConf := startConf
	nextConf.CryptopanKey = "key2"
	nextConf.DisableHistogramSender = true
	nextConf.IgnoredClientIPsFile = ""
	nextConf.IgnoredQuestionNamesFile = ""
	sc := &sequenceConfiger{configs: []config{nextConf}}
	edm.configer = sc
	edm.reloadMinimiserConfigCh = []chan struct{}{make(chan struct{}, 1)}
	edm.reloadHistogramSenderConfigCh = make(chan struct{}, 1)

	events := make(chan fsnotify.Event)
	done := make(chan struct{})
	go func() {
		defer close(done)
		configUpdater(events, edm)
	}()
	events <- fsnotify.Event{Name: "config.toml", Op: fsnotify.Write}
	for range 100 {
		if edm.getConfig().CryptopanKey == "key2" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if edm.getConfig().CryptopanKey != "key2" {
		t.Fatalf("config was not updated: %#v", edm.getConfig())
	}
	select {
	case <-edm.reloadMinimiserConfigCh[0]:
	case <-time.After(time.Second):
		t.Fatal("minimiser reload notification not queued")
	}
	select {
	case <-edm.reloadHistogramSenderConfigCh:
	case <-time.After(time.Second):
		t.Fatal("histogram reload notification not queued")
	}
	close(events)
	<-done
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

// runConfigUpdaterUntil drives a single fsnotify event through configUpdater
// and waits for the debounce timer + processing to apply nextConf, then
// shuts the goroutine down. log is wired to a syncBuf so subtests can
// assert on the reload paths that have no other observable side-effect
// without racing the worker on the log write.
func runConfigUpdaterUntil(t *testing.T, edm *dnstapMinimiser, sc *sequenceConfiger, expect func() bool) {
	t.Helper()
	oldDebounce := configUpdateDebounce
	t.Cleanup(func() { configUpdateDebounce = oldDebounce })
	configUpdateDebounce = 5 * time.Millisecond

	edm.configer = sc
	edm.reloadMinimiserConfigCh = []chan struct{}{make(chan struct{}, 1)}
	edm.reloadHistogramSenderConfigCh = make(chan struct{}, 1)

	events := make(chan fsnotify.Event)
	done := make(chan struct{})
	go func() {
		defer close(done)
		configUpdater(events, edm)
	}()
	events <- fsnotify.Event{Name: "config.toml", Op: fsnotify.Write}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if expect() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(events)
	<-done
	if !expect() {
		t.Fatal("configUpdater did not reach the expected state")
	}
}

// TestConfigUpdaterBranches covers reload arms that TestConfigUpdater
// (cryptopan key + disable-histogram-sender + ignored-files clear) does
// not reach.
func TestConfigUpdaterBranches(t *testing.T) {
	t.Run("non-reload-tagged field warns", func(t *testing.T) {
		buf := &syncBuf{}
		edm := newTestDnstapMinimiser(t, defaultTC)
		edm.log = slog.New(slog.NewJSONHandler(buf, nil))

		startConf := edm.getConfig()
		next := startConf
		// DataDir has no reload:"true" tag, so changing it triggers the
		// "requires restart" warning.
		next.DataDir = "/tmp/edm-changed"
		runConfigUpdaterUntil(t, edm, &sequenceConfiger{configs: []config{next}}, func() bool {
			return strings.Contains(buf.String(), "requires restart")
		})
	})

	t.Run("HTTP cert path change reloads cert", func(t *testing.T) {
		buf := &syncBuf{}
		edm := newTestDnstapMinimiser(t, defaultTC)
		edm.log = slog.New(slog.NewJSONHandler(buf, nil))

		// Start with the histogram sender enabled and a valid cert so
		// the late-init branch does not also fire and obscure the
		// cert-change assertion.
		certPath, keyPath, _ := testCertFiles(t)
		startConf := edm.getConfig()
		startConf.DisableHistogramSender = false
		startConf.HTTPClientCertFile = certPath
		startConf.HTTPClientKeyFile = keyPath
		edm.conf = startConf

		next := startConf
		next.HTTPClientCertFile = filepath.Join(t.TempDir(), "missing.crt")
		runConfigUpdaterUntil(t, edm, &sequenceConfiger{configs: []config{next}}, func() bool {
			return strings.Contains(buf.String(), "loadHTTPClientCert")
		})
	})

	t.Run("MQTT cert path change reloads cert", func(t *testing.T) {
		buf := &syncBuf{}
		edm := newTestDnstapMinimiser(t, defaultTC)
		edm.log = slog.New(slog.NewJSONHandler(buf, nil))

		certPath, keyPath, _ := testCertFiles(t)
		startConf := edm.getConfig()
		startConf.DisableMQTT = false
		startConf.MQTTClientCertFile = certPath
		startConf.MQTTClientKeyFile = keyPath
		edm.conf = startConf

		next := startConf
		next.MQTTClientCertFile = filepath.Join(t.TempDir(), "missing.crt")
		runConfigUpdaterUntil(t, edm, &sequenceConfiger{configs: []config{next}}, func() bool {
			return strings.Contains(buf.String(), "loadMQTTClientCert")
		})
	})
}

// TestDiskCleanerOsReadDirError covers the non-ENOENT osReadDir error
// branch: TestMonitorAndDiskCleaner exercises the success path and the
// ENOENT-skip arm; here we inject a generic error and assert it is
// logged as "unable to read sent dir".
func TestDiskCleanerOsReadDirError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		swapSeam(t, &osReadDir, func(string) ([]os.DirEntry, error) { return nil, errInjected })

		buf := &syncBuf{}
		ctx, cancel := context.WithCancel(t.Context())
		edm := &dnstapMinimiser{
			ctx:  ctx,
			stop: cancel,
			log:  slog.New(slog.NewJSONHandler(buf, nil)),
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go edm.diskCleaner(&wg, t.TempDir())
		// Advance just past the diskCleanerInterval so a tick fires.
		time.Sleep(diskCleanerInterval + time.Second)
		cancel()
		wg.Wait()

		if !strings.Contains(buf.String(), "unable to read sent dir") {
			t.Fatalf("expected read-dir error log, got: %q", buf.String())
		}
	})
}

// TestAddFSWatchersErrorOnBadPath covers the addFSWatchers error branch:
// asking fsnotify to watch a non-existent directory fails with
// ENOENT, which addFSWatchers wraps and returns.
func TestAddFSWatchersErrorOnBadPath(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	bogus := filepath.Join(t.TempDir(), "missing-dir", "watched")
	err := edm.addFSWatchers(map[string][]func() error{bogus: {func() error { return nil }}})
	if err == nil || !strings.Contains(err.Error(), "addFSWatchers") {
		t.Fatalf("err = %v, want addFSWatchers wrap", err)
	}
}

func TestMonitorAndDiskCleaner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		edm := &dnstapMinimiser{
			ctx:                    ctx,
			stop:                   cancel,
			log:                    slog.New(slog.NewTextHandler(io.Discard, nil)),
			promNewQnameChannelLen: prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_gauge"}),
			newQnamePublisherCh:    make(chan *protocols.NewQnameJSON, 3),
		}
		edm.newQnamePublisherCh = make(chan *protocols.NewQnameJSON, 3)
		edm.newQnamePublisherCh <- &protocols.NewQnameJSON{}

		var wg sync.WaitGroup
		wg.Add(1)
		go edm.monitorChannelLen(&wg)
		time.Sleep(time.Second)
		edm.stop()
		wg.Wait()

		sentDir := t.TempDir()
		oldFile := filepath.Join(sentDir, "dns_histogram-2026-05-28T12-00-00Z_2026-05-28T12-01-00Z.parquet")
		if err := os.WriteFile(oldFile, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		oldTime := time.Now().Add(-13 * time.Hour)
		if err := os.Chtimes(oldFile, oldTime, oldTime); err != nil {
			t.Fatal(err)
		}
		edm.ctx, edm.stop = context.WithCancel(t.Context())
		wg.Add(1)
		go edm.diskCleaner(&wg, sentDir)
		time.Sleep(time.Minute)
		edm.stop()
		wg.Wait()
		if _, err := os.Stat(oldFile); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("old file still exists: %v", err)
		}
	})
}

func TestDataCollector(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tc := defaultTC
		edm := &dnstapMinimiser{
			conf:               config{HistogramHLLExplicitThreshold: tc.CryptopanAddressEntries},
			log:                slog.New(slog.NewTextHandler(io.Discard, nil)),
			sessionCollectorCh: make(chan *sessionData, 1),
			sessionWriterCh:    make(chan *prevSessions, 1),
			histogramWriterCh:  make(chan *wellKnownDomainsData, 1),
		}

		path := testDawgFile(t, "example.com.")
		finder, modTime, err := loadDawgFile(path)
		if err != nil {
			t.Fatal(err)
		}
		wkd, err := newWellKnownDomainsTracker(finder, modTime)
		if err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go edm.dataCollector(&wg, wkd, path)

		edm.sessionCollectorCh <- &sessionData{ServerID: ptr("server")}
		wkd.updateCh <- wkdUpdate{
			histogramData: histogramData{ACount: 1, OKCount: 1},
			dawgIndex:     0,
			dawgModTime:   modTime,
			ip:            netip.MustParseAddr("198.51.100.20"),
			hllHash:       1,
		}
		time.Sleep(timeUntilNextMinute())
		close(wkd.stop)
		wg.Wait()

		if _, ok := <-edm.sessionWriterCh; !ok {
			t.Fatal("sessionWriterCh closed before queued session could be read")
		}
		if _, ok := <-edm.histogramWriterCh; !ok {
			t.Fatal("histogramWriterCh closed before queued histogram could be read")
		}
	})
}

func TestRunMinimiserFlows(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	edm.reloadMinimiserConfigCh = []chan struct{}{make(chan struct{}, 1)}
	edm.newQnamePublisherCh = make(chan *protocols.NewQnameJSON, 1)
	edm.sessionCollectorCh = make(chan *sessionData, 1)
	cache, err := lru.New[string, struct{}](2)
	if err != nil {
		t.Fatal(err)
	}
	db := newTestPebble(t)
	finder := testDawgFinder(t, "known.example.")
	wkd, err := newWellKnownDomainsTracker(finder, time.Time{})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go edm.runMinimiser(0, &wg, cache, db, nil, defaultLabelLimit, wkd)

	queryFrame := marshaledDnstap(t, testDnstapMessage(t, dnstap.Message_CLIENT_QUERY, dnstap.SocketFamily_INET, packedDNSMsg(t, "query.example.", dns.TypeA, dns.RcodeSuccess)))
	edm.inputChannel <- queryFrame

	knownFrame := marshaledDnstap(t, testDnstapMessage(t, dnstap.Message_CLIENT_RESPONSE, dnstap.SocketFamily_INET, packedDNSMsg(t, "known.example.", dns.TypeA, dns.RcodeSuccess)))
	edm.inputChannel <- knownFrame
	select {
	case <-wkd.updateCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for WKD update")
	}

	newFrame := marshaledDnstap(t, testDnstapMessage(t, dnstap.Message_CLIENT_RESPONSE, dnstap.SocketFamily_INET, packedDNSMsg(t, "new.example.", dns.TypeA, dns.RcodeSuccess)))
	edm.inputChannel <- newFrame
	select {
	case ev := <-edm.newQnamePublisherCh:
		if ev.Qname != "new.example." {
			t.Fatalf("new qname = %s", ev.Qname)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for new_qname event")
	}

	select {
	case sd := <-edm.sessionCollectorCh:
		if sd.ResponseTime == nil {
			t.Fatalf("session missing response time: %#v", sd)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for session")
	}

	edm.conf.DisableSessionFiles = true
	// Signal a config reload, then send it a second time. The reload channel
	// has capacity 1, so the second send blocks until runMinimiser has
	// received (and therefore applied) the first, guaranteeing the
	// disabled-session config is in effect before we feed the next frame.
	// Without this, runMinimiser's select could pick the buffered input frame
	// before the buffered reload and emit a session with the stale config.
	edm.reloadMinimiserConfigCh[0] <- struct{}{}
	edm.reloadMinimiserConfigCh[0] <- struct{}{}
	edm.inputChannel <- newFrame
	time.Sleep(20 * time.Millisecond)
	select {
	case sd := <-edm.sessionCollectorCh:
		t.Fatalf("unexpected session while disabled: %#v", sd)
	default:
	}

	edm.stop()
	wg.Wait()
}

// TestRunMinimiserScratchClientIP verifies that the per-worker scratch buffer
// used to keep the unpseudonymised client IP yields the correct raw address
// for every frame, even when a single worker processes consecutive frames of
// different address families. It feeds an IPv4 frame followed by an IPv6 frame
// (which fills the scratch buffer completely) and checks that each emitted
// wkdUpdate carries the original client IP and its matching HLL hash, proving
// the IP is captured before pseudonymisation and that reusing the scratch
// buffer does not corrupt earlier or later frames.
func TestRunMinimiserScratchClientIP(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	edm.reloadMinimiserConfigCh = []chan struct{}{make(chan struct{}, 1)}
	edm.newQnamePublisherCh = make(chan *protocols.NewQnameJSON, 1)
	edm.sessionCollectorCh = make(chan *sessionData, 1)
	cache, err := lru.New[string, struct{}](2)
	if err != nil {
		t.Fatal(err)
	}
	db := newTestPebble(t)
	wkd, err := newWellKnownDomainsTracker(testDawgFinder(t, "known.example."), time.Time{})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go edm.runMinimiser(0, &wg, cache, db, nil, defaultLabelLimit, wkd)

	tests := []struct {
		family dnstap.SocketFamily
		ip     netip.Addr
	}{
		{dnstap.SocketFamily_INET, netip.MustParseAddr("198.51.100.20")},
		{dnstap.SocketFamily_INET6, netip.MustParseAddr("2001:db8::20")},
	}
	for _, tc := range tests {
		frame := marshaledDnstap(t, testDnstapMessage(t, dnstap.Message_CLIENT_RESPONSE, tc.family, packedDNSMsg(t, "known.example.", dns.TypeA, dns.RcodeSuccess)))
		edm.inputChannel <- frame
		select {
		case wu := <-wkd.updateCh:
			if wu.ip != tc.ip {
				t.Fatalf("client IP for %s: got %s, want %s", tc.family, wu.ip, tc.ip)
			}
			wantHash := murmur3.Sum64(tc.ip.AsSlice())
			if wu.hllHash != wantHash {
				t.Fatalf("HLL hash for %s: got %d, want %d", tc.family, wu.hllHash, wantHash)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for WKD update for %s", tc.family)
		}
	}

	edm.stop()
	wg.Wait()
}

func TestRunMinimiserParseAndIgnoreFlows(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	edm.reloadMinimiserConfigCh = []chan struct{}{make(chan struct{}, 1)}
	edm.newQnamePublisherCh = make(chan *protocols.NewQnameJSON)
	edm.sessionCollectorCh = make(chan *sessionData)
	cache, err := lru.New[string, struct{}](2)
	if err != nil {
		t.Fatal(err)
	}
	db := newTestPebble(t)
	wkd, err := newWellKnownDomainsTracker(testDawgFinder(t, "known.example."), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	var builder netipx.IPSetBuilder
	builder.AddPrefix(netip.MustParsePrefix("198.51.100.20/32"))
	edm.ignoredClientsIPSet, err = builder.IPSet()
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go edm.runMinimiser(0, &wg, cache, db, nil, defaultLabelLimit, wkd)
	edm.inputChannel <- []byte("not protobuf")
	wg.Wait()

	edm = newTestDnstapMinimiser(t, defaultTC)
	edm.reloadMinimiserConfigCh = []chan struct{}{make(chan struct{}, 1)}
	edm.newQnamePublisherCh = make(chan *protocols.NewQnameJSON)
	edm.sessionCollectorCh = make(chan *sessionData)
	edm.ignoredClientsIPSet, err = builder.IPSet()
	if err != nil {
		t.Fatal(err)
	}
	wg.Add(1)
	go edm.runMinimiser(0, &wg, cache, db, nil, defaultLabelLimit, wkd)
	edm.inputChannel <- marshaledDnstap(t, testDnstapMessage(t, dnstap.Message_CLIENT_RESPONSE, dnstap.SocketFamily_INET, packedDNSMsg(t, "ignored.example.", dns.TypeA, dns.RcodeSuccess)))
	time.Sleep(20 * time.Millisecond)
	edm.stop()
	wg.Wait()
}

func TestRunWithDisabledSenders(t *testing.T) {
	oldNotifyContext := notifyContext
	oldListenAndServeHTTP := listenAndServeHTTP
	t.Cleanup(func() {
		notifyContext = oldNotifyContext
		listenAndServeHTTP = oldListenAndServeHTTP
		viper.Reset()
	})

	ctx, cancel := context.WithCancel(t.Context())
	notifyContext = func(context.Context, ...os.Signal) (context.Context, context.CancelFunc) {
		return ctx, cancel
	}
	listenAndServeHTTP = func(*http.Server) error {
		return http.ErrServerClosed
	}

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
	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(logger, level)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not exit")
	}
}
