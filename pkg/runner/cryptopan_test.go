package runner

import (
	"errors"
	"net/netip"
	"testing"

	"github.com/dnstapir/edm/pkg/dnstap"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/twmb/murmur3"
	"github.com/yawning/cryptopan"
)

func TestPseudonymiseDnstap(t *testing.T) {
	// The original addresses we want to pseudonymise
	origQueryAddr4 := netip.MustParseAddr("198.51.100.20")
	origRespAddr4 := netip.MustParseAddr("198.51.100.30")
	origQueryAddr6 := netip.MustParseAddr("2001:db8:1122:3344:5566:7788:99aa:bbcc")
	origRespAddr6 := netip.MustParseAddr("2001:db8:1122:3344:5566:7788:99aa:ddee")

	// The expected result given our first and second keys
	expectedPseudoQueryAddr4 := netip.MustParseAddr("58.92.11.53")
	expectedPseudoRespAddr4 := netip.MustParseAddr("58.92.11.62")
	expectedPseudoQueryAddrUpdated4 := netip.MustParseAddr("185.204.164.235")
	expectedPseudoRespAddrUpdated4 := netip.MustParseAddr("185.204.164.225")

	expectedPseudoQueryAddr6 := netip.MustParseAddr("b780:8dc8:6ed9:cbc5:4d61:a6bb:6255:5a03")
	expectedPseudoRespAddr6 := netip.MustParseAddr("b780:8dc8:6ed9:cbc5:4d61:a6bb:6255:262d")
	expectedPseudoQueryAddrUpdated6 := netip.MustParseAddr("3f29:478:21d2:2c44:6915:7ca7:8654:aa28")
	expectedPseudoRespAddrUpdated6 := netip.MustParseAddr("3f29:478:21d2:2c44:6915:7ca7:8654:d21f")

	dt4 := &dnstap.Message{
		Flags:        dnstap.VALID_QUERY_ADDR | dnstap.VALID_RESPONSE_ADDR,
		QueryAddr:    origQueryAddr4,
		ResponseAddr: origRespAddr4,
	}
	dt6 := &dnstap.Message{
		Flags:        dnstap.VALID_QUERY_ADDR | dnstap.VALID_RESPONSE_ADDR,
		QueryAddr:    origQueryAddr6,
		ResponseAddr: origRespAddr6,
	}

	edm := newRealCryptopanTestDnstapMinimiser(t, defaultTC)

	if edm.testCryptopanCache() != nil {
		if edm.testCryptopanCache().Len() != 0 {
			t.Fatalf("there should be no entries in newly initialised cryptopan cache but it contains items: %d", edm.testCryptopanCache().Len())
		}
	}

	edm.testPseudonymiseDnstap(dt4)
	edm.testPseudonymiseDnstap(dt6)

	// Verify we are not accidentally getting IPv4-mapped IPv6 address
	if !dt4.QueryAddr.Is4() {
		t.Fatalf("pseudonymised IPv4 query address appears to be IPv4-mapped IPv6 address: %s", dt4.QueryAddr)
	}
	if !dt4.ResponseAddr.Is4() {
		t.Fatalf("pseudonymised IPv4 response address appears to be IPv4-mapped IPv6 address: %s", dt4.ResponseAddr)
	}

	// Verify they are different from the original addresses
	if origQueryAddr4 == dt4.QueryAddr {
		t.Fatalf("pseudonymised IPv4 query address %s is the same as the orignal address %s", dt4.QueryAddr, origQueryAddr4)
	}
	if origRespAddr4 == dt4.ResponseAddr {
		t.Fatalf("pseudonymised IPv4 response address %s is the same as the orignal address %s", dt4.ResponseAddr, origRespAddr4)
	}
	if origQueryAddr6 == dt6.QueryAddr {
		t.Fatalf("pseudonymised IPv6 query address %s is the same as the orignal address %s", dt6.QueryAddr, origQueryAddr6)
	}
	if origRespAddr6 == dt6.ResponseAddr {
		t.Fatalf("pseudonymised IPv6 response address %s is the same as the orignal address %s", dt6.ResponseAddr, origRespAddr6)
	}

	// Verify they are different as expected
	if dt4.QueryAddr != expectedPseudoQueryAddr4 {
		t.Fatalf("pseudonymised IPv4 query address %s is not the expected address %s", dt4.QueryAddr, expectedPseudoQueryAddr4)
	}
	if dt4.ResponseAddr != expectedPseudoRespAddr4 {
		t.Fatalf("pseudonymised IPv4 resp address %s is not the expected address %s", dt4.ResponseAddr, expectedPseudoRespAddr4)
	}
	if dt6.QueryAddr != expectedPseudoQueryAddr6 {
		t.Fatalf("pseudonymised IPv6 query address %s is not the expected address %s", dt6.QueryAddr, expectedPseudoQueryAddr6)
	}
	if dt6.ResponseAddr != expectedPseudoRespAddr6 {
		t.Fatalf("pseudonymised IPv6 resp address %s is not the expected address %s", dt6.ResponseAddr, expectedPseudoRespAddr6)
	}

	if edm.testCryptopanCache() != nil {
		if edm.testCryptopanCache().Len() == 0 {
			t.Fatalf("there should be entries in the cryptopan cache but it is empty")
		}

		// Verify the entry in the cache is the same as the one we got back
		cachedPseudoQueryAddr4, ok := edm.testCryptopanCache().Get(origQueryAddr4)
		if !ok {
			t.Fatalf("unable to lookup IPv4 query address %s in cache", origQueryAddr4)
		}
		if cachedPseudoQueryAddr4 != dt4.QueryAddr {
			t.Fatalf("cached pseudonymised IPv4 query address %s is not the same as the calculated address %s", cachedPseudoQueryAddr4, dt4.QueryAddr)
		}

		cachedPseudoRespAddr4, ok := edm.testCryptopanCache().Get(origRespAddr4)
		if !ok {
			t.Fatalf("unable to lookup IPv4 response address %s in cache", origRespAddr4)
		}
		if cachedPseudoRespAddr4 != dt4.ResponseAddr {
			t.Fatalf("cached pseudonymised IPv4 response address %s is not the same as the calculated address %s", cachedPseudoRespAddr4, dt4.ResponseAddr)
		}

		cachedPseudoQueryAddr6, ok := edm.testCryptopanCache().Get(origQueryAddr6)
		if !ok {
			t.Fatalf("unable to lookup IPv6 query address %s in cache", origQueryAddr6)
		}
		if cachedPseudoQueryAddr6 != dt6.QueryAddr {
			t.Fatalf("cached pseudonymised IPv6 query address %s is not the same as the calculated address %s", cachedPseudoQueryAddr6, dt6.QueryAddr)
		}

		cachedPseudoRespAddr6, ok := edm.testCryptopanCache().Get(origRespAddr6)
		if !ok {
			t.Fatalf("unable to lookup IPv6 response address %s in cache", origRespAddr6)
		}
		if cachedPseudoRespAddr6 != dt6.ResponseAddr {
			t.Fatalf("cached pseudonymised IPv6 response address %s is not the same as the calculated address %s", cachedPseudoRespAddr6, dt6.ResponseAddr)
		}
	}

	if edm.testCryptopanCache() != nil {
		t.Logf("number of pseudonymisation cache entries before reset: %d", edm.testCryptopanCache().Len())
	}

	if edm.testCryptopanCache() != nil {
		for _, key := range edm.testCryptopanCache().Keys() {
			value, ok := edm.testCryptopanCache().Get(key)
			if !ok {
				t.Fatalf("unable to extract value for key before reset: %s", key)
			}

			t.Logf("inital cache key: %s, value: %s", key, value)
		}
	}

	// Replace the cryptopan instance and verify we now get different pseudonymised results
	err := edm.setCryptopan("key2", defaultTC.CryptopanKeySalt, defaultTC.CryptopanAddressEntries)
	if err != nil {
		t.Fatalf("unable to call edm.SetCryptopan: %s", err)
	}

	// Mirror the per-worker cache purge that runMinimiser would do on
	// detecting a cryptopan generation change.
	edm.testResetCryptopanCache()

	if edm.testCryptopanCache() != nil {
		if edm.testCryptopanCache().Len() != 0 {
			t.Fatalf("there should be no cache entries in replaced cryptopan cache but it contains items: %d", edm.testCryptopanCache().Len())
		}
	}

	// store current pseudonymised addresses
	pseudoQueryAddr4 := dt4.QueryAddr
	pseudoRespAddr4 := dt4.ResponseAddr
	pseudoQueryAddr6 := dt6.QueryAddr
	pseudoRespAddr6 := dt6.ResponseAddr

	// Reset the addresses and pseudonymise again with the updated key
	dt4.QueryAddr = origQueryAddr4
	dt4.ResponseAddr = origRespAddr4
	dt6.QueryAddr = origQueryAddr6
	dt6.ResponseAddr = origRespAddr6

	edm.testPseudonymiseDnstap(dt4)
	edm.testPseudonymiseDnstap(dt6)

	// Verify they are different from the original addresses
	if origQueryAddr4 == dt4.QueryAddr {
		t.Fatalf("updated pseudonymised IPv4 query address %s is the same as the orignal address %s", dt4.QueryAddr, origQueryAddr4)
	}
	if origRespAddr4 == dt4.ResponseAddr {
		t.Fatalf("updated pseudonymised IPv4 response address %s is the same as the orignal address %s", dt4.ResponseAddr, origRespAddr4)
	}
	if origQueryAddr6 == dt6.QueryAddr {
		t.Fatalf("updated pseudonymised IPv6 query address %s is the same as the orignal address %s", dt6.QueryAddr, origQueryAddr6)
	}
	if origRespAddr6 == dt6.ResponseAddr {
		t.Fatalf("updated pseudonymised IPv6 response address %s is the same as the orignal address %s", dt6.ResponseAddr, origRespAddr6)
	}

	// Verify the new pseudo addresses are different from the previous pseudo addresses
	if pseudoQueryAddr4 == dt4.QueryAddr {
		t.Fatalf("updated pseudonymised IPv4 query address %s is the same as the orignal pseudonymised address %s", dt4.QueryAddr, pseudoQueryAddr4)
	}
	if pseudoRespAddr4 == dt4.ResponseAddr {
		t.Fatalf("updated pseudonymised IPv4 response address %s is the same as the orignal pseudonymised address %s", dt4.ResponseAddr, pseudoRespAddr4)
	}
	if pseudoQueryAddr6 == dt6.QueryAddr {
		t.Fatalf("updated pseudonymised IPv6 query address %s is the same as the orignal pseudonymised address %s", dt6.QueryAddr, pseudoQueryAddr6)
	}
	if pseudoRespAddr6 == dt6.ResponseAddr {
		t.Fatalf("updated pseudonymised IPv6 response address %s is the same as the orignal pseudonymised address %s", dt6.ResponseAddr, pseudoRespAddr6)
	}

	// Verify they are different as expected
	if dt4.QueryAddr != expectedPseudoQueryAddrUpdated4 {
		t.Fatalf("updated pseudonymised IPv4 query address %s is not the expected address %s", dt4.QueryAddr, expectedPseudoQueryAddrUpdated4)
	}
	if dt4.ResponseAddr != expectedPseudoRespAddrUpdated4 {
		t.Fatalf("updated pseudonymised IPv4 resp address %s is not the expected address %s", dt4.ResponseAddr, expectedPseudoRespAddrUpdated4)
	}
	if dt6.QueryAddr != expectedPseudoQueryAddrUpdated6 {
		t.Fatalf("updated pseudonymised IPv6 query address %s is not the expected address %s", dt6.QueryAddr, expectedPseudoQueryAddrUpdated6)
	}
	if dt6.ResponseAddr != expectedPseudoRespAddrUpdated6 {
		t.Fatalf("updated pseudonymised IPv6 resp address %s is not the expected address %s", dt6.ResponseAddr, expectedPseudoRespAddrUpdated6)
	}

	if edm.testCryptopanCache() != nil {
		t.Logf("number of pseudonymisation cache entries before end: %d", edm.testCryptopanCache().Len())
		for _, key := range edm.testCryptopanCache().Keys() {
			value, ok := edm.testCryptopanCache().Get(key)
			if !ok {
				t.Fatalf("unable to extract value for key before end: %s", key)
			}

			t.Logf("reset cache key: %s, value: %s", key, value)
		}
	}

	// Replace the cryptopan instance with uncached version and the first key and verify we get the same pseudonymised results
	err = edm.setCryptopan(defaultTC.CryptopanKey, defaultTC.CryptopanKeySalt, 0)
	if err != nil {
		t.Fatalf("unable to call edm.SetCryptopan with 0 cache size: %s", err)
	}

	// Mirror the per-worker cache purge + disable that runMinimiser would
	// do in production: drop the existing test cache and zero the config
	// so testCryptopanCache returns nil (uncached path).
	edm.testResetCryptopanCache()
	edm.conf.CryptopanAddressEntries = 0

	// Reset the addresses and pseudonymise again with the updated key
	dt4.QueryAddr = origQueryAddr4
	dt4.ResponseAddr = origRespAddr4
	dt6.QueryAddr = origQueryAddr6
	dt6.ResponseAddr = origRespAddr6

	edm.testPseudonymiseDnstap(dt4)
	edm.testPseudonymiseDnstap(dt6)

	// Verify we are not accidentally getting IPv4-mapped IPv6 address
	if !dt4.QueryAddr.Is4() {
		t.Fatalf("uncached pseudonymised IPv4 query address appears to be IPv4-mapped IPv6 address: %s", dt4.QueryAddr)
	}
	if !dt4.ResponseAddr.Is4() {
		t.Fatalf("uncached pseudonymised IPv4 response address appears to be IPv4-mapped IPv6 address: %s", dt4.ResponseAddr)
	}

	// Verify they are different from the original addresses
	if origQueryAddr4 == dt4.QueryAddr {
		t.Fatalf("uncached pseudonymised IPv4 query address %s is the same as the orignal address %s", dt4.QueryAddr, origQueryAddr4)
	}
	if origRespAddr4 == dt4.ResponseAddr {
		t.Fatalf("uncached pseudonymised IPv4 response address %s is the same as the orignal address %s", dt4.ResponseAddr, origRespAddr4)
	}
	if origQueryAddr6 == dt6.QueryAddr {
		t.Fatalf("uncached pseudonymised IPv6 query address %s is the same as the orignal address %s", dt6.QueryAddr, origQueryAddr6)
	}
	if origRespAddr6 == dt6.ResponseAddr {
		t.Fatalf("uncached pseudonymised IPv6 response address %s is the same as the orignal address %s", dt6.ResponseAddr, origRespAddr6)
	}

	// Verify they are different as expected
	if dt4.QueryAddr != expectedPseudoQueryAddr4 {
		t.Fatalf("uncached pseudonymised IPv4 query address %s is not the expected address %s", dt4.QueryAddr, expectedPseudoQueryAddr4)
	}
	if dt4.ResponseAddr != expectedPseudoRespAddr4 {
		t.Fatalf("uncached pseudonymised IPv4 resp address %s is not the expected address %s", dt4.ResponseAddr, expectedPseudoRespAddr4)
	}
	if dt6.QueryAddr != expectedPseudoQueryAddr6 {
		t.Fatalf("uncached pseudonymised IPv6 query address %s is not the expected address %s", dt6.QueryAddr, expectedPseudoQueryAddr6)
	}
	if dt6.ResponseAddr != expectedPseudoRespAddr6 {
		t.Fatalf("uncached pseudonymised IPv6 resp address %s is not the expected address %s", dt6.ResponseAddr, expectedPseudoRespAddr6)
	}
}

func BenchmarkPseudonymiseDnstapWithCache4(b *testing.B) {
	b.ReportAllocs()

	// The original addresses we want to pseudonymise
	origQueryAddr4 := netip.MustParseAddr("198.51.100.20")
	origRespAddr4 := netip.MustParseAddr("198.51.100.30")

	edm := newRealCryptopanTestDnstapMinimiser(b, defaultTC)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		dt4 := &dnstap.Message{
			Flags:        dnstap.VALID_QUERY_ADDR | dnstap.VALID_RESPONSE_ADDR,
			QueryAddr:    origQueryAddr4,
			ResponseAddr: origRespAddr4,
		}
		edm.testPseudonymiseDnstap(dt4)
	}
}

func BenchmarkPseudonymiseDnstapWithoutCache4(b *testing.B) {
	b.ReportAllocs()

	// The original addresses we want to pseudonymise
	origQueryAddr4 := netip.MustParseAddr("198.51.100.20")
	origRespAddr4 := netip.MustParseAddr("198.51.100.30")

	uncachedTC := defaultTC
	uncachedTC.CryptopanAddressEntries = 0

	edm := newRealCryptopanTestDnstapMinimiser(b, uncachedTC)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		dt4 := &dnstap.Message{
			Flags:        dnstap.VALID_QUERY_ADDR | dnstap.VALID_RESPONSE_ADDR,
			QueryAddr:    origQueryAddr4,
			ResponseAddr: origRespAddr4,
		}
		edm.testPseudonymiseDnstap(dt4)
	}
}

func BenchmarkPseudonymiseDnstapWithCache6(b *testing.B) {
	b.ReportAllocs()

	// The original addresses we want to pseudonymise
	origQueryAddr6 := netip.MustParseAddr("2001:db8:1122:3344:5566:7788:99aa:bbcc")
	origRespAddr6 := netip.MustParseAddr("2001:db8:1122:3344:5566:7788:99aa:ddee")

	edm := newRealCryptopanTestDnstapMinimiser(b, defaultTC)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		dt6 := &dnstap.Message{
			Flags:        dnstap.VALID_QUERY_ADDR | dnstap.VALID_RESPONSE_ADDR,
			QueryAddr:    origQueryAddr6,
			ResponseAddr: origRespAddr6,
		}
		edm.testPseudonymiseDnstap(dt6)
	}
}

func BenchmarkPseudonymiseDnstapWithoutCache6(b *testing.B) {
	b.ReportAllocs()

	// The original addresses we want to pseudonymise
	origQueryAddr6 := netip.MustParseAddr("2001:db8:1122:3344:5566:7788:99aa:bbcc")
	origRespAddr6 := netip.MustParseAddr("2001:db8:1122:3344:5566:7788:99aa:ddee")

	uncachedTC := defaultTC
	uncachedTC.CryptopanAddressEntries = 0

	edm := newRealCryptopanTestDnstapMinimiser(b, uncachedTC)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		dt6 := &dnstap.Message{
			Flags:        dnstap.VALID_QUERY_ADDR | dnstap.VALID_RESPONSE_ADDR,
			QueryAddr:    origQueryAddr6,
			ResponseAddr: origRespAddr6,
		}
		edm.testPseudonymiseDnstap(dt6)
	}
}

func BenchmarkMurmurHasher(b *testing.B) {
	b.ReportAllocs()

	ipBytes := netip.MustParseAddr("198.51.100.20").AsSlice()

	murmur3Hasher := murmur3.New64()

	for n := 0; n < b.N; n++ {
		murmur3Hasher.Write(ipBytes) // #nosec G104 -- Write() on hash.Hash never returns an error (https://pkg.go.dev/hash#Hash)
		murmur3Hasher.Sum64()
		murmur3Hasher.Reset()
	}
}

func BenchmarkMurmurSum64(b *testing.B) {
	b.ReportAllocs()

	ipBytes := netip.MustParseAddr("198.51.100.20").AsSlice()

	for n := 0; n < b.N; n++ {
		murmur3.Sum64(ipBytes)
	}
}

func TestCompareMurmurHashing(t *testing.T) {
	murmur3Hasher := murmur3.New64()

	ipAddrs := []string{"198.51.100.20", "198.51.100.21", "198.51.100.22"}

	for _, ipAddr := range ipAddrs {
		ipBytes := netip.MustParseAddr(ipAddr).AsSlice()
		murmur3Hasher.Write(ipBytes) // #nosec G104 -- Write() on hash.Hash never returns an error (https://pkg.go.dev/hash#Hash)
		hasherRes := murmur3Hasher.Sum64()
		murmur3Hasher.Reset()

		sumRes := murmur3.Sum64(ipBytes)

		if hasherRes != sumRes {
			t.Fatalf("have: %d, want: %d", hasherRes, sumRes)
		}
	}
}

func TestSetCryptopanInvalidCacheSize(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	if err := edm.setCryptopan("key", "salt", -1); err == nil {
		t.Fatal("setCryptopan accepted negative cache size")
	}
}

func TestSetCryptopanFactoryErrorDoesNotAdvanceGeneration(t *testing.T) {
	factory := &secondCallErrorCryptopanFactory{}
	deps := newTestDependencies()
	deps.CryptopanFactory = factory
	edm := newTestDnstapMinimiserWithDependencies(t, defaultTC, deps)
	gen := edm.cryptopanGen.Load()
	cpn := edm.cryptopan.Load()

	err := edm.setCryptopan("key2", defaultTC.CryptopanKeySalt, defaultTC.CryptopanAddressEntries)
	if !errors.Is(err, errInjected) {
		t.Fatalf("setCryptopan error = %v, want errInjected", err)
	}
	if got := edm.cryptopanGen.Load(); got != gen {
		t.Fatalf("cryptopanGen after failed setCryptopan = %d, want %d", got, gen)
	}
	if got := edm.cryptopan.Load(); got != cpn {
		t.Fatal("cryptopan pointer changed after failed setCryptopan")
	}
}

type secondCallErrorCryptopanFactory struct {
	calls int
}

func (factory *secondCallErrorCryptopanFactory) NewCryptopan(key, salt string) (*cryptopan.Cryptopan, error) {
	factory.calls++
	if factory.calls > 1 {
		return nil, errInjected
	}
	return fastTestCryptopanFactory{}.NewCryptopan(key, salt)
}

// TestPseudonymiseIPCacheBranches covers the three pseudonymiseIP cache
// branches that TestIPConversionErrorsAndPseudonymiseInvalid (bad-slice)
// and TestPseudonymiseDnstap (one-shot success) do not reach: cache hit,
// cache eviction at the LRU size limit, and the cache-disabled path
// reached via a nil cache. pseudonymiseIP takes the per-worker cache and
// the cryptopan snapshot as parameters, so each subtest manages its own
// cache the way runMinimiser does.
func TestPseudonymiseIPCacheBranches(t *testing.T) {
	addrA := netip.MustParseAddr("198.51.100.20")
	addrB := netip.MustParseAddr("198.51.100.30")

	t.Run("cache hit on repeat", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		cpn := edm.cryptopan.Load()
		cache, err := lru.New[netip.Addr, netip.Addr](10)
		if err != nil {
			t.Fatalf("lru.New: %v", err)
		}
		// First call populates the cache, second returns the cached
		// value via cache.Get — exercising the cacheHit arm.
		first, err := edm.pseudonymiseIP(addrA, cpn, cache)
		if err != nil {
			t.Fatalf("first: %v", err)
		}
		second, err := edm.pseudonymiseIP(addrA, cpn, cache)
		if err != nil {
			t.Fatalf("second: %v", err)
		}
		if first != second {
			t.Fatalf("cache hit produced different result: %v vs %v", first, second)
		}
		// pseudonymiseIP is deterministic, so first==second holds even
		// if caching were silently bypassed. Pin the assertion to the
		// observable side-effect of the hit arm: exactly one entry in
		// the LRU, keyed by addrA.
		if got := cache.Len(); got != 1 {
			t.Fatalf("cache len = %d, want 1", got)
		}
		if !cache.Contains(netip.MustParseAddr("198.51.100.20")) {
			t.Fatal("cache does not contain addrA")
		}
	})

	t.Run("cache eviction at size limit", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		cpn := edm.cryptopan.Load()
		// Shrink the LRU to a single entry so the second distinct
		// address evicts the first — exercising the evicted arm and
		// the promCryptopanCacheEvicted.Inc() call.
		cache, err := lru.New[netip.Addr, netip.Addr](1)
		if err != nil {
			t.Fatalf("lru.New: %v", err)
		}
		if _, err := edm.pseudonymiseIP(addrA, cpn, cache); err != nil {
			t.Fatalf("populate: %v", err)
		}
		if _, err := edm.pseudonymiseIP(addrB, cpn, cache); err != nil {
			t.Fatalf("evict: %v", err)
		}
		if cache.Len() != 1 {
			t.Fatalf("cache len = %d, want 1 after eviction", cache.Len())
		}
	})

	t.Run("cache disabled bypasses cache logic", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		cpn := edm.cryptopan.Load()
		// A nil cache skips the cache-Get and cache-Add branches
		// entirely, mirroring CryptopanAddressEntries == 0.
		if _, err := edm.pseudonymiseIP(addrA, cpn, nil); err != nil {
			t.Fatalf("pseudonymiseIP with disabled cache: %v", err)
		}
	})
}

func TestIPConversionErrorsAndPseudonymiseInvalid(t *testing.T) {
	zeroAddr := netip.Addr{}

	edm := newTestDnstapMinimiser(t, defaultTC)
	got, err := edm.pseudonymiseIP(zeroAddr, edm.cryptopan.Load(), nil)
	if err == nil {
		t.Fatal("invalid pseudonymiseIP succeeded")
	}
	if got != zeroAddr {
		t.Fatalf("invalid pseudonymiseIP returned %v", got)
	}

	dt := &dnstap.Message{
		Flags:        dnstap.VALID_QUERY_ADDR | dnstap.VALID_RESPONSE_ADDR,
		QueryAddr:    zeroAddr,
		ResponseAddr: zeroAddr,
	}
	edm.testPseudonymiseDnstap(dt)
	if dt.QueryAddr != zeroAddr || dt.ResponseAddr != zeroAddr {
		t.Fatalf("invalid dnstap addresses were not zeroed: %#v", dt)
	}
}

// TestSetCryptopanBumpsGeneration verifies the contract that runMinimiser
// workers rely on: every successful setCryptopan call must increment
// edm.cryptopanGen by exactly one and atomic.Store a new cryptopan
// pointer. Workers compare cryptopanGen against their last-seen value to
// know when to Purge their local Crypto-PAn cache; if the generation
// didn't strictly advance on each rotation, stale entries from the
// previous key would silently leak through.
func TestSetCryptopanBumpsGeneration(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)

	// NewDnstapMinimiser called setCryptopan once during construction; the
	// generation we observe here is therefore the post-construction
	// baseline, not zero. We only care about strict monotonic advancement
	// per call, so capture the baseline and compare deltas.
	baselineGen := edm.cryptopanGen.Load()
	baselinePtr := edm.cryptopan.Load()
	if baselinePtr == nil {
		t.Fatalf("cryptopan pointer should be non-nil after NewDnstapMinimiser")
	}
	prevPtr := baselinePtr

	const rotations = 5
	for i := 1; i <= rotations; i++ {
		// Use a different key each time so we'd notice if the cryptopan
		// pointer was being reused (cryptopan.New produces a new instance
		// per call, so identical-key calls also produce distinct pointers
		// - but varying the key catches accidental short-circuit
		// optimisations more obviously).
		key := "rotation-key-" + string(rune('0'+i))
		if err := edm.setCryptopan(key, defaultTC.CryptopanKeySalt, defaultTC.CryptopanAddressEntries); err != nil {
			t.Fatalf("rotation %d: setCryptopan failed: %s", i, err)
		}

		gotGen := edm.cryptopanGen.Load()
		wantGen := baselineGen + uint64(i)
		if gotGen != wantGen {
			t.Fatalf("rotation %d: cryptopanGen have: %d, want: %d", i, gotGen, wantGen)
		}

		gotPtr := edm.cryptopan.Load()
		if gotPtr == nil {
			t.Fatalf("rotation %d: cryptopan pointer should not be nil", i)
		}
		// Compare against the previous rotation (not just the baseline) so a
		// rotation that reuses the immediately prior pointer is also caught.
		if gotPtr == prevPtr {
			t.Fatalf("rotation %d: cryptopan pointer was not replaced (still equal to the previous rotation's pointer)", i)
		}
		prevPtr = gotPtr
	}
}

// TestSetCryptopanCacheEntriesArgumentIgnored documents (and locks in) that
// the cacheEntries argument is a no-op for cache sizing: caches are owned
// per-worker by runMinimiser, and setCryptopan only swaps the cryptopan
// instance and bumps the generation. If a future change accidentally
// re-introduced shared cache state on setCryptopan it would re-introduce the
// contention this design avoids, so we pin the contract here.
func TestSetCryptopanCacheEntriesArgumentIgnored(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)

	// Wildly different cacheEntries values - including 0 (the sentinel that
	// disables the per-worker cache) and a very large value - must all behave
	// the same from setCryptopan's perspective: bump generation, swap pointer,
	// do not touch any per-worker cache state (there is none on edm itself).
	for _, n := range []int{0, 1, 1_000, 1_000_000} {
		genBefore := edm.cryptopanGen.Load()
		err := edm.setCryptopan(defaultTC.CryptopanKey, defaultTC.CryptopanKeySalt, n)
		if err != nil {
			t.Fatalf("setCryptopan(cacheEntries=%d) failed: %s", n, err)
		}
		if got := edm.cryptopanGen.Load(); got != genBefore+1 {
			t.Fatalf("setCryptopan(cacheEntries=%d): gen have: %d, want: %d", n, got, genBefore+1)
		}
	}
}

// TestGetCryptopanAESKeyDeterministic locks in the key-derivation contract
// that operators depend on: identical (key, salt) must produce identical
// AES bytes across runs and process restarts (i.e. argon2 is deterministic
// for a given parameter set). Operators rely on this so that on-disk data
// pseudonymised before a restart can still be correlated against data
// pseudonymised after - provided the configured key/salt did not change.
func TestGetCryptopanAESKeyDeterministic(t *testing.T) {
	const key = "operator-key"
	const salt = "operator-salt-aabbccdd"

	first := getCryptopanAESKey(key, salt)
	second := getCryptopanAESKey(key, salt)

	if len(first) != 32 {
		t.Fatalf("aes key length have: %d, want: 32", len(first))
	}
	if string(first) != string(second) {
		t.Fatalf("getCryptopanAESKey not deterministic for the same input")
	}

	// And differing inputs must produce different keys, otherwise the
	// derivation would be pointless. We don't audit the Argon2 strength
	// here - only that two trivially distinct inputs disagree.
	if string(first) == string(getCryptopanAESKey(key+"!", salt)) {
		t.Fatalf("getCryptopanAESKey returned same bytes for differing keys")
	}
	if string(first) == string(getCryptopanAESKey(key, salt+"!")) {
		t.Fatalf("getCryptopanAESKey returned same bytes for differing salts")
	}
}
