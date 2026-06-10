package runner

import (
	"bytes"
	"io"
	"log/slog"
	"net/netip"
	"testing"

	dnstap "github.com/dnstap/golang-dnstap"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/spaolacci/murmur3"
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

	dt4 := &dnstap.Dnstap{
		Message: &dnstap.Message{
			QueryAddress:    origQueryAddr4.AsSlice(),
			ResponseAddress: origRespAddr4.AsSlice(),
		},
	}
	dt6 := &dnstap.Dnstap{
		Message: &dnstap.Message{
			QueryAddress:    origQueryAddr6.AsSlice(),
			ResponseAddress: origRespAddr6.AsSlice(),
		},
	}

	edm := newTestDnstapMinimiser(t, defaultTC)

	if edm.testCryptopanCache() != nil {
		if edm.testCryptopanCache().Len() != 0 {
			t.Fatalf("there should be no entries in newly initialised cryptopan cache but it contains items: %d", edm.testCryptopanCache().Len())
		}
	}

	edm.testPseudonymiseDnstap(dt4)
	edm.testPseudonymiseDnstap(dt6)

	pseudoQueryAddr4, ok := netip.AddrFromSlice(dt4.Message.QueryAddress)
	if !ok {
		t.Fatal("unable to parse IPv4 QueryAddress")
	}
	pseudoRespAddr4, ok := netip.AddrFromSlice(dt4.Message.ResponseAddress)
	if !ok {
		t.Fatal("unable to parse IPv4 ResponseAddress")
	}

	pseudoQueryAddr6, ok := netip.AddrFromSlice(dt6.Message.QueryAddress)
	if !ok {
		t.Fatal("unable to parse IPv6 QueryAddress")
	}
	pseudoRespAddr6, ok := netip.AddrFromSlice(dt6.Message.ResponseAddress)
	if !ok {
		t.Fatal("unable to parse IPv6 ResponseAddress")
	}

	// Verify we are not accidentally getting IPv4-mapped IPv6 address
	if !pseudoQueryAddr4.Is4() {
		t.Fatalf("pseudonymised IPv4 query address appears to be IPv4-mapped IPv6 address: %s", pseudoQueryAddr4)
	}
	if !pseudoRespAddr4.Is4() {
		t.Fatalf("pseudonymised IPv4 response address appears to be IPv4-mapped IPv6 address: %s", pseudoRespAddr4)
	}

	// Verify they are different from the original addresses
	if origQueryAddr4 == pseudoQueryAddr4 {
		t.Fatalf("pseudonymised IPv4 query address %s is the same as the orignal address %s", pseudoQueryAddr4, origQueryAddr4)
	}
	if origRespAddr4 == pseudoRespAddr4 {
		t.Fatalf("pseudonymised IPv4 response address %s is the same as the orignal address %s", pseudoRespAddr4, origRespAddr4)
	}
	if origQueryAddr6 == pseudoQueryAddr6 {
		t.Fatalf("pseudonymised IPv6 query address %s is the same as the orignal address %s", pseudoQueryAddr6, origQueryAddr6)
	}
	if origRespAddr6 == pseudoRespAddr6 {
		t.Fatalf("pseudonymised IPv6 response address %s is the same as the orignal address %s", pseudoRespAddr6, origRespAddr6)
	}

	// Verify they are different as expected
	if pseudoQueryAddr4 != expectedPseudoQueryAddr4 {
		t.Fatalf("pseudonymised IPv4 query address %s is not the expected address %s", pseudoQueryAddr4, expectedPseudoQueryAddr4)
	}
	if pseudoRespAddr4 != expectedPseudoRespAddr4 {
		t.Fatalf("pseudonymised IPv4 resp address %s is not the expected address %s", pseudoRespAddr4, expectedPseudoRespAddr4)
	}
	if pseudoQueryAddr6 != expectedPseudoQueryAddr6 {
		t.Fatalf("pseudonymised IPv6 query address %s is not the expected address %s", pseudoQueryAddr6, expectedPseudoQueryAddr6)
	}
	if pseudoRespAddr6 != expectedPseudoRespAddr6 {
		t.Fatalf("pseudonymised IPv6 resp address %s is not the expected address %s", pseudoRespAddr6, expectedPseudoRespAddr6)
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
		if cachedPseudoQueryAddr4 != pseudoQueryAddr4 {
			t.Fatalf("cached pseudonymised IPv4 query address %s is not the same as the calculated address %s", cachedPseudoQueryAddr4, pseudoQueryAddr4)
		}

		cachedPseudoRespAddr4, ok := edm.testCryptopanCache().Get(origRespAddr4)
		if !ok {
			t.Fatalf("unable to lookup IPv4 response address %s in cache", origRespAddr4)
		}
		if cachedPseudoRespAddr4 != pseudoRespAddr4 {
			t.Fatalf("cached pseudonymised IPv4 response address %s is not the same as the calculated address %s", cachedPseudoRespAddr4, pseudoRespAddr4)
		}

		cachedPseudoQueryAddr6, ok := edm.testCryptopanCache().Get(origQueryAddr6)
		if !ok {
			t.Fatalf("unable to lookup IPv6 query address %s in cache", origQueryAddr6)
		}
		if cachedPseudoQueryAddr6 != pseudoQueryAddr6 {
			t.Fatalf("cached pseudonymised IPv6 query address %s is not the same as the calculated address %s", cachedPseudoQueryAddr6, pseudoQueryAddr6)
		}

		cachedPseudoRespAddr6, ok := edm.testCryptopanCache().Get(origRespAddr6)
		if !ok {
			t.Fatalf("unable to lookup IPv6 response address %s in cache", origRespAddr6)
		}
		if cachedPseudoRespAddr6 != pseudoRespAddr6 {
			t.Fatalf("cached pseudonymised IPv6 response address %s is not the same as the calculated address %s", cachedPseudoRespAddr6, pseudoRespAddr6)
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

	// Reset the addresses and pseudonymise again with the updated key
	dt4.Message.QueryAddress = origQueryAddr4.AsSlice()
	dt4.Message.ResponseAddress = origRespAddr4.AsSlice()
	dt6.Message.QueryAddress = origQueryAddr6.AsSlice()
	dt6.Message.ResponseAddress = origRespAddr6.AsSlice()

	edm.testPseudonymiseDnstap(dt4)
	edm.testPseudonymiseDnstap(dt6)

	pseudoQueryAddrUpdated4, ok := netip.AddrFromSlice(dt4.Message.QueryAddress)
	if !ok {
		t.Fatal("unable to parse second IPv4 QueryAddress")
	}
	pseudoRespAddrUpdated4, ok := netip.AddrFromSlice(dt4.Message.ResponseAddress)
	if !ok {
		t.Fatal("unable to parse second IPv4 ResponseAddress")
	}
	pseudoQueryAddrUpdated6, ok := netip.AddrFromSlice(dt6.Message.QueryAddress)
	if !ok {
		t.Fatal("unable to parse second IPv6 QueryAddress")
	}
	pseudoRespAddrUpdated6, ok := netip.AddrFromSlice(dt6.Message.ResponseAddress)
	if !ok {
		t.Fatal("unable to parse second IPv6 ResponseAddress")
	}

	// Verify they are different from the original addresses
	if origQueryAddr4 == pseudoQueryAddrUpdated4 {
		t.Fatalf("updated pseudonymised IPv4 query address %s is the same as the orignal address %s", pseudoQueryAddrUpdated4, origQueryAddr4)
	}
	if origRespAddr4 == pseudoRespAddrUpdated4 {
		t.Fatalf("updated pseudonymised IPv4 response address %s is the same as the orignal address %s", pseudoRespAddrUpdated4, origRespAddr4)
	}
	if origQueryAddr6 == pseudoQueryAddrUpdated6 {
		t.Fatalf("updated pseudonymised IPv6 query address %s is the same as the orignal address %s", pseudoQueryAddrUpdated6, origQueryAddr6)
	}
	if origRespAddr6 == pseudoRespAddrUpdated6 {
		t.Fatalf("updated pseudonymised IPv6 response address %s is the same as the orignal address %s", pseudoRespAddrUpdated6, origRespAddr6)
	}

	// Verify the new pseudo addresses are different from the previous pseudo addresses
	if pseudoQueryAddr4 == pseudoQueryAddrUpdated4 {
		t.Fatalf("updated pseudonymised IPv4 query address %s is the same as the orignal pseudonymised address %s", pseudoQueryAddrUpdated4, pseudoQueryAddr4)
	}
	if pseudoRespAddr4 == pseudoRespAddrUpdated4 {
		t.Fatalf("updated pseudonymised IPv4 response address %s is the same as the orignal pseudonymised address %s", pseudoRespAddrUpdated4, pseudoRespAddr4)
	}
	if pseudoQueryAddr6 == pseudoQueryAddrUpdated6 {
		t.Fatalf("updated pseudonymised IPv6 query address %s is the same as the orignal pseudonymised address %s", pseudoQueryAddrUpdated6, pseudoQueryAddr6)
	}
	if pseudoRespAddr6 == pseudoRespAddrUpdated6 {
		t.Fatalf("updated pseudonymised IPv6 response address %s is the same as the orignal pseudonymised address %s", pseudoRespAddrUpdated6, pseudoRespAddr6)
	}

	// Verify they are different as expected
	if pseudoQueryAddrUpdated4 != expectedPseudoQueryAddrUpdated4 {
		t.Fatalf("updated pseudonymised IPv4 query address %s is not the expected address %s", pseudoQueryAddrUpdated4, expectedPseudoQueryAddrUpdated4)
	}
	if pseudoRespAddrUpdated4 != expectedPseudoRespAddrUpdated4 {
		t.Fatalf("updated pseudonymised IPv4 resp address %s is not the expected address %s", pseudoRespAddrUpdated4, expectedPseudoRespAddrUpdated4)
	}
	if pseudoQueryAddrUpdated6 != expectedPseudoQueryAddrUpdated6 {
		t.Fatalf("updated pseudonymised IPv6 query address %s is not the expected address %s", pseudoQueryAddrUpdated6, expectedPseudoQueryAddrUpdated6)
	}
	if pseudoRespAddrUpdated6 != expectedPseudoRespAddrUpdated6 {
		t.Fatalf("updated pseudonymised IPv6 resp address %s is not the expected address %s", pseudoRespAddrUpdated6, expectedPseudoRespAddrUpdated6)
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
	dt4.Message.QueryAddress = origQueryAddr4.AsSlice()
	dt4.Message.ResponseAddress = origRespAddr4.AsSlice()
	dt6.Message.QueryAddress = origQueryAddr6.AsSlice()
	dt6.Message.ResponseAddress = origRespAddr6.AsSlice()

	edm.testPseudonymiseDnstap(dt4)
	edm.testPseudonymiseDnstap(dt6)

	uncachedPseudoQueryAddr4, ok := netip.AddrFromSlice(dt4.Message.QueryAddress)
	if !ok {
		t.Fatal("unable to parse uncached IPv4 QueryAddress")
	}
	uncachedPseudoRespAddr4, ok := netip.AddrFromSlice(dt4.Message.ResponseAddress)
	if !ok {
		t.Fatal("unable to parse uncached IPv4 ResponseAddress")
	}
	uncachedPseudoQueryAddr6, ok := netip.AddrFromSlice(dt6.Message.QueryAddress)
	if !ok {
		t.Fatal("unable to parse uncached IPv6 QueryAddress")
	}
	uncachedPseudoRespAddr6, ok := netip.AddrFromSlice(dt6.Message.ResponseAddress)
	if !ok {
		t.Fatal("unable to parse uncached IPv6 ResponseAddress")
	}

	// Verify we are not accidentally getting IPv4-mapped IPv6 address
	if !uncachedPseudoQueryAddr4.Is4() {
		t.Fatalf("uncached pseudonymised IPv4 query address appears to be IPv4-mapped IPv6 address: %s", uncachedPseudoQueryAddr4)
	}
	if !uncachedPseudoRespAddr4.Is4() {
		t.Fatalf("uncached pseudonymised IPv4 response address appears to be IPv4-mapped IPv6 address: %s", uncachedPseudoRespAddr4)
	}

	// Verify they are different from the original addresses
	if origQueryAddr4 == uncachedPseudoQueryAddr4 {
		t.Fatalf("uncached pseudonymised IPv4 query address %s is the same as the orignal address %s", uncachedPseudoQueryAddr4, origQueryAddr4)
	}
	if origRespAddr4 == uncachedPseudoRespAddr4 {
		t.Fatalf("uncached pseudonymised IPv4 response address %s is the same as the orignal address %s", uncachedPseudoRespAddr4, origRespAddr4)
	}
	if origQueryAddr6 == uncachedPseudoQueryAddr6 {
		t.Fatalf("uncached pseudonymised IPv6 query address %s is the same as the orignal address %s", uncachedPseudoQueryAddr6, origQueryAddr6)
	}
	if origRespAddr6 == uncachedPseudoRespAddr6 {
		t.Fatalf("uncached pseudonymised IPv6 response address %s is the same as the orignal address %s", uncachedPseudoRespAddr6, origRespAddr6)
	}

	// Verify they are different as expected
	if uncachedPseudoQueryAddr4 != expectedPseudoQueryAddr4 {
		t.Fatalf("uncached pseudonymised IPv4 query address %s is not the expected address %s", uncachedPseudoQueryAddr4, expectedPseudoQueryAddr4)
	}
	if uncachedPseudoRespAddr4 != expectedPseudoRespAddr4 {
		t.Fatalf("uncached pseudonymised IPv4 resp address %s is not the expected address %s", uncachedPseudoRespAddr4, expectedPseudoRespAddr4)
	}
	if uncachedPseudoQueryAddr6 != expectedPseudoQueryAddr6 {
		t.Fatalf("uncached pseudonymised IPv6 query address %s is not the expected address %s", uncachedPseudoQueryAddr6, expectedPseudoQueryAddr6)
	}
	if uncachedPseudoRespAddr6 != expectedPseudoRespAddr6 {
		t.Fatalf("uncached pseudonymised IPv6 resp address %s is not the expected address %s", uncachedPseudoRespAddr6, expectedPseudoRespAddr6)
	}
}

func BenchmarkPseudonymiseDnstapWithCache4(b *testing.B) {
	b.ReportAllocs()

	// The original addresses we want to pseudonymise
	origQueryAddr4 := netip.MustParseAddr("198.51.100.20")
	origRespAddr4 := netip.MustParseAddr("198.51.100.30")

	edm := newTestDnstapMinimiser(b, defaultTC)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		dt4 := &dnstap.Dnstap{
			Message: &dnstap.Message{
				QueryAddress:    origQueryAddr4.AsSlice(),
				ResponseAddress: origRespAddr4.AsSlice(),
			},
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

	edm := newTestDnstapMinimiser(b, uncachedTC)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		dt4 := &dnstap.Dnstap{
			Message: &dnstap.Message{
				QueryAddress:    origQueryAddr4.AsSlice(),
				ResponseAddress: origRespAddr4.AsSlice(),
			},
		}
		edm.testPseudonymiseDnstap(dt4)
	}
}

func BenchmarkPseudonymiseDnstapWithCache6(b *testing.B) {
	b.ReportAllocs()

	// The original addresses we want to pseudonymise
	origQueryAddr6 := netip.MustParseAddr("2001:db8:1122:3344:5566:7788:99aa:bbcc")
	origRespAddr6 := netip.MustParseAddr("2001:db8:1122:3344:5566:7788:99aa:ddee")

	edm := newTestDnstapMinimiser(b, defaultTC)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		dt6 := &dnstap.Dnstap{
			Message: &dnstap.Message{
				QueryAddress:    origQueryAddr6.AsSlice(),
				ResponseAddress: origRespAddr6.AsSlice(),
			},
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

	edm := newTestDnstapMinimiser(b, uncachedTC)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		dt6 := &dnstap.Dnstap{
			Message: &dnstap.Message{
				QueryAddress:    origQueryAddr6.AsSlice(),
				ResponseAddress: origRespAddr6.AsSlice(),
			},
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

// TestPseudonymiseIPCacheBranches covers the three pseudonymiseIP cache
// branches that TestIPConversionErrorsAndPseudonymiseInvalid (bad-slice)
// and TestPseudonymiseDnstap (one-shot success) do not reach: cache hit,
// cache eviction at the LRU size limit, and the cache-disabled path
// reached via a nil cache. pseudonymiseIP takes the per-worker cache and
// the cryptopan snapshot as parameters, so each subtest manages its own
// cache the way runMinimiser does.
func TestPseudonymiseIPCacheBranches(t *testing.T) {
	addrA := netip.MustParseAddr("198.51.100.20").AsSlice()
	addrB := netip.MustParseAddr("198.51.100.30").AsSlice()

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
		if !bytes.Equal(first, second) {
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
	if _, err := ipBytesToInt([]byte{1, 2, 3}); err == nil {
		t.Fatal("short IPv4 bytes succeeded")
	}
	if _, _, err := ip6BytesToInt([]byte{1, 2, 3}); err == nil {
		t.Fatal("short IPv6 bytes succeeded")
	}

	edm := newTestDnstapMinimiser(t, defaultTC)
	got, err := edm.pseudonymiseIP([]byte{1, 2, 3}, edm.cryptopan.Load(), nil)
	if err == nil {
		t.Fatal("invalid pseudonymiseIP succeeded")
	}
	if !bytes.Equal(got, []byte{0, 0, 0}) {
		t.Fatalf("invalid pseudonymiseIP returned %v", got)
	}

	dt := &dnstap.Dnstap{Message: &dnstap.Message{QueryAddress: []byte{1, 2, 3}, ResponseAddress: []byte{4, 5, 6}}}
	edm.testPseudonymiseDnstap(dt)
	if !bytes.Equal(dt.Message.QueryAddress, []byte{0, 0, 0}) || !bytes.Equal(dt.Message.ResponseAddress, []byte{0, 0, 0}) {
		t.Fatalf("invalid dnstap addresses were not zeroed: %#v", dt.Message)
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
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	edm, err := NewDnstapMinimiser(defaultTC, logger)
	if err != nil {
		t.Fatalf("unable to setup edm: %s", err)
	}
	t.Cleanup(func() { cleanupTestMinimiser(edm) })

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
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	edm, err := NewDnstapMinimiser(defaultTC, logger)
	if err != nil {
		t.Fatalf("unable to setup edm: %s", err)
	}
	t.Cleanup(func() { cleanupTestMinimiser(edm) })

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
