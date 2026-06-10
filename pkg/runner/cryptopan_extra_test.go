package runner

import (
	"io"
	"log/slog"
	"testing"
)

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
