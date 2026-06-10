package runner

import (
	"context"
	"net/netip"
	"sync"
	"testing"

	dnstap "github.com/dnstap/golang-dnstap"
	lru "github.com/hashicorp/golang-lru/v2"
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

func cleanupTestMinimiser(edm *DnstapMinimiser) {
	if edm.fsWatcher != nil {
		_ = edm.fsWatcher.Close()
		edm.fsWatcher = nil
	}
}
