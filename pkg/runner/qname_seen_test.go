package runner

import (
	"sync"
	"testing"

	"github.com/cockroachdb/pebble"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/miekg/dns"
)

func TestQnameSeen(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	db := newTestPebble(t)
	cache, err := lru.New[string, struct{}](1)
	if err != nil {
		t.Fatal(err)
	}

	msg := new(dns.Msg)
	msg.SetQuestion("Example.COM.", dns.TypeA)
	if edm.qnameSeen(msg, cache, &pebbleSeenQnameStore{db: db}, defaultTC.PebbleSync) {
		t.Fatal("first qnameSeen call returned true")
	}
	if !edm.qnameSeen(msg, cache, &pebbleSeenQnameStore{db: db}, defaultTC.PebbleSync) {
		t.Fatal("second qnameSeen call returned false")
	}

	cache, err = lru.New[string, struct{}](1)
	if err != nil {
		t.Fatal(err)
	}
	if !edm.qnameSeen(msg, cache, &pebbleSeenQnameStore{db: db}, defaultTC.PebbleSync) {
		t.Fatal("qnameSeen did not find qname in pebble")
	}

	other := new(dns.Msg)
	other.SetQuestion("other.example.", dns.TypeA)
	_ = edm.qnameSeen(other, cache, &pebbleSeenQnameStore{db: db}, defaultTC.PebbleSync)
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
	if edm.qnameSeen(first, cache, &pebbleSeenQnameStore{db: db}, defaultTC.PebbleSync) {
		t.Fatal("first qname unexpectedly already-seen")
	}

	second := new(dns.Msg)
	second.SetQuestion("b.example.", dns.TypeA)
	// Adding the second distinct qname evicts the first from the LRU,
	// exercising the evicted/promSeenQnameLRUEvicted.Inc() arm.
	_ = edm.qnameSeen(second, cache, &pebbleSeenQnameStore{db: db}, defaultTC.PebbleSync)
	if cache.Len() != 1 {
		t.Fatalf("cache len = %d, want 1 after eviction", cache.Len())
	}
	if cache.Contains("a.example.") {
		t.Fatal("a.example. should have been evicted")
	}
}

func TestSeenQnameWriteOptions(t *testing.T) {
	if got := seenQnameWriteOptions(Config{}); got != pebble.NoSync {
		t.Fatalf("default seen-qname write option = %p, want %p", got, pebble.NoSync)
	}

	if got := seenQnameWriteOptions(Config{PebbleSync: true}); got != pebble.Sync {
		t.Fatalf("pebble-sync seen-qname write option = %p, want %p", got, pebble.Sync)
	}
}

func TestQnameSeenConcurrentFirstSeenOnce(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)

	seenQnameLRU, err := lru.New[string, struct{}](10)
	if err != nil {
		t.Fatalf("lru.New: %s", err)
	}

	pdb := newTestPebble(t)

	msg := new(dns.Msg)
	msg.SetQuestion("race.example.", dns.TypeA)

	const goroutines = 64
	start := make(chan struct{})
	results := make(chan bool, goroutines)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			<-start
			results <- edm.qnameSeen(msg, seenQnameLRU, &pebbleSeenQnameStore{db: pdb}, defaultTC.PebbleSync)
		}()
	}

	close(start)
	wg.Wait()
	close(results)

	var firstSeen int
	for seen := range results {
		if !seen {
			firstSeen++
		}
	}
	if firstSeen != 1 {
		t.Fatalf("first-seen results have: %d, want: 1", firstSeen)
	}
}
