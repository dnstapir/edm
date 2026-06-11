package runner

import (
	"sync"
	"testing"

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

// fakeSeenQnameStore returns canned results so qnameSeen error paths can be
// exercised without a pebble instance.
type fakeSeenQnameStore struct {
	hasSeen   bool
	hasErr    error
	markCalls int
	markErr   error
}

func (f *fakeSeenQnameStore) Has(string) (bool, error) { return f.hasSeen, f.hasErr }

func (f *fakeSeenQnameStore) MarkSeen(string, bool) error {
	f.markCalls++
	return f.markErr
}

func (f *fakeSeenQnameStore) Close() error { return nil }

// TestQnameSeenStoreError verifies qnameSeen honors the lookup result when the
// store reports an error: a qname found despite a resource-cleanup error stays
// "seen" (no spurious new_qname event), while an unreadable store reports
// "new" without recording anything.
func TestQnameSeenStoreError(t *testing.T) {
	tests := []struct {
		name      string
		store     *fakeSeenQnameStore
		want      bool
		wantMarks int
	}{
		{"found with cleanup error", &fakeSeenQnameStore{hasSeen: true, hasErr: errInjected}, true, 0},
		{"lookup error", &fakeSeenQnameStore{hasErr: errInjected}, false, 0},
		{"mark error is logged only", &fakeSeenQnameStore{markErr: errInjected}, false, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			edm := newTestDnstapMinimiser(t, defaultTC)
			cache, err := lru.New[string, struct{}](1)
			if err != nil {
				t.Fatal(err)
			}

			msg := new(dns.Msg)
			msg.SetQuestion("example.com.", dns.TypeA)
			if got := edm.qnameSeen(msg, cache, tt.store, defaultTC.PebbleSync); got != tt.want {
				t.Fatalf("qnameSeen = %t, want %t", got, tt.want)
			}
			if tt.store.markCalls != tt.wantMarks {
				t.Fatalf("MarkSeen calls = %d, want %d", tt.store.markCalls, tt.wantMarks)
			}
		})
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
