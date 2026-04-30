package runner

import (
	"sync"
	"testing"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/miekg/dns"
)

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
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			<-start
			results <- edm.qnameSeen(msg, seenQnameLRU, pdb)
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
