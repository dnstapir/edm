package runner

import (
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/miekg/dns"
)

func TestQnameSeenConcurrentFirstSeenOnce(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	edm, err := newDnstapMinimiser(logger, defaultTC)
	if err != nil {
		t.Fatalf("newDnstapMinimiser: %s", err)
	}
	t.Cleanup(edm.stop)

	seenQnameLRU, err := lru.New[string, struct{}](10)
	if err != nil {
		t.Fatalf("lru.New: %s", err)
	}

	pdb, err := pebble.Open(filepath.Join(t.TempDir(), "pebble"), &pebble.Options{})
	if err != nil {
		t.Fatalf("pebble.Open: %s", err)
	}
	t.Cleanup(func() {
		if err := pdb.Close(); err != nil {
			t.Fatalf("pdb.Close: %s", err)
		}
	})

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
