package runner

import (
	"strconv"
	"testing"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/miekg/dns"
)

// BenchmarkQnameSeen measures the qnameSeen hot path: a qname that has already
// been seen, which is the common per-packet case in runMinimiser (the first
// sighting of any qname happens once; every later packet for it takes this
// LRU-hit path). It runs in parallel because runMinimiser fans out across
// workers that all share one seenQnameLRU, one pebble DB, and seenQnameMutex, so
// the benchmark reflects the real contention on those shared structures.
func BenchmarkQnameSeen(b *testing.B) {
	edm := newTestDnstapMinimiser(b, defaultTC)

	const nQnames = 10_000
	cache, err := lru.New[string, struct{}](nQnames * 2) // oversized so nothing is evicted
	if err != nil {
		b.Fatalf("lru.New: %s", err)
	}
	pdb := newTestPebble(b)
	writeOpts := seenQnameWriteOptions(defaultTC.config)

	msgs := make([]*dns.Msg, nQnames)
	for i := range msgs {
		m := new(dns.Msg)
		m.SetQuestion("host"+strconv.Itoa(i)+".example.com.", dns.TypeA)
		msgs[i] = m
		// Record it so the benchmarked calls below all take the seen path.
		edm.qnameSeen(m, cache, pdb, writeOpts)
	}

	// Sanity check (in the benchmark goroutine, not the parallel workers): a
	// seeded qname must report as already seen.
	if !edm.qnameSeen(msgs[0], cache, pdb, writeOpts) {
		b.Fatal("seeded qname should report as already seen")
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			edm.qnameSeen(msgs[i%nQnames], cache, pdb, writeOpts)
			i++
		}
	})
}
