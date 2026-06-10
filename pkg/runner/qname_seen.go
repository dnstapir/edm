package runner

import (
	"strings"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/miekg/dns"
)

// qnameSeen reports whether qname has been seen since startup, recording it
// (in the in-memory LRU and in the store) on first sight. syncWrites selects
// fsynced store inserts and mirrors [Config.PebbleSync].
//
// The check-and-record runs under edm.seenQnameMutex so concurrent minimiser
// workers report any given qname as new at most once.
func (edm *DnstapMinimiser) qnameSeen(msg *dns.Msg, seenQnameLRU *lru.Cache[string, struct{}], store SeenQnameStore, syncWrites bool) bool {
	qname := strings.ToLower(msg.Question[0].Name)
	edm.seenQnameMutex.Lock()
	defer edm.seenQnameMutex.Unlock()

	_, ok := seenQnameLRU.Get(qname)
	if ok {
		// It exists in the LRU cache
		return true
	}
	// Add it to the LRU
	evicted := seenQnameLRU.Add(qname, struct{}{})
	if evicted {
		edm.promSeenQnameLRUEvicted.Inc()
	}

	seen, err := store.Has(qname)
	if err != nil {
		// Has reports seen=true together with a non-nil error when the
		// value was found but releasing its resources failed; honor the
		// lookup result so an already-recorded qname is not republished
		// as new. The insert is skipped either way: the qname is already
		// recorded, or the store is in unknown shape.
		edm.log.Error("unable to get key from seen-qname store", "error", err)
		return seen
	}
	if seen {
		return true
	}

	if err := store.MarkSeen(qname, syncWrites); err != nil {
		edm.log.Error("unable to insert key in seen-qname store", "error", err)
	}
	return false
}
