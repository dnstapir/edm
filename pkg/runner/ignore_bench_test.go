package runner

import (
	"net/netip"
	"testing"

	dnstap "github.com/dnstap/golang-dnstap"
	"github.com/miekg/dns"
)

// BenchmarkIgnoreChecksParallel measures the per-packet ignore-list read path
// (clientIPIsIgnored + questionIsIgnored) under concurrent minimiser workers.
//
// These reads are taken on every packet by every worker. This benchmark
// isolates the synchronisation cost of that read path — the ignore lists are
// left unset, so each call is just the snapshot read plus a nil check, which is
// exactly the part this code controls (an atomic.Pointer load versus, in the
// previous design, a RWMutex.RLock). Run with -cpu=1,8,... to see how it scales
// across workers; the lock-free version should stay flat while a reader-lock
// version degrades as cores (and thus reader-lock contention) increase.
func BenchmarkIgnoreChecksParallel(b *testing.B) {
	edm := newTestDnstapMinimiser(b, defaultTC)

	dt := &dnstap.Dnstap{
		Message: &dnstap.Message{
			QueryAddress: netip.MustParseAddr("198.51.100.20").AsSlice(),
		},
	}
	msg := new(dns.Msg)
	msg.SetQuestion("example.com.", dns.TypeA)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			edm.clientIPIsIgnored(dt)
			edm.questionIsIgnored(msg)
		}
	})
}
