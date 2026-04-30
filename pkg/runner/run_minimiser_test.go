package runner

import (
	"sync"
	"testing"
	"time"

	dnstap "github.com/dnstap/golang-dnstap"
	"github.com/dnstapir/edm/pkg/protocols"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/miekg/dns"
)

// TestRunMinimiserSessionSendUnblocksOnContextCancel verifies that
// runMinimiser stops blocking on a full sessionCollectorCh once the context
// is cancelled.
//
// sessionCollectorCh is pre-filled to capacity so the session send blocks, and
// newQnamePublisherCh is left unbuffered so receiving the new_qname event
// proves runMinimiser has reached the session send. With the send guarded by a
// select on edm.ctx.Done, cancelling the context lets runMinimiser exit; an
// unconditional send would deadlock and waitOrFail would time out.
func TestRunMinimiserSessionSendUnblocksOnContextCancel(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	edm.reloadMinimiserConfigCh = []chan struct{}{make(chan struct{}, 1)}
	edm.newQnamePublisherCh = make(chan *protocols.NewQnameJSON)
	edm.sessionCollectorCh = make(chan *sessionData, 1)
	edm.sessionCollectorCh <- &sessionData{}

	seenQnameLRU, err := lru.New[string, struct{}](10)
	if err != nil {
		t.Fatalf("lru.New: %s", err)
	}
	pdb := newTestPebble(t)
	wkdTracker, err := newWellKnownDomainsTracker(testDawgFinder(t, "known.example."), time.Unix(0, 0))
	if err != nil {
		t.Fatalf("newWellKnownDomainsTracker: %s", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go edm.runMinimiser(0, &wg, seenQnameLRU, pdb, nil, defaultLabelLimit, wkdTracker)

	frame := marshaledDnstap(t, testDnstapMessage(t, dnstap.Message_CLIENT_RESPONSE, dnstap.SocketFamily_INET, packedDNSMsg(t, "new.example.", dns.TypeA, dns.RcodeSuccess)))
	edm.inputChannel <- frame

	// Receiving the new_qname event proves runMinimiser is past the
	// publisher send and about to perform the (blocked) session send.
	select {
	case <-edm.newQnamePublisherCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for new_qname event")
	}

	edm.stop()
	waitOrFail(t, &wg, 2*time.Second, "runMinimiser did not exit while blocked on a full sessionCollectorCh after context cancellation")
}
