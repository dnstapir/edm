package runner

import (
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	dnstap "github.com/dnstap/golang-dnstap"
	"github.com/dnstapir/edm/pkg/protocols"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/miekg/dns"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"
)

func TestSeenQnameWriteOptions(t *testing.T) {
	if got := seenQnameWriteOptions(config{}); got != pebble.NoSync {
		t.Fatalf("default seen-qname write option = %p, want %p", got, pebble.NoSync)
	}

	if got := seenQnameWriteOptions(config{PebbleSync: true}); got != pebble.Sync {
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
			results <- edm.qnameSeen(msg, seenQnameLRU, pdb, seenQnameWriteOptions(defaultTC.config))
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

// TestRunMinimiserSessionSendUnblocksOnContextCancel verifies that
// runMinimiser stops blocking on a full sessionCollectorCh once the context
// is cancelled.
//
// sessionCollectorCh is pre-filled to capacity so the session send blocks, and
// newQnamePublisherCh is buffered (cap 1) so runMinimiser's non-blocking
// publisher send lands in the buffer instead of being dropped by its default
// case; receiving that event proves runMinimiser is past the publisher send and
// into the (blocked) session send. With the send guarded by a select on
// edm.ctx.Done, cancelling the context lets runMinimiser exit; an unconditional
// send would deadlock and waitOrFail would time out.
func TestRunMinimiserSessionSendUnblocksOnContextCancel(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	edm.reloadMinimiserConfigCh = []chan struct{}{make(chan struct{}, 1)}
	edm.newQnamePublisherCh = make(chan *protocols.NewQnameJSON, 1)
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

func TestRunMinimiserSkipsMalformedFrames(t *testing.T) {
	edm, seenQnameLRU, pdb, wkdTracker := newRunMinimiserTestFixture(t, "example.com.")

	var wg sync.WaitGroup
	wg.Add(1)
	go edm.runMinimiser(0, &wg, seenQnameLRU, pdb, nil, defaultLabelLimit, wkdTracker)
	t.Cleanup(func() {
		edm.stop()
		waitOrFail(t, &wg, 2*time.Second, "runMinimiser did not exit after stop")
	})

	// Not protobuf at all.
	edm.inputChannel <- []byte{0xff, 0x01, 0x02}
	// Valid envelope but no Message.
	edm.inputChannel <- marshaledDnstap(t, &dnstap.Dnstap{Type: dnstap.Dnstap_MESSAGE.Enum()})
	// Message present but its required Type is missing.
	edm.inputChannel <- marshalPartialDnstap(t, &dnstap.Dnstap{
		Type:    dnstap.Dnstap_MESSAGE.Enum(),
		Message: &dnstap.Message{},
	})
	// A well-formed response for a well-known domain must still be processed.
	edm.inputChannel <- marshaledDnstap(t, testDnstapMessage(t, dnstap.Message_CLIENT_RESPONSE, dnstap.SocketFamily_INET, packedDNSMsg(t, "example.com.", dns.TypeA, dns.RcodeSuccess)))

	select {
	case wu := <-wkdTracker.updateCh:
		if wu.dawgIndex == dawgNotFound {
			t.Fatal("valid frame after malformed input was not treated as well-known")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("valid frame after malformed input was not processed")
	}

	// Exactly one parse error is counted: only the missing-Message frame both
	// unmarshals and reaches a parse-error guard. The non-protobuf frame and
	// the missing-Type frame fail proto.Unmarshal first (dnstap is proto2, so a
	// Message without its required Type is rejected on unmarshal) and are
	// skipped without counting. The valid frame is processed after all three,
	// so by the time it reaches wkdTracker the counter has settled.
	var m dto.Metric
	if err := edm.promDNSParseError.Write(&m); err != nil {
		t.Fatalf("write promDNSParseError: %s", err)
	}
	if got := m.GetCounter().GetValue(); got != 1 {
		t.Fatalf("promDNSParseError = %v, want 1", got)
	}
}

func newRunMinimiserTestFixture(t *testing.T, knownDomains ...string) (*dnstapMinimiser, *lru.Cache[string, struct{}], *pebble.DB, *wellKnownDomainsTracker) {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	edm, err := newDnstapMinimiser(logger, defaultTC)
	if err != nil {
		t.Fatalf("newDnstapMinimiser: %s", err)
	}
	t.Cleanup(edm.stop)
	edm.reloadMinimiserConfigCh = []chan struct{}{make(chan struct{}, 1)}

	seenQnameLRU, err := lru.New[string, struct{}](10)
	if err != nil {
		t.Fatalf("lru.New: %s", err)
	}

	pdb := newTestPebble(t)

	wkdTracker, err := newWellKnownDomainsTracker(testDawgFinder(t, knownDomains...), time.Time{})
	if err != nil {
		t.Fatalf("newWellKnownDomainsTracker: %s", err)
	}

	return edm, seenQnameLRU, pdb, wkdTracker
}

// marshalPartialDnstap marshals dt without enforcing required fields, allowing
// frames that exercise the minimiser's nil/missing-field guards.
func marshalPartialDnstap(t *testing.T, dt *dnstap.Dnstap) []byte {
	t.Helper()

	frame, err := proto.MarshalOptions{AllowPartial: true}.Marshal(dt)
	if err != nil {
		t.Fatalf("proto.Marshal: %s", err)
	}
	return frame
}
