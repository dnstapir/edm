package runner

import (
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	dnstap "github.com/dnstap/golang-dnstap"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/miekg/dns"
	"google.golang.org/protobuf/proto"
)

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
