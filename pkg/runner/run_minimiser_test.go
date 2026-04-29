package runner

import (
	"io"
	"log/slog"
	"net/netip"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	dnstap "github.com/dnstap/golang-dnstap"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/miekg/dns"
	"github.com/smhanov/dawg"
	"google.golang.org/protobuf/proto"
)

func TestRunMinimiserSkipsMalformedFrames(t *testing.T) {
	edm, seenQnameLRU, pdb, wkdTracker := newRunMinimiserTestFixture(t, "example.com.")

	var wg sync.WaitGroup
	wg.Add(1)
	go edm.runMinimiser(0, &wg, seenQnameLRU, pdb, nil, defaultLabelLimit, wkdTracker)

	edm.inputChannel <- []byte{0xff, 0x01, 0x02}
	edm.inputChannel <- marshalDnstap(t, &dnstap.Dnstap{Type: dnstap.Dnstap_MESSAGE.Enum()})
	edm.inputChannel <- marshalDnstap(t, &dnstap.Dnstap{
		Type:    dnstap.Dnstap_MESSAGE.Enum(),
		Message: &dnstap.Message{},
	}, proto.MarshalOptions{AllowPartial: true})
	edm.inputChannel <- validDnstapResponseFrame(t, "example.com.")

	select {
	case wu := <-wkdTracker.updateCh:
		if wu.dawgIndex == dawgNotFound {
			t.Fatal("valid frame after malformed input was not treated as well-known")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("valid frame after malformed input was not processed")
	}

	edm.stop()
	waitOrFail(t, &wg, 2*time.Second, "runMinimiser did not exit after stop")
}

func newRunMinimiserTestFixture(t *testing.T, knownDomains ...string) (*dnstapMinimiser, *lru.Cache[string, struct{}], *pebble.DB, *wellKnownDomainsTracker) {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	edm, err := newDnstapMinimiser(logger, defaultTC)
	if err != nil {
		t.Fatalf("newDnstapMinimiser: %s", err)
	}
	t.Cleanup(func() { _ = edm.Close() })
	edm.reloadMinimiserConfigCh = []chan struct{}{make(chan struct{}, 1)}

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

	dBuilder := dawg.New()
	for _, domain := range knownDomains {
		dBuilder.Add(domain)
	}
	wkdTracker, err := newWellKnownDomainsTracker(dBuilder.Finish(), time.Unix(0, 0))
	if err != nil {
		t.Fatalf("newWellKnownDomainsTracker: %s", err)
	}

	return edm, seenQnameLRU, pdb, wkdTracker
}

func validDnstapResponseFrame(t *testing.T, qname string) []byte {
	t.Helper()

	msg := new(dns.Msg)
	msg.SetQuestion(qname, dns.TypeA)
	msg.Response = true

	wire, err := msg.Pack()
	if err != nil {
		t.Fatalf("dns message Pack: %s", err)
	}

	messageType := dnstap.Message_CLIENT_RESPONSE
	socketFamily := dnstap.SocketFamily_INET
	socketProtocol := dnstap.SocketProtocol_UDP
	queryPort := uint32(12345)
	responsePort := uint32(53)
	responseSec := uint64(1700000000)
	responseNsec := uint32(0)

	return marshalDnstap(t, &dnstap.Dnstap{
		Type: dnstap.Dnstap_MESSAGE.Enum(),
		Message: &dnstap.Message{
			Type:             &messageType,
			SocketFamily:     &socketFamily,
			SocketProtocol:   &socketProtocol,
			QueryAddress:     netip.MustParseAddr("198.51.100.10").AsSlice(),
			ResponseAddress:  netip.MustParseAddr("198.51.100.53").AsSlice(),
			QueryPort:        &queryPort,
			ResponsePort:     &responsePort,
			ResponseTimeSec:  &responseSec,
			ResponseTimeNsec: &responseNsec,
			ResponseMessage:  wire,
		},
	})
}

func marshalDnstap(t *testing.T, dt *dnstap.Dnstap, opts ...proto.MarshalOptions) []byte {
	t.Helper()

	var (
		frame []byte
		err   error
	)
	if len(opts) > 0 {
		frame, err = opts[0].Marshal(dt)
	} else {
		frame, err = proto.Marshal(dt)
	}
	if err != nil {
		t.Fatalf("proto.Marshal: %s", err)
	}
	return frame
}
