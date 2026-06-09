package runner

import (
	"io"
	"log/slog"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/smhanov/dawg"
	"github.com/spaolacci/murmur3"
)

func TestDataCollectorFlushesPendingDataOnShutdown(t *testing.T) {
	edm, wkdTracker := newDataCollectorTestFixture(t, "example.com.")

	var wg sync.WaitGroup
	wg.Add(1)
	go edm.dataCollector(&wg, wkdTracker, "unused-in-shutdown-test.dawg")

	serverID := "serverID"
	edm.sessionCollectorCh <- &sessionData{ServerID: &serverID}

	msg := new(dns.Msg)
	msg.SetQuestion("example.com.", dns.TypeA)
	ip := netip.MustParseAddr("198.51.100.10")
	dawgIndex, suffixMatch, dawgModTime := wkdTracker.lookup(msg)
	wkdTracker.updateCh <- wkdUpdate{
		dawgIndex:   dawgIndex,
		suffixMatch: suffixMatch,
		dawgModTime: dawgModTime,
		histogramData: histogramData{
			ACount:  1,
			OKCount: 1,
		},
		hllHash: murmur3.Sum64(ip.AsSlice()),
		ip:      ip,
	}

	close(wkdTracker.stop)
	waitOrFail(t, &wg, 2*time.Second, "dataCollector did not exit after stop")

	ps, ok := <-edm.sessionWriterCh
	if !ok {
		t.Fatal("sessionWriterCh closed without flushing pending session data")
	}
	if len(ps.sessions) != 1 {
		t.Fatalf("flushed sessions have: %d, want: 1", len(ps.sessions))
	}
	if ps.startTime.IsZero() {
		t.Fatal("flushed sessions should carry the collector interval start")
	}
	if ps.rotationTime.Before(ps.startTime) {
		t.Fatalf("session interval is inverted: start=%s stop=%s", ps.startTime, ps.rotationTime)
	}

	prevWKD, ok := <-edm.histogramWriterCh
	if !ok {
		t.Fatal("histogramWriterCh closed without flushing pending histogram data")
	}
	if len(prevWKD.m) != 1 {
		t.Fatalf("flushed histogram domains have: %d, want: 1", len(prevWKD.m))
	}
	got := prevWKD.m[dawgIndex]
	if got == nil {
		t.Fatalf("flushed histogram missing DAWG index %d", dawgIndex)
	}
	if got.ACount != 1 || got.OKCount != 1 {
		t.Fatalf("flushed histogram counts have A=%d OK=%d, want A=1 OK=1", got.ACount, got.OKCount)
	}
	if prevWKD.startTime.IsZero() {
		t.Fatal("flushed histogram should carry the collector interval start")
	}
	if prevWKD.rotationTime.Before(prevWKD.startTime) {
		t.Fatalf("histogram interval is inverted: start=%s stop=%s", prevWKD.startTime, prevWKD.rotationTime)
	}
}

func TestDataCollectorAdvancesSessionIntervalWhenRotationFails(t *testing.T) {
	edm, wkdTracker := newDataCollectorTestFixture(t, "example.com.")

	var wg sync.WaitGroup
	wg.Add(1)
	// A missing dawg file makes rotateTracker fail, exercising the path
	// where session data is flushed but histogram rotation errors out.
	go edm.dataCollector(&wg, wkdTracker, "missing-dawg-file.dawg")

	firstServerID := "first"
	edm.sessionCollectorCh <- &sessionData{ServerID: &firstServerID}

	rotationTime := time.Now().UTC()
	done := make(chan error, 1)
	edm.parquetRotationRequestCh <- parquetRotationRequest{
		rotationTime: rotationTime,
		done:         done,
	}
	if err := <-done; err == nil {
		t.Fatal("manual rotation with missing dawg file should fail")
	}

	// The failed rotation still flushed the first session interval.
	first, ok := <-edm.sessionWriterCh
	if !ok {
		t.Fatal("sessionWriterCh closed without flushing the first session interval")
	}
	if len(first.sessions) != 1 {
		t.Fatalf("first flushed sessions have: %d, want: 1", len(first.sessions))
	}
	if !first.rotationTime.Equal(rotationTime) {
		t.Fatalf("first flushed sessions stop at %s, want %s", first.rotationTime, rotationTime)
	}

	secondServerID := "second"
	edm.sessionCollectorCh <- &sessionData{ServerID: &secondServerID}

	close(wkdTracker.stop)
	waitOrFail(t, &wg, 2*time.Second, "dataCollector did not exit after stop")

	// The shutdown flush must start the second session interval at the
	// failed rotation's time, proving the session boundary advanced even
	// though histogram rotation failed.
	second, ok := <-edm.sessionWriterCh
	if !ok {
		t.Fatal("sessionWriterCh closed without flushing the second session interval")
	}
	if len(second.sessions) != 1 {
		t.Fatalf("second flushed sessions have: %d, want: 1", len(second.sessions))
	}
	if !second.startTime.Equal(rotationTime) {
		t.Fatalf("second session interval starts at %s, want %s", second.startTime, rotationTime)
	}
}

func newDataCollectorTestFixture(t *testing.T, knownDomains ...string) (*dnstapMinimiser, *wellKnownDomainsTracker) {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	edm, err := newDnstapMinimiser(logger, defaultTC)
	if err != nil {
		t.Fatalf("newDnstapMinimiser: %s", err)
	}
	t.Cleanup(func() { cleanupTestMinimiser(edm) })

	dBuilder := dawg.New()
	for _, domain := range knownDomains {
		dBuilder.Add(domain)
	}
	wkdTracker, err := newWellKnownDomainsTracker(dBuilder.Finish(), time.Unix(0, 0))
	if err != nil {
		t.Fatalf("newWellKnownDomainsTracker: %s", err)
	}

	return edm, wkdTracker
}
