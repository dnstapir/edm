package runner

import (
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/spaolacci/murmur3"
)

func TestDataCollectorFlushesPendingDataOnShutdown(t *testing.T) {
	edm, _, _, wkdTracker := newRunMinimiserTestFixture(t, "example.com.")

	var wg sync.WaitGroup
	wg.Add(1)
	go edm.dataCollector(&wg, wkdTracker, "unused-in-shutdown-test.dawg")

	edm.sessionCollectorCh <- &sessionData{ServerID: ptr("serverID")}

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
		return
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
