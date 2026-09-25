package runner

import (
	"io"
	"log/slog"
	"math"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/miekg/dns"
	"github.com/smhanov/dawg"
)

func TestDataCollectorFlushesPendingDataOnShutdown(t *testing.T) {
	edm, wkdTracker := newDataCollectorTestFixture(t, "example.com.")

	var wg sync.WaitGroup
	wg.Go(func() { edm.dataCollector(wkdTracker, "unused-in-shutdown-test.dawg") })

	edm.sessionCollectorCh <- &sessionData{ServerID: []byte("serverID")}

	msg := new(dns.Msg)
	msg.SetQuestion("example.com.", dns.TypeA)
	dawgIndex, suffixMatch, dawgModTime := wkdTracker.lookup(msg)
	wkdTracker.updateCh <- wkdUpdate{
		dawgIndex:   dawgIndex,
		suffixMatch: suffixMatch,
		dawgModTime: dawgModTime,
		histogramData: histogramData{
			ACount:  1,
			OKCount: 1,
		},
		hllDataSource: IdentifierIPv4,
		hllHash:       4444,
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
	got, ok := prevWKD.m[dawgIndex]
	if !ok || got == nil {
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

func TestDataCollectorAdvancesSessionIntervalWhenRotationFails(t *testing.T) {
	edm, wkdTracker := newDataCollectorTestFixture(t, "example.com.")

	var wg sync.WaitGroup
	// A requested reload of a missing dawg file makes rotateTracker fail,
	// exercising the path where session data is flushed but histogram
	// rotation errors out.
	edm.dawgReloadRequested.Store(true)
	wg.Go(func() { edm.dataCollector(wkdTracker, "missing-dawg-file.dawg") })

	edm.sessionCollectorCh <- &sessionData{ServerID: []byte("first")}

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

	edm.sessionCollectorCh <- &sessionData{ServerID: []byte("second")}

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

func newDataCollectorTestFixture(t *testing.T, knownDomains ...string) (*DnstapMinimiser, *wellKnownDomainsTracker) {
	t.Helper()

	edm := newTestDnstapMinimiser(t, defaultTC)

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

func TestDataCollector(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deps := defaultDependencies()
		edm := &DnstapMinimiser{
			conf:               Config{HistogramHLLExplicitThreshold: defaultTC.HistogramHLLExplicitThreshold},
			log:                slog.New(slog.NewTextHandler(io.Discard, nil)),
			deps:               deps,
			sessionCollectorCh: make(chan *sessionData, 1),
			sessionWriterCh:    make(chan *prevSessions, 1),
			histogramWriterCh:  make(chan *wellKnownDomainsData, 1),
		}

		path := testDawgFile(t, "example.com.")
		finder, modTime, err := (realDawgLoader{fs: osFileSystem{}}).LoadDawgFile(path)
		if err != nil {
			t.Fatal(err)
		}
		wkd, err := newWellKnownDomainsTracker(finder, modTime)
		if err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		wg.Go(func() { edm.dataCollector(wkd, path) })

		edm.sessionCollectorCh <- &sessionData{ServerID: []byte("server")}
		wkd.updateCh <- wkdUpdate{
			histogramData: histogramData{ACount: 1, OKCount: 1},
			dawgIndex:     0,
			dawgModTime:   modTime,
			hllDataSource: IdentifierIPv4,
			hllHash:       4444,
		}
		time.Sleep(timeUntilNextMinute())
		close(wkd.stop)
		wg.Wait()

		if _, ok := <-edm.sessionWriterCh; !ok {
			t.Fatal("sessionWriterCh closed before queued session could be read")
		}
		if _, ok := <-edm.histogramWriterCh; !ok {
			t.Fatal("histogramWriterCh closed before queued histogram could be read")
		}
	})
}

func TestDataCollectorHistogramIdentityCounters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// setup edm
		edm, wkdTracker := newDataCollectorTestFixture(t, "example.com.")
		go edm.dataCollector(wkdTracker, "testdata/ignored-question-names.empty.dawg")

		// setup a well known domain update
		msg := new(dns.Msg)
		msg.SetQuestion("example.com.", dns.TypeA)
		dawgIndex, suffixMatch, dawgModTime := wkdTracker.lookup(msg)
		update := wkdUpdate{
			dawgIndex:   dawgIndex,
			suffixMatch: suffixMatch,
			dawgModTime: dawgModTime,
		}

		// send zero IPv4
		// [nothing to do]

		// send one IPv6
		update.hllDataSource = IdentifierIPv6
		update.hllHash = math.MaxUint64
		wkdTracker.updateCh <- update

		// send two Other
		update.hllDataSource = IdentifierOther
		update.hllHash = math.MaxUint64
		wkdTracker.updateCh <- update
		wkdTracker.updateCh <- update

		// wait until the histogram gets packed up for sending to core
		time.Sleep(timeUntilNextMinuteFrom(time.Now()))

		// extract histogram
		histogram, ok := <-edm.histogramWriterCh
		if !ok {
			t.Fatal("histogramWriterCh closed without flushing pending histogram data")
		}
		if len(histogram.m) != 1 {
			t.Fatalf("flushed histogram domains have: %d, want: 1", len(histogram.m))
		}
		// extract data for our domain
		got, ok := histogram.m[dawgIndex]
		if !ok || got == nil {
			t.Fatalf("flushed histogram missing DAWG index %d", dawgIndex)
			return
		}

		// check counters
		if got.v4ClientHLL.Cardinality() != 0 {
			t.Fatalf("Incorrect cardinality of IPv4: got %d != 0", got.v4ClientHLL.Cardinality())
		}
		if got.v6ClientHLL.Cardinality() != 1 {
			t.Fatalf("Incorrect cardinality of IPv6: got %d != 1", got.v6ClientHLL.Cardinality())
		}
		if got.NotValidIPCount != 2 {
			t.Fatalf("Incorrect count of identifier of unknown origin: got %d != 2", got.NotValidIPCount)
		}

		// cleanup
		close(wkdTracker.stop)
	})
}
