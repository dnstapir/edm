package runner

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/smhanov/dawg"
	"github.com/spaolacci/murmur3"
)

func TestDataCollectorManualParquetRotationFlushesPendingData(t *testing.T) {
	edm, wkdTracker, dawgFile := newManualParquetRotationTestFixture(t, "example.com.")

	var wg sync.WaitGroup
	wg.Add(1)
	go edm.dataCollector(&wg, wkdTracker, dawgFile)

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

	rotationTime := time.Now().UTC().Add(time.Second)
	req := parquetRotationRequest{
		rotationTime: rotationTime,
		done:         make(chan error, 1),
	}
	edm.parquetRotationRequestCh <- req

	select {
	case err := <-req.done:
		if err != nil {
			t.Fatalf("manual parquet rotation failed: %s", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("manual parquet rotation timed out")
	}

	ps, ok := <-edm.sessionWriterCh
	if !ok {
		t.Fatal("sessionWriterCh closed without manual session flush")
	}
	if len(ps.sessions) != 1 {
		t.Fatalf("manual session flush has: %d, want: 1", len(ps.sessions))
	}
	if !ps.rotationTime.Equal(rotationTime) {
		t.Fatalf("manual session rotation time have: %s, want: %s", ps.rotationTime, rotationTime)
	}

	prevWKD, ok := <-edm.histogramWriterCh
	if !ok {
		t.Fatal("histogramWriterCh closed without manual histogram flush")
	}
	if !prevWKD.rotationTime.Equal(rotationTime) {
		t.Fatalf("manual histogram rotation time have: %s, want: %s", prevWKD.rotationTime, rotationTime)
	}
	got := prevWKD.m[dawgIndex]
	if got == nil {
		t.Fatalf("manual histogram flush missing DAWG index %d", dawgIndex)
	}
	if got.ACount != 1 || got.OKCount != 1 {
		t.Fatalf("manual histogram counts have A=%d OK=%d, want A=1 OK=1", got.ACount, got.OKCount)
	}

	close(wkdTracker.stop)
	waitForWaitGroup(t, &wg, 2*time.Second, "dataCollector did not exit after stop")
}

func TestManualParquetRotationHandlerRejectsNonPost(t *testing.T) {
	edm := newManualParquetRotationTestMinimiser(t)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/debug/rotate-parquet", nil)
	edm.manualParquetRotationHandler(t.Context(), rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status have: %d, want: %d", rec.Code, http.StatusMethodNotAllowed)
	}
	if allow := rec.Header().Get("Allow"); allow != http.MethodPost {
		t.Fatalf("Allow header have: %q, want: %q", allow, http.MethodPost)
	}
}

// TestManualParquetRotationHandlerSurfacesRotationError verifies the 500
// arm: when the rotation worker signals an error via req.done, the handler
// surfaces it as an HTTP 500 with the error string in the body.
func TestManualParquetRotationHandlerSurfacesRotationError(t *testing.T) {
	edm := newManualParquetRotationTestMinimiser(t)

	rotationHandled := make(chan struct{})
	go func() {
		req := <-edm.parquetRotationRequestCh
		req.done <- errInjected
		close(rotationHandled)
	}()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/debug/rotate-parquet", nil)
	edm.manualParquetRotationHandler(t.Context(), rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
	if !strings.Contains(rec.Body.String(), errInjected.Error()) {
		t.Fatalf("body %q does not contain injected error %q", rec.Body.String(), errInjected.Error())
	}

	select {
	case <-rotationHandled:
	case <-time.After(2 * time.Second):
		t.Fatal("rotation worker goroutine did not finish")
	}
}

// TestManualParquetRotationHandlerShutsDownDuringSend verifies the 503
// arm: when the run context is canceled before anything reads from the request
// channel, the handler aborts the send select and responds with 503.
func TestManualParquetRotationHandlerShutsDownDuringSend(t *testing.T) {
	edm := newManualParquetRotationTestMinimiser(t)
	ctx, cancel := testRunContext(t)

	// Fill the rotation channel so the handler's send select cannot
	// take the chan-send case, forcing it onto the ctx.Done branch.
	edm.parquetRotationRequestCh <- parquetRotationRequest{
		rotationTime: time.Now(),
		done:         make(chan error, 1),
	}
	cancel()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/debug/rotate-parquet", nil)
	edm.manualParquetRotationHandler(ctx, rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if !strings.Contains(rec.Body.String(), "shutting down") {
		t.Fatalf("body %q does not mention shutdown", rec.Body.String())
	}
}

// TestManualParquetRotationHandlerCancelDuringWait verifies the
// r.Context().Done() arm of the second select: once the request is
// queued, cancelling the HTTP request's context (while the worker
// withholds req.done) makes the handler return immediately without
// writing a status, leaving the recorder at its 200 default.
func TestManualParquetRotationHandlerCancelDuringWait(t *testing.T) {
	edm := newManualParquetRotationTestMinimiser(t)

	queued := make(chan parquetRotationRequest, 1)
	go func() {
		// Consume the request but deliberately do NOT signal req.done
		// so the handler stays blocked in its second select.
		queued <- <-edm.parquetRotationRequestCh
	}()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/debug/rotate-parquet", nil).WithContext(ctx)

	done := make(chan struct{})
	go func() {
		defer close(done)
		edm.manualParquetRotationHandler(t.Context(), rec, req)
	}()

	// Wait until the worker confirmed the handler queued the request,
	// then cancel the request context.
	select {
	case <-queued:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not queue the rotation request")
	}
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not return after request context cancel")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want default 200 (handler should return without writing)", rec.Code)
	}
}

func TestManualParquetRotationHandlerAcceptsCompletedRotation(t *testing.T) {
	edm := newManualParquetRotationTestMinimiser(t)

	rotationHandled := make(chan struct{})
	go func() {
		req := <-edm.parquetRotationRequestCh
		if req.rotationTime.IsZero() {
			t.Error("rotation request should include a timestamp")
		}
		req.done <- nil
		close(rotationHandled)
	}()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/debug/rotate-parquet", nil)
	edm.manualParquetRotationHandler(t.Context(), rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status have: %d, want: %d", rec.Code, http.StatusAccepted)
	}

	select {
	case <-rotationHandled:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not send a parquet rotation request")
	}
}

func newManualParquetRotationTestFixture(t *testing.T, domains ...string) (*DnstapMinimiser, *wellKnownDomainsTracker, string) {
	t.Helper()

	edm := newManualParquetRotationTestMinimiser(t)
	dawgFile, dawgFinder := writeManualParquetRotationTestDawgFile(t, domains...)
	wkdTracker, err := newWellKnownDomainsTracker(dawgFinder, time.Time{})
	if err != nil {
		t.Fatalf("newWellKnownDomainsTracker: %s", err)
	}

	return edm, wkdTracker, dawgFile
}

func newManualParquetRotationTestMinimiser(t *testing.T) *DnstapMinimiser {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	edm, err := NewDnstapMinimiser(defaultTC, logger)
	if err != nil {
		t.Fatalf("NewDnstapMinimiser: %s", err)
	}
	t.Cleanup(func() {
		if edm.fsWatcher != nil {
			_ = edm.fsWatcher.Close()
		}
	})

	return edm
}

func writeManualParquetRotationTestDawgFile(t *testing.T, domains ...string) (string, dawg.Finder) {
	t.Helper()

	slices.Sort(domains)
	builder := dawg.New()
	for _, domain := range domains {
		builder.Add(domain)
	}
	finder := builder.Finish()
	path := filepath.Join(t.TempDir(), "well-known-domains.dawg")
	if _, err := finder.Save(path); err != nil {
		t.Fatalf("dawg.Save: %s", err)
	}
	return path, finder
}

func waitForWaitGroup(t *testing.T, wg *sync.WaitGroup, timeout time.Duration, msg string) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal(msg)
	}
}
