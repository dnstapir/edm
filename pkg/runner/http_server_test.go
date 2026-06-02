package runner

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNewPprofServer(t *testing.T) {
	s := newPprofServer("127.0.0.1:0")
	if s.Addr != "127.0.0.1:0" {
		t.Fatalf("Addr = %q, want 127.0.0.1:0", s.Addr)
	}
	if s.ReadTimeout == 0 || s.WriteTimeout == 0 {
		t.Fatalf("timeouts not set: read=%s write=%s", s.ReadTimeout, s.WriteTimeout)
	}
	// pprof handlers live on http.DefaultServeMux via the blank import; the
	// server's nil Handler picks DefaultServeMux up at request time, so check
	// one of the well-known pprof routes resolves to a real handler.
	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	rec := httptest.NewRecorder()
	http.DefaultServeMux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("/debug/pprof/ status = %d, want 200", rec.Code)
	}
}

func TestNewMetricsServer(t *testing.T) {
	t.Run("metrics endpoint responds", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		s := edm.newMetricsServer("127.0.0.1:0", false)
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		rec := httptest.NewRecorder()
		s.Handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("/metrics status = %d, want 200", rec.Code)
		}
		// The promhttp InstrumentMetricHandler registers its own counter
		// against edm.promReg, so its name always appears in /metrics output
		// regardless of which other collectors are active.
		if !strings.Contains(rec.Body.String(), "promhttp_metric_handler_requests_total") {
			t.Fatalf("/metrics body lacks prometheus output: %q", rec.Body.String())
		}
	})

	t.Run("rotate-parquet absent when disabled", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		s := edm.newMetricsServer("127.0.0.1:0", false)
		req := httptest.NewRequest(http.MethodPost, "/debug/rotate-parquet", nil)
		rec := httptest.NewRecorder()
		s.Handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("/debug/rotate-parquet status = %d, want 404 when disabled", rec.Code)
		}
	})

	t.Run("rotate-parquet present when enabled", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		s := edm.newMetricsServer("127.0.0.1:0", true)
		// A GET should be rejected with 405 by manualParquetRotationHandler's
		// method-check, proving the route is wired without involving the
		// (unstarted) rotation worker that a POST would block on.
		req := httptest.NewRequest(http.MethodGet, "/debug/rotate-parquet", nil)
		rec := httptest.NewRecorder()
		s.Handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("/debug/rotate-parquet status = %d, want 405 when enabled", rec.Code)
		}
	})
}
