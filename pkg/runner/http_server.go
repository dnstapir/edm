package runner

import (
	"context"
	"net/http"
	_ "net/http/pprof" // #nosec G108 -- pprofServer only listens to localhost
	"time"

	_ "github.com/grafana/pyroscope-go/godeltaprof/http/pprof" // revive linter: keep blank import close to where it is used for now.
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	metricsServerWriteTimeout        = 10 * time.Second
	manualParquetRotationWaitTimeout = metricsServerWriteTimeout - time.Second
	// pprofWriteTimeout has to outlast a full pprof profile, whose default
	// `seconds` parameter is 30 — anything shorter would truncate the
	// default CPU profile.
	pprofWriteTimeout = 31 * time.Second
)

// newPprofServer constructs the pprof HTTP server that serves net/http/pprof
// from the default mux (the package's init() registers the handlers on import).
// Address is parameterised so tests can listen on an ephemeral port.
func newPprofServer(addr string) *http.Server {
	return &http.Server{
		Addr:         addr,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: pprofWriteTimeout,
	}
}

// newMetricsServer constructs the metrics HTTP server: /metrics is wired
// against edm.promReg, and /debug/rotate-parquet is registered only when
// enableManualRotation is true. Address is parameterised so tests can
// listen on an ephemeral port.
func (edm *DnstapMinimiser) newMetricsServer(ctx context.Context, addr string, enableManualRotation bool) *http.Server {
	mux := http.NewServeMux()
	// Setup custom promHandler since we want to use our per-edm registry.
	mux.Handle("/metrics", promhttp.InstrumentMetricHandler(edm.promReg, promhttp.HandlerFor(edm.promReg, promhttp.HandlerOpts{Registry: edm.promReg})))
	if enableManualRotation {
		mux.HandleFunc("/debug/rotate-parquet", func(w http.ResponseWriter, r *http.Request) {
			edm.manualParquetRotationHandler(ctx, w, r)
		})
	}
	return &http.Server{
		Addr:           addr,
		Handler:        mux,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   metricsServerWriteTimeout,
		MaxHeaderBytes: 1 << 20,
	}
}
