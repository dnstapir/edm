package runner

import (
	"context"
	"net/http"
	"time"
)

type parquetRotationRequest struct {
	rotationTime time.Time
	done         chan error
}

func (edm *DnstapMinimiser) manualParquetRotationHandler(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	req := parquetRotationRequest{
		rotationTime: edm.deps.Clock.Now().UTC(),
		done:         make(chan error, 1),
	}

	select {
	case edm.parquetRotationRequestCh <- req:
	case <-ctx.Done():
		http.Error(w, "edm is shutting down", http.StatusServiceUnavailable)
		return
	case <-r.Context().Done():
		return
	}

	select {
	case err := <-req.done:
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		if _, err := w.Write([]byte("rotation requested\n")); err != nil {
			edm.log.Error("manualParquetRotationHandler: failed to write response", "error", err)
		}
	case <-edm.deps.Clock.After(manualParquetRotationWaitTimeout):
		http.Error(w, "timed out waiting for parquet rotation", http.StatusGatewayTimeout)
	case <-r.Context().Done():
		return
	}
}
