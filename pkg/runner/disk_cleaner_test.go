package runner

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/dnstapir/edm/pkg/protocols"
	"github.com/prometheus/client_golang/prometheus"
)

func TestDiskCleanerRetentionThreshold(t *testing.T) {
	now := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)

	if !sentHistogramExpired(now.Add(-sentHistogramRetention-time.Hour), now) {
		t.Fatal("histogram older than the retention window should expire")
	}

	if sentHistogramExpired(now.Add(-sentHistogramRetention+time.Hour), now) {
		t.Fatal("histogram within the retention window should not expire")
	}

	// Exactly at the retention boundary: sentHistogramExpired uses a strict
	// greater-than, so a histogram aged exactly 24 hours is not yet expired.
	if sentHistogramExpired(now.Add(-sentHistogramRetention), now) {
		t.Fatal("histogram exactly at the 24 hour retention boundary should not expire")
	}
}

// TestDiskCleanerOsReadDirError covers the non-ENOENT ReadDir error
// branch: TestMonitorAndDiskCleaner exercises the success path and the
// ENOENT-skip arm; here we inject a generic error and assert it is
// logged as "unable to read sent dir".
func TestDiskCleanerOsReadDirError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		buf := &syncBuf{}
		ctx, cancel := context.WithCancel(t.Context())
		deps := defaultDependencies()
		deps.FileSystem = faultingFileSystem{fileSystem: deps.FileSystem, readDir: func(string) ([]os.DirEntry, error) { return nil, errInjected }}
		deps.DiskCleanerInterval = time.Millisecond
		edm := &DnstapMinimiser{
			log:  slog.New(slog.NewJSONHandler(buf, nil)),
			deps: deps,
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go edm.diskCleaner(ctx, &wg, t.TempDir())
		// Advance just past the disk-cleaner interval so a tick fires.
		time.Sleep(time.Second)
		cancel()
		wg.Wait()

		if !strings.Contains(buf.String(), "unable to read sent dir") {
			t.Fatalf("expected read-dir error log, got: %q", buf.String())
		}
	})
}

func TestMonitorAndDiskCleaner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		deps := defaultDependencies()
		deps.MonitorChannelInterval = time.Millisecond
		deps.DiskCleanerInterval = time.Millisecond
		edm := &DnstapMinimiser{
			log:                    slog.New(slog.NewTextHandler(io.Discard, nil)),
			deps:                   deps,
			promNewQnameChannelLen: prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_gauge"}),
			newQnamePublisherCh:    make(chan *protocols.NewQnameJSON, 3),
		}
		edm.newQnamePublisherCh = make(chan *protocols.NewQnameJSON, 3)
		edm.newQnamePublisherCh <- &protocols.NewQnameJSON{}

		var wg sync.WaitGroup
		wg.Add(1)
		go edm.monitorChannelLen(ctx, &wg)
		time.Sleep(time.Second)
		cancel()
		wg.Wait()

		sentDir := t.TempDir()
		oldFile := filepath.Join(sentDir, "dns_histogram-2026-05-28T12-00-00Z_2026-05-28T12-01-00Z.parquet")
		if err := os.WriteFile(oldFile, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		// One hour past the retention window, so diskCleaner must remove it.
		oldTime := time.Now().Add(-sentHistogramRetention - time.Hour)
		if err := os.Chtimes(oldFile, oldTime, oldTime); err != nil {
			t.Fatal(err)
		}
		ctx, cancel = context.WithCancel(t.Context())
		t.Cleanup(cancel)
		wg.Add(1)
		go edm.diskCleaner(ctx, &wg, sentDir)
		time.Sleep(time.Minute)
		cancel()
		wg.Wait()
		if _, err := os.Stat(oldFile); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("old file still exists: %v", err)
		}
	})
}
