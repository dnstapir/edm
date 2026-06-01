package runner

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// discardEDM returns a minimal minimiser with a no-op logger, enough for
// exercising the file-operation helpers that only touch edm.log.
func discardEDM() *dnstapMinimiser {
	return &dnstapMinimiser{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

// swapSeam temporarily replaces a seam variable for the duration of the test.
func swapSeam[T any](t *testing.T, target *T, replacement T) {
	t.Helper()
	old := *target
	*target = replacement
	t.Cleanup(func() { *target = old })
}

func TestCreateFile(t *testing.T) {
	t.Run("creates missing parent dir and retries", func(t *testing.T) {
		edm := discardEDM()
		dst := filepath.Join(t.TempDir(), "missing", "sub", "out.parquet")

		f, err := edm.createFile(dst)
		if err != nil {
			t.Fatalf("createFile: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		if _, err := os.Stat(dst); err != nil {
			t.Fatalf("expected file to exist: %v", err)
		}
	})

	t.Run("mkdir failure is reported", func(t *testing.T) {
		edm := discardEDM()
		swapSeam(t, &osMkdirAll, func(string, os.FileMode) error {
			return errors.New("mkdir boom")
		})
		dst := filepath.Join(t.TempDir(), "missing", "out.parquet")
		if _, err := edm.createFile(dst); err == nil {
			t.Fatal("createFile succeeded despite mkdir failure")
		}
	})

	t.Run("non-ENOENT create error is reported", func(t *testing.T) {
		edm := discardEDM()
		swapSeam(t, &osCreate, func(string) (*os.File, error) {
			return nil, errors.New("permission boom")
		})
		if _, err := edm.createFile(filepath.Join(t.TempDir(), "out.parquet")); err == nil {
			t.Fatal("createFile succeeded despite create error")
		}
	})
}

func TestRenameFile(t *testing.T) {
	t.Run("creates missing dest dir and retries", func(t *testing.T) {
		edm := discardEDM()
		src := writeTempFile(t, "src", []byte("payload"))
		dst := filepath.Join(t.TempDir(), "newdir", "dst")

		if err := edm.renameFile(src, dst); err != nil {
			t.Fatalf("renameFile: %v", err)
		}
		if _, err := os.Stat(dst); err != nil {
			t.Fatalf("expected renamed file: %v", err)
		}
	})

	t.Run("dest dir exists but rename fails", func(t *testing.T) {
		edm := discardEDM()
		swapSeam(t, &osRename, func(string, string) error { return fs.ErrNotExist })
		swapSeam(t, &osStat, func(string) (os.FileInfo, error) { return nil, nil })
		if err := edm.renameFile("src", "dst"); err == nil {
			t.Fatal("renameFile succeeded despite persistent rename failure")
		}
	})

	t.Run("stat error is reported", func(t *testing.T) {
		edm := discardEDM()
		swapSeam(t, &osRename, func(string, string) error { return fs.ErrNotExist })
		swapSeam(t, &osStat, func(string) (os.FileInfo, error) {
			return nil, errors.New("stat boom")
		})
		if err := edm.renameFile("src", "dst"); err == nil {
			t.Fatal("renameFile succeeded despite stat error")
		}
	})

	t.Run("mkdir failure is reported", func(t *testing.T) {
		edm := discardEDM()
		swapSeam(t, &osRename, func(string, string) error { return fs.ErrNotExist })
		swapSeam(t, &osStat, func(string) (os.FileInfo, error) { return nil, fs.ErrNotExist })
		swapSeam(t, &osMkdirAll, func(string, os.FileMode) error {
			return errors.New("mkdir boom")
		})
		if err := edm.renameFile("src", "dst"); err == nil {
			t.Fatal("renameFile succeeded despite mkdir failure")
		}
	})

	t.Run("non-ENOENT rename error is reported", func(t *testing.T) {
		edm := discardEDM()
		swapSeam(t, &osRename, func(string, string) error {
			return errors.New("rename boom")
		})
		if err := edm.renameFile("src", "dst"); err == nil {
			t.Fatal("renameFile succeeded despite rename error")
		}
	})
}

// TestSessionWriterLogsCreateError verifies the sessionWriter worker logs and
// keeps running when createSessionFile fails. The failure is injected via the
// osCreate seam so writeSessionParquet is never reached.
func TestSessionWriterLogsCreateError(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	var buf bytes.Buffer
	edm.log = slog.New(slog.NewJSONHandler(&buf, nil))

	swapSeam(t, &osCreate, func(string) (*os.File, error) {
		return nil, errors.New("create boom")
	})

	edm.sessionWriterCh <- &prevSessions{rotationTime: time.Now()}
	close(edm.sessionWriterCh)

	var wg sync.WaitGroup
	wg.Add(1)
	go edm.sessionWriter(t.TempDir(), &wg)
	waitForWaitGroup(t, &wg, 5*time.Second, "sessionWriter did not exit")

	if !strings.Contains(buf.String(), `"level":"ERROR"`) || !strings.Contains(buf.String(), "sessionWriter") {
		t.Fatalf("expected error log from sessionWriter, got: %q", buf.String())
	}
}

// TestHistogramWriterLogsCreateError mirrors the session writer test for the
// histogram writer worker.
func TestHistogramWriterLogsCreateError(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	var buf bytes.Buffer
	edm.log = slog.New(slog.NewJSONHandler(&buf, nil))

	swapSeam(t, &osCreate, func(string) (*os.File, error) {
		return nil, errors.New("create boom")
	})

	edm.histogramWriterCh <- &wellKnownDomainsData{
		rotationTime: time.Now(),
		m:            map[int]*histogramData{},
	}
	close(edm.histogramWriterCh)

	var wg sync.WaitGroup
	wg.Add(1)
	go edm.histogramWriter(defaultLabelLimit, t.TempDir(), &wg)
	waitForWaitGroup(t, &wg, 5*time.Second, "histogramWriter did not exit")

	if !strings.Contains(buf.String(), `"level":"ERROR"`) || !strings.Contains(buf.String(), "histogramWriter") {
		t.Fatalf("expected error log from histogramWriter, got: %q", buf.String())
	}
}
