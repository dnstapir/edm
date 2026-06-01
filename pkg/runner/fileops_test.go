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

// errInjected is the sentinel failure injected through the file-op seams so
// the error-path assertions can confirm (via errors.Is) that the injected
// failure is the one surfaced, rather than some unrelated error.
var errInjected = errors.New("injected failure")

// discardEDM returns a minimal minimiser with a no-op logger. It intentionally
// only sets the logger, which is all the file-operation helpers under test
// touch; it deliberately does not go through newTestDnstapMinimiser.
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
		swapSeam(t, &osMkdirAll, func(string, os.FileMode) error { return errInjected })
		dst := filepath.Join(t.TempDir(), "missing", "out.parquet")
		_, err := edm.createFile(dst)
		if !errors.Is(err, errInjected) {
			t.Fatalf("createFile error = %v, want %v", err, errInjected)
		}
	})

	t.Run("non-ENOENT create error is reported", func(t *testing.T) {
		edm := discardEDM()
		swapSeam(t, &osCreate, func(string) (*os.File, error) { return nil, errInjected })
		_, err := edm.createFile(filepath.Join(t.TempDir(), "out.parquet"))
		if !errors.Is(err, errInjected) {
			t.Fatalf("createFile error = %v, want %v", err, errInjected)
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
		// A real FileInfo (not (nil,nil)) so the stub matches os.Stat's
		// contract; osStat returning a nil error breaks the retry loop and
		// makes renameFile surface the rename error (here fs.ErrNotExist).
		info, statErr := os.Stat(t.TempDir())
		if statErr != nil {
			t.Fatal(statErr)
		}
		swapSeam(t, &osRename, func(string, string) error { return fs.ErrNotExist })
		swapSeam(t, &osStat, func(string) (os.FileInfo, error) { return info, nil })
		err := edm.renameFile("src", "dst")
		if !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("renameFile error = %v, want fs.ErrNotExist", err)
		}
	})

	t.Run("stat error is reported", func(t *testing.T) {
		edm := discardEDM()
		swapSeam(t, &osRename, func(string, string) error { return fs.ErrNotExist })
		swapSeam(t, &osStat, func(string) (os.FileInfo, error) { return nil, errInjected })
		err := edm.renameFile("src", "dst")
		if !errors.Is(err, errInjected) {
			t.Fatalf("renameFile error = %v, want %v", err, errInjected)
		}
	})

	t.Run("mkdir failure is reported", func(t *testing.T) {
		edm := discardEDM()
		swapSeam(t, &osRename, func(string, string) error { return fs.ErrNotExist })
		swapSeam(t, &osStat, func(string) (os.FileInfo, error) { return nil, fs.ErrNotExist })
		swapSeam(t, &osMkdirAll, func(string, os.FileMode) error { return errInjected })
		err := edm.renameFile("src", "dst")
		if !errors.Is(err, errInjected) {
			t.Fatalf("renameFile error = %v, want %v", err, errInjected)
		}
	})

	t.Run("non-ENOENT rename error is reported", func(t *testing.T) {
		edm := discardEDM()
		swapSeam(t, &osRename, func(string, string) error { return errInjected })
		err := edm.renameFile("src", "dst")
		if !errors.Is(err, errInjected) {
			t.Fatalf("renameFile error = %v, want %v", err, errInjected)
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

	swapSeam(t, &osCreate, func(string) (*os.File, error) { return nil, errInjected })

	edm.sessionWriterCh <- &prevSessions{rotationTime: time.Now()}
	close(edm.sessionWriterCh)

	var wg sync.WaitGroup
	wg.Add(1)
	go edm.sessionWriter(t.TempDir(), &wg)
	// waitForWaitGroup blocks until wg.Done(), establishing happens-before for
	// the buffer read below (the worker's last write precedes its Done()).
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

	swapSeam(t, &osCreate, func(string) (*os.File, error) { return nil, errInjected })

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
