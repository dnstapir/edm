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

// errInjected is the sentinel failure injected through fake dependencies so
// the error-path assertions can confirm (via errors.Is) that the injected
// failure is the one surfaced, rather than some unrelated error.
var errInjected = errors.New("injected failure")

// discardEDM returns a minimal minimiser with a no-op logger. It intentionally
// only sets the logger, which is all the file-operation helpers under test
// touch; it deliberately does not go through newTestDnstapMinimiser.
func discardEDM() *DnstapMinimiser {
	return &DnstapMinimiser{
		log:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		deps: defaultDependencies(),
	}
}

type faultingFileSystem struct {
	FileSystem
	create   func(string) (File, error)
	rename   func(string, string) error
	remove   func(string) error
	mkdirAll func(string, os.FileMode) error
	stat     func(string) (os.FileInfo, error)
	readDir  func(string) ([]os.DirEntry, error)
}

func (ffs faultingFileSystem) Create(name string) (File, error) {
	if ffs.create != nil {
		return ffs.create(name)
	}
	return ffs.FileSystem.Create(name)
}

func (ffs faultingFileSystem) Rename(oldpath, newpath string) error {
	if ffs.rename != nil {
		return ffs.rename(oldpath, newpath)
	}
	return ffs.FileSystem.Rename(oldpath, newpath)
}

func (ffs faultingFileSystem) Remove(name string) error {
	if ffs.remove != nil {
		return ffs.remove(name)
	}
	return ffs.FileSystem.Remove(name)
}

func (ffs faultingFileSystem) MkdirAll(path string, perm os.FileMode) error {
	if ffs.mkdirAll != nil {
		return ffs.mkdirAll(path, perm)
	}
	return ffs.FileSystem.MkdirAll(path, perm)
}

func (ffs faultingFileSystem) Stat(name string) (os.FileInfo, error) {
	if ffs.stat != nil {
		return ffs.stat(name)
	}
	return ffs.FileSystem.Stat(name)
}

func (ffs faultingFileSystem) ReadDir(name string) ([]os.DirEntry, error) {
	if ffs.readDir != nil {
		return ffs.readDir(name)
	}
	return ffs.FileSystem.ReadDir(name)
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
		edm.deps.FileSystem = faultingFileSystem{FileSystem: edm.deps.FileSystem, mkdirAll: func(string, os.FileMode) error { return errInjected }}
		dst := filepath.Join(t.TempDir(), "missing", "out.parquet")
		_, err := edm.createFile(dst)
		if !errors.Is(err, errInjected) {
			t.Fatalf("createFile error = %v, want %v", err, errInjected)
		}
	})

	t.Run("non-ENOENT create error is reported", func(t *testing.T) {
		edm := discardEDM()
		edm.deps.FileSystem = faultingFileSystem{FileSystem: edm.deps.FileSystem, create: func(string) (File, error) { return nil, errInjected }}
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
		// contract; Stat returning a nil error breaks the retry loop and
		// makes renameFile surface the rename error (here fs.ErrNotExist).
		info, statErr := os.Stat(t.TempDir())
		if statErr != nil {
			t.Fatal(statErr)
		}
		edm.deps.FileSystem = faultingFileSystem{
			FileSystem: edm.deps.FileSystem,
			rename:     func(string, string) error { return fs.ErrNotExist },
			stat:       func(string) (os.FileInfo, error) { return info, nil },
		}
		err := edm.renameFile("src", "dst")
		if !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("renameFile error = %v, want fs.ErrNotExist", err)
		}
	})

	t.Run("stat error is reported", func(t *testing.T) {
		edm := discardEDM()
		edm.deps.FileSystem = faultingFileSystem{
			FileSystem: edm.deps.FileSystem,
			rename:     func(string, string) error { return fs.ErrNotExist },
			stat:       func(string) (os.FileInfo, error) { return nil, errInjected },
		}
		err := edm.renameFile("src", "dst")
		if !errors.Is(err, errInjected) {
			t.Fatalf("renameFile error = %v, want %v", err, errInjected)
		}
	})

	t.Run("mkdir failure is reported", func(t *testing.T) {
		edm := discardEDM()
		edm.deps.FileSystem = faultingFileSystem{
			FileSystem: edm.deps.FileSystem,
			rename:     func(string, string) error { return fs.ErrNotExist },
			stat:       func(string) (os.FileInfo, error) { return nil, fs.ErrNotExist },
			mkdirAll:   func(string, os.FileMode) error { return errInjected },
		}
		err := edm.renameFile("src", "dst")
		if !errors.Is(err, errInjected) {
			t.Fatalf("renameFile error = %v, want %v", err, errInjected)
		}
	})

	t.Run("non-ENOENT rename error is reported", func(t *testing.T) {
		edm := discardEDM()
		edm.deps.FileSystem = faultingFileSystem{FileSystem: edm.deps.FileSystem, rename: func(string, string) error { return errInjected }}
		err := edm.renameFile("src", "dst")
		if !errors.Is(err, errInjected) {
			t.Fatalf("renameFile error = %v, want %v", err, errInjected)
		}
	})
}

func TestWriteRotatedParquet(t *testing.T) {
	t.Run("happy", func(t *testing.T) {
		edm := discardEDM()
		dir := t.TempDir()
		tmp := filepath.Join(dir, "data.tmp")
		final := filepath.Join(dir, "data")

		name, err := edm.writeRotatedParquet("test", tmp, final, func(w io.Writer) error {
			_, err := w.Write([]byte("hello"))
			return err
		})
		if err != nil {
			t.Fatalf("writeRotatedParquet: %v", err)
		}
		if name != final {
			t.Fatalf("name = %q, want %q", name, final)
		}
		if _, err := os.Stat(tmp); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("tmp file should be gone after rename, stat err = %v", err)
		}
		data, err := os.ReadFile(final)
		if err != nil {
			t.Fatalf("read final: %v", err)
		}
		if string(data) != "hello" {
			t.Fatalf("contents = %q, want %q", string(data), "hello")
		}
	})

	t.Run("createFile error", func(t *testing.T) {
		edm := discardEDM()
		edm.deps.FileSystem = faultingFileSystem{FileSystem: edm.deps.FileSystem, create: func(string) (File, error) { return nil, errInjected }}
		_, err := edm.writeRotatedParquet("test", filepath.Join(t.TempDir(), "x"), "y", func(io.Writer) error {
			return nil
		})
		if !errors.Is(err, errInjected) {
			t.Fatalf("error = %v, want %v", err, errInjected)
		}
	})

	t.Run("write error removes temp file", func(t *testing.T) {
		edm := discardEDM()
		dir := t.TempDir()
		tmp := filepath.Join(dir, "data.tmp")
		final := filepath.Join(dir, "data")

		_, err := edm.writeRotatedParquet("test", tmp, final, func(io.Writer) error {
			return errInjected
		})
		if !errors.Is(err, errInjected) {
			t.Fatalf("error = %v, want %v", err, errInjected)
		}
		if _, err := os.Stat(tmp); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("tmp file should be removed after write error, stat err = %v", err)
		}
		if _, err := os.Stat(final); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("final file should not exist, stat err = %v", err)
		}
	})

	t.Run("write error and remove failure is logged", func(t *testing.T) {
		edm := discardEDM()
		var buf bytes.Buffer
		edm.log = slog.New(slog.NewJSONHandler(&buf, nil))

		dir := t.TempDir()
		tmp := filepath.Join(dir, "data.tmp")
		final := filepath.Join(dir, "data")

		edm.deps.FileSystem = faultingFileSystem{FileSystem: edm.deps.FileSystem, remove: func(string) error { return errInjected }}

		_, err := edm.writeRotatedParquet("test", tmp, final, func(io.Writer) error {
			return errInjected
		})
		if !errors.Is(err, errInjected) {
			t.Fatalf("error = %v, want %v", err, errInjected)
		}
		if !strings.Contains(buf.String(), "unable to remove test outFile") {
			t.Fatalf("expected remove failure log, got: %q", buf.String())
		}
	})

	t.Run("explicit Close error after successful write", func(t *testing.T) {
		edm := discardEDM()
		dir := t.TempDir()
		tmp := filepath.Join(dir, "data.tmp")
		final := filepath.Join(dir, "data")

		// Closing outFile inside the write closure makes the helper's
		// explicit Close() fail with os.ErrClosed.
		_, err := edm.writeRotatedParquet("test", tmp, final, func(w io.Writer) error {
			return w.(*os.File).Close()
		})
		if !errors.Is(err, os.ErrClosed) {
			t.Fatalf("error = %v, want os.ErrClosed", err)
		}
		// Close-error path sets writeFailed, so the temp file is removed.
		if _, err := os.Stat(tmp); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("tmp file should be removed, stat err = %v", err)
		}
	})

	t.Run("deferred Close error is logged", func(t *testing.T) {
		edm := discardEDM()
		var buf bytes.Buffer
		edm.log = slog.New(slog.NewJSONHandler(&buf, nil))

		dir := t.TempDir()
		tmp := filepath.Join(dir, "data.tmp")
		final := filepath.Join(dir, "data")

		// Closing outFile inside the write closure AND returning an error
		// causes the deferred Close() (which only runs when fileOpen=true)
		// to fail with os.ErrClosed.
		_, err := edm.writeRotatedParquet("test", tmp, final, func(w io.Writer) error {
			_ = w.(*os.File).Close()
			return errInjected
		})
		if !errors.Is(err, errInjected) {
			t.Fatalf("error = %v, want %v", err, errInjected)
		}
		if !strings.Contains(buf.String(), "unable to do deferred close of test outFile") {
			t.Fatalf("expected deferred close failure log, got: %q", buf.String())
		}
	})

	t.Run("rename error leaves temp file", func(t *testing.T) {
		edm := discardEDM()
		dir := t.TempDir()
		tmp := filepath.Join(dir, "data.tmp")
		final := filepath.Join(dir, "data")

		edm.deps.FileSystem = faultingFileSystem{FileSystem: edm.deps.FileSystem, rename: func(string, string) error { return errInjected }}

		_, err := edm.writeRotatedParquet("test", tmp, final, func(w io.Writer) error {
			_, err := w.Write([]byte("hello"))
			return err
		})
		if !errors.Is(err, errInjected) {
			t.Fatalf("error = %v, want %v", err, errInjected)
		}
		// Rename failure intentionally leaves the temp file in place.
		if _, err := os.Stat(tmp); err != nil {
			t.Fatalf("tmp file should remain after rename error, stat err = %v", err)
		}
		if _, err := os.Stat(final); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("final file should not exist, stat err = %v", err)
		}
	})
}

// TestSessionWriterLogsCreateError verifies the sessionWriter worker logs and
// keeps running when createSessionFile fails. The failure is injected through
// FileSystem.Create so writeSessionParquet is never reached.
func TestSessionWriterLogsCreateError(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	var buf bytes.Buffer
	edm.log = slog.New(slog.NewJSONHandler(&buf, nil))

	edm.deps.FileSystem = faultingFileSystem{FileSystem: edm.deps.FileSystem, create: func(string) (File, error) { return nil, errInjected }}

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

	edm.deps.FileSystem = faultingFileSystem{FileSystem: edm.deps.FileSystem, create: func(string) (File, error) { return nil, errInjected }}

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
