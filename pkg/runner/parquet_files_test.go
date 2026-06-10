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
	"testing"
	"time"
)

func TestTimestampsFromFilenameRejectsMalformedNames(t *testing.T) {
	tests := []string{
		"dns_histogram.parquet",
		"dns_histogram-2026-04-30T12-00-00Z.parquet",
		"dns_histogram-bad_2026-04-30T12-01-00Z.parquet",
		"dns_histogram-2026-04-30T12-00-00Z_bad.parquet",
	}

	for _, name := range tests {
		t.Run(name, func(t *testing.T) {
			if _, _, err := timestampsFromFilename(name); err == nil {
				t.Fatal("timestampsFromFilename returned nil error")
			}
		})
	}
}

func TestFileAndFilenameHelpers(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	base := t.TempDir()
	start := time.Date(2026, 5, 28, 12, 0, 0, 0, time.FixedZone("test", 2*60*60))
	stop := start.Add(time.Minute)

	tmpName, finalName := buildParquetFilenames(base, "dns_histogram", start, stop)
	if !strings.HasSuffix(tmpName, ".parquet.tmp") || !strings.HasSuffix(finalName, ".parquet") {
		t.Fatalf("unexpected filenames: %q %q", tmpName, finalName)
	}
	if timestampToFileString(start.UTC()) != "2026-05-28T10-00-00Z" {
		t.Fatalf("unexpected timestamp string: %s", timestampToFileString(start.UTC()))
	}
	if got := getStartTimeFromRotationTime(stop); !got.Equal(start) {
		t.Fatalf("start time = %v, want %v", got, start)
	}

	parsedStart, parsedStop, err := timestampsFromFilename(filepath.Base(finalName))
	if err != nil {
		t.Fatal(err)
	}
	if !parsedStart.Equal(start.UTC()) || !parsedStop.Equal(stop.UTC()) {
		t.Fatalf("parsed times = %v %v", parsedStart, parsedStop)
	}
	if _, _, err := timestampsFromFilename("dns_histogram-bad_bad.parquet"); err == nil {
		t.Fatal("bad timestamp filename succeeded")
	}
	if _, _, err := timestampsFromFilename("dns_histogram-2026-05-28T10-00-00Z_bad.parquet"); err == nil {
		t.Fatal("bad stop timestamp filename succeeded")
	}

	out, err := edm.createFile(filepath.Join(base, "missing", "created.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	if err := edm.renameFile(out.Name(), filepath.Join(base, "sent", "created.txt")); err != nil {
		t.Fatal(err)
	}
	if _, err := edm.createFile(base); err == nil {
		t.Fatal("createFile on directory succeeded")
	}
	if err := edm.renameFile(filepath.Join(base, "nope"), filepath.Join(base, "dst")); err == nil {
		t.Fatal("rename missing source succeeded")
	}
}

func TestCreateSessionAndHistogramFiles(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	dataDir := t.TempDir()
	rotationTime := time.Date(2026, 5, 28, 12, 1, 0, 0, time.UTC)
	ps := &prevSessions{
		rotationTime: rotationTime,
		sessions: []*sessionData{{
			dnsLabels: dnsLabels{Label0: ptr("com")},
			ServerID:  ptr("server"),
		}},
	}
	sessionFile, err := edm.createSessionFile(ps, dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if strings.HasSuffix(sessionFile, ".tmp") {
		t.Fatalf("session file kept tmp suffix: %s", sessionFile)
	}

	finder := testDawgFinder(t, "example.com.")
	hd := edm.newHistogramData(getHllDefaults(0), false)
	wkd := &wellKnownDomainsData{rotationTime: rotationTime, dawgFinder: finder, m: map[int]*histogramData{0: hd}}
	histFile, err := edm.createHistogramFile(wkd, defaultLabelLimit, filepath.Join(dataDir, "parquet", "histograms", "outbox"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.HasSuffix(histFile, ".tmp") {
		t.Fatalf("histogram file kept tmp suffix: %s", histFile)
	}
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
