package runner

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestLoadDawgFileStagedPrivateMapping is the core guarantee of staging: the
// loaded finder is backed by a private copy, so overwriting the source in
// place afterwards cannot affect it. It also checks the returned modification
// time is the source's (the DAWG version stamp) and that the staged copy
// carries that timestamp.
func TestLoadDawgFileStagedPrivateMapping(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	src := testDawgFile(t, "example.com.")

	srcInfo, err := os.Stat(src)
	if err != nil {
		t.Fatal(err)
	}

	finder, modTime, err := edm.loadDawgFileStaged(src)
	if err != nil {
		t.Fatalf("loadDawgFileStaged: %s", err)
	}
	if finder.IndexOf("example.com.") == dawgNotFound {
		t.Fatal("expected example.com. in the loaded DAWG")
	}
	if !modTime.Equal(srcInfo.ModTime()) {
		t.Fatalf("modTime = %v, want source mtime %v", modTime, srcInfo.ModTime())
	}

	// The staged copy lives under data-dir and carries the source mtime.
	staged := filepath.Join(edm.getConfig().DataDir, dawgStagingDirName, filepath.Base(src))
	stagedInfo, err := os.Stat(staged)
	if err != nil {
		t.Fatalf("staged copy missing: %s", err)
	}
	if !stagedInfo.ModTime().Equal(srcInfo.ModTime()) {
		t.Fatalf("staged mtime = %v, want %v", stagedInfo.ModTime(), srcInfo.ModTime())
	}

	// Overwrite the source in place. The finder is mapped from the private
	// copy, so it must keep working; with a direct mmap of the source this
	// would corrupt or crash in-flight lookups.
	if err := os.WriteFile(src, []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if finder.IndexOf("example.com.") == dawgNotFound {
		t.Fatal("finder broke after the source was overwritten in place; mapping is not private")
	}
}

func TestStageDawgCopyPreservesMtimeAndContent(t *testing.T) {
	src := writeTempFile(t, "src.dawg", []byte("dawg-bytes-123"))
	want := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := os.Chtimes(src, want, want); err != nil {
		t.Fatal(err)
	}

	staged, err := stageDawgCopy(src, t.TempDir())
	if err != nil {
		t.Fatalf("stageDawgCopy: %s", err)
	}
	if got := filepath.Base(staged); got != "src.dawg" {
		t.Fatalf("staged base = %q, want src.dawg", got)
	}
	got, err := os.ReadFile(staged)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "dawg-bytes-123" {
		t.Fatalf("staged content = %q", got)
	}
	info, err := os.Stat(staged)
	if err != nil {
		t.Fatal(err)
	}
	if !info.ModTime().Equal(want) {
		t.Fatalf("staged mtime = %v, want %v", info.ModTime(), want)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("staged perm = %o, want 600", perm)
	}
}

// TestStageDawgCopyReplacesPrevious checks that re-staging a file with the same
// base name reuses the stable canonical path and replaces its content, so a
// reader of the previous copy keeps a stable file (the rename unlinks the old
// inode rather than truncating it).
func TestStageDawgCopyReplacesPrevious(t *testing.T) {
	stagingDir := t.TempDir()
	srcA := writeTempFile(t, "x.dawg", []byte("AAA"))
	stagedA, err := stageDawgCopy(srcA, stagingDir)
	if err != nil {
		t.Fatalf("stageDawgCopy A: %s", err)
	}

	srcB := writeTempFile(t, "x.dawg", []byte("BBBB"))
	stagedB, err := stageDawgCopy(srcB, stagingDir)
	if err != nil {
		t.Fatalf("stageDawgCopy B: %s", err)
	}

	if stagedA != stagedB {
		t.Fatalf("staged path changed across reloads: %q then %q, want a stable canonical name", stagedA, stagedB)
	}
	got, err := os.ReadFile(stagedB)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "BBBB" {
		t.Fatalf("canonical content = %q, want BBBB", got)
	}

	// No incoming temp files left behind.
	entries, err := os.ReadDir(stagingDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("staging dir has %d entries, want 1 (the canonical copy)", len(entries))
	}
}

func TestLoadDawgFileStagedEmptySource(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	src := writeTempFile(t, "empty.dawg", nil)
	if _, _, err := edm.loadDawgFileStaged(src); !errors.Is(err, errEmptyDawgFile) {
		t.Fatalf("loadDawgFileStaged = %v, want errEmptyDawgFile", err)
	}
}

func TestLoadDawgFileStagedMissingSource(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	missing := filepath.Join(t.TempDir(), "missing.dawg")
	if _, _, err := edm.loadDawgFileStaged(missing); err == nil {
		t.Fatal("loadDawgFileStaged succeeded for a missing source, want error")
	}
}

func TestPrepareDawgStaging(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	stagingDir := filepath.Join(edm.getConfig().DataDir, dawgStagingDirName)
	if err := os.MkdirAll(stagingDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stagingDir, "orphan"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := edm.prepareDawgStaging(); err != nil {
		t.Fatalf("prepareDawgStaging: %s", err)
	}
	if _, err := os.Stat(stagingDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staging dir still present after prepare: %v", err)
	}
}
