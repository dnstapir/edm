package runner

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeDawg builds a DAWG containing domains and saves it to path, overwriting
// any existing file.
func writeDawg(t *testing.T, path string, domains ...string) {
	t.Helper()
	finder := testDawgFinder(t, domains...)
	if _, err := finder.Save(path); err != nil {
		t.Fatalf("save dawg: %s", err)
	}
	if err := finder.Close(); err != nil {
		t.Fatalf("close dawg: %s", err)
	}
}

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
	staged := filepath.Join(edm.getConfig().DataDir, dawgStagingDirName, stagedDawgName(src))
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

// TestLoadDawgFileStagedVersionTagTracksSource pins that the returned modtime
// (the dawgModTime version stamp) follows the source across reloads, so a new
// source version produces a new tag and in-flight histogram updates built
// against the old DAWG are discarded.
func TestLoadDawgFileStagedVersionTagTracksSource(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	src := filepath.Join(t.TempDir(), "wkd.dawg")

	writeDawg(t, src, "a.example.")
	t1 := time.Date(2021, 5, 6, 7, 8, 9, 0, time.UTC)
	if err := os.Chtimes(src, t1, t1); err != nil {
		t.Fatal(err)
	}
	_, mt1, err := edm.loadDawgFileStaged(src)
	if err != nil {
		t.Fatalf("first load: %s", err)
	}
	if !mt1.Equal(t1) {
		t.Fatalf("modTime = %v, want %v", mt1, t1)
	}

	writeDawg(t, src, "b.example.")
	t2 := time.Date(2022, 9, 10, 11, 12, 13, 0, time.UTC)
	if err := os.Chtimes(src, t2, t2); err != nil {
		t.Fatal(err)
	}
	_, mt2, err := edm.loadDawgFileStaged(src)
	if err != nil {
		t.Fatalf("second load: %s", err)
	}
	if !mt2.Equal(t2) {
		t.Fatalf("modTime = %v, want %v", mt2, t2)
	}
	if mt1.Equal(mt2) {
		t.Fatal("version tag did not change when the source mtime changed")
	}
}

// TestLoadDawgFileStagedSurvivesRestage pins the rename-over safety property:
// a finder mapped from one staged copy keeps working after a later reload
// re-stages a new version over the same canonical path.
func TestLoadDawgFileStagedSurvivesRestage(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	src := filepath.Join(t.TempDir(), "wkd.dawg")

	writeDawg(t, src, "a.example.")
	finderA, _, err := edm.loadDawgFileStaged(src)
	if err != nil {
		t.Fatalf("load A: %s", err)
	}

	writeDawg(t, src, "b.example.")
	finderB, _, err := edm.loadDawgFileStaged(src)
	if err != nil {
		t.Fatalf("load B: %s", err)
	}

	if finderA.IndexOf("a.example.") == dawgNotFound {
		t.Fatal("finder A broke after re-staging; rename-over did not preserve the old inode")
	}
	if finderA.IndexOf("b.example.") != dawgNotFound {
		t.Fatal("finder A unexpectedly sees the new content")
	}
	if finderB.IndexOf("b.example.") == dawgNotFound {
		t.Fatal("finder B missing the new content")
	}
}

// TestLoadDawgFileStagedCorruptSource verifies a corrupt (non-empty,
// unparseable) source fails the load gracefully instead of crashing: the DAWG
// library panics on malformed input, and loadStagedFinder must turn that into
// an error.
func TestLoadDawgFileStagedCorruptSource(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	src := writeTempFile(t, "corrupt.dawg", []byte("bad"))

	_, _, err := edm.loadDawgFileStaged(src)
	if err == nil {
		t.Fatal("expected an error loading a corrupt DAWG")
	}
	if errors.Is(err, errEmptyDawgFile) {
		t.Fatalf("got errEmptyDawgFile for a non-empty corrupt file: %v", err)
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

func TestStageDawgCopyPreservesMtimeAndContent(t *testing.T) {
	src := writeTempFile(t, "src.dawg", []byte("dawg-bytes-123"))
	want := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := os.Chtimes(src, want, want); err != nil {
		t.Fatal(err)
	}

	staged, modTime, err := stageDawgCopy(src, t.TempDir())
	if err != nil {
		t.Fatalf("stageDawgCopy: %s", err)
	}
	if !modTime.Equal(want) {
		t.Fatalf("returned modTime = %v, want %v", modTime, want)
	}
	if base := filepath.Base(staged); !strings.HasSuffix(base, "-src.dawg") {
		t.Fatalf("staged base = %q, want a hash-prefixed -src.dawg name", base)
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

// TestStageDawgCopyReplacesPrevious checks that re-staging the same source
// reuses the stable canonical path and replaces its content, so a reader of
// the previous copy keeps a stable file (the rename unlinks the old inode
// rather than truncating it) and no incoming temp files are left behind.
func TestStageDawgCopyReplacesPrevious(t *testing.T) {
	stagingDir := t.TempDir()
	src := filepath.Join(t.TempDir(), "x.dawg")

	if err := os.WriteFile(src, []byte("AAA"), 0o600); err != nil {
		t.Fatal(err)
	}
	stagedA, _, err := stageDawgCopy(src, stagingDir)
	if err != nil {
		t.Fatalf("stageDawgCopy A: %s", err)
	}

	if err := os.WriteFile(src, []byte("BBBB"), 0o600); err != nil {
		t.Fatal(err)
	}
	stagedB, _, err := stageDawgCopy(src, stagingDir)
	if err != nil {
		t.Fatalf("stageDawgCopy B: %s", err)
	}

	if stagedA != stagedB {
		t.Fatalf("staged path changed for the same source: %q then %q", stagedA, stagedB)
	}
	got, err := os.ReadFile(stagedB)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "BBBB" {
		t.Fatalf("canonical content = %q, want BBBB", got)
	}
	entries, err := os.ReadDir(stagingDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("staging dir has %d entries, want 1 (the canonical copy)", len(entries))
	}
}

// TestStageDawgCopyNoCollisionOnSameBaseName guards the fix for two configured
// sources that share a base name: they must stage to distinct files and not
// clobber each other.
func TestStageDawgCopyNoCollisionOnSameBaseName(t *testing.T) {
	stagingDir := t.TempDir()
	srcA := filepath.Join(t.TempDir(), "x.dawg")
	srcB := filepath.Join(t.TempDir(), "x.dawg")
	if err := os.WriteFile(srcA, []byte("AAA"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(srcB, []byte("BBBB"), 0o600); err != nil {
		t.Fatal(err)
	}

	stagedA, _, err := stageDawgCopy(srcA, stagingDir)
	if err != nil {
		t.Fatalf("stageDawgCopy A: %s", err)
	}
	stagedB, _, err := stageDawgCopy(srcB, stagingDir)
	if err != nil {
		t.Fatalf("stageDawgCopy B: %s", err)
	}

	if stagedA == stagedB {
		t.Fatalf("same-base-name sources collided on %q", stagedA)
	}
	gotA, err := os.ReadFile(stagedA)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotA) != "AAA" {
		t.Fatalf("staged A = %q, want AAA (clobbered?)", gotA)
	}
	gotB, err := os.ReadFile(stagedB)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotB) != "BBBB" {
		t.Fatalf("staged B = %q, want BBBB", gotB)
	}
}

// TestStageDawgCopyMkdirError exercises the staging-directory creation failure
// path: a staging dir whose parent is a regular file cannot be created.
func TestStageDawgCopyMkdirError(t *testing.T) {
	notADir := writeTempFile(t, "file", []byte("x"))
	src := writeTempFile(t, "src.dawg", []byte("data"))

	if _, _, err := stageDawgCopy(src, filepath.Join(notADir, "staging")); err == nil {
		t.Fatal("expected an error when the staging dir parent is a file")
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
