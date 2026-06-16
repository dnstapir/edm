package runner

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/smhanov/dawg"
)

// dawgStagingDirName is the subdirectory of the configured data-dir that holds
// EDM's private copies of the memory-mapped DAWG files.
const dawgStagingDirName = "dawg-staging"

// loadDawgFileStaged loads srcPath as a DAWG from a private copy kept on the
// data-dir filesystem, returning the [dawg.Finder] and srcPath's modification
// time.
//
// The DAWG files are memory-mapped, so EDM never maps the operator-supplied
// file directly: an in-place overwrite of a mapped file would corrupt or
// SIGBUS in-flight lookups that still reference the old mapping. Instead the
// source is copied into the staging directory and the private copy is mapped,
// which decouples the live mapping from later changes to srcPath.
//
// The returned time is srcPath's modification time, sampled once from the file
// that is actually copied. It tags the DAWG version that in-flight histogram
// updates are matched against (the data collector discards updates whose
// dawgModTime no longer matches the active snapshot), so it must reflect the
// source rather than the moment the copy was made.
func (edm *DnstapMinimiser) loadDawgFileStaged(srcPath string) (dawg.Finder, time.Time, error) {
	stagingDir := filepath.Join(edm.getConfig().DataDir, dawgStagingDirName)
	staged, modTime, err := stageDawgCopy(srcPath, stagingDir)
	if err != nil {
		return nil, time.Time{}, err
	}

	// The previous copy at the same path was unlinked by stageDawgCopy's
	// rename and is kept alive by the prior mapping until the GC reclaims it
	// (see the ignoredQuestions field comment on DnstapMinimiser).
	finder, err := loadStagedFinder(edm.deps.DawgLoader, staged)
	if err != nil {
		return nil, time.Time{}, err
	}
	return finder, modTime, nil
}

// loadStagedFinder mmaps the staged DAWG copy, converting a panic from the
// DAWG library into an error.
//
// [dawg.Load] panics rather than returning an error on a malformed file, so a
// corrupt or half-written source would otherwise crash the reload goroutine
// (and the process) instead of failing the one reload. Recovering keeps the
// previous DAWG mapped and the service running, matching the rest of the
// reload path's "log and keep old state" behavior.
func loadStagedFinder(loader dawgLoader, staged string) (finder dawg.Finder, err error) {
	defer func() {
		if r := recover(); r != nil {
			finder = nil
			err = fmt.Errorf("loadStagedFinder: loading DAWG %q panicked (corrupt file?): %v", staged, r)
		}
	}()
	finder, _, err = loader.LoadDawgFile(staged)
	return
}

// stagedDawgName returns a staging file name for srcPath that cannot collide
// with a different source sharing the same base name. A short hash of the
// cleaned path keeps distinct sources distinct, while the base name is kept as
// a readable suffix so the staging directory is unsurprising to inspect.
func stagedDawgName(srcPath string) string {
	sum := sha256.Sum256([]byte(filepath.Clean(srcPath)))
	return hex.EncodeToString(sum[:8]) + "-" + filepath.Base(srcPath)
}

// stageDawgCopy copies srcPath into stagingDir and returns the staged path and
// srcPath's modification time.
//
// The copy is written to a temporary file in stagingDir and atomically renamed
// over any existing staged copy of the same source, so a reader mapping the
// previous copy keeps a stable file: the rename unlinks the old inode rather
// than truncating it. The staged copy carries the source's modification time
// so it is unsurprising to inspect and preserves the DAWG version stamp. A
// zero-length source returns [errEmptyDawgFile] without staging anything.
func stageDawgCopy(srcPath, stagingDir string) (stagedPath string, modTime time.Time, err error) {
	if err = os.MkdirAll(stagingDir, 0o700); err != nil {
		return "", time.Time{}, fmt.Errorf("stageDawgCopy: %w", err)
	}

	in, err := os.Open(srcPath) // #nosec G304 -- configured runtime DAWG path
	if err != nil {
		return "", time.Time{}, fmt.Errorf("stageDawgCopy: %w", err)
	}
	defer func() { _ = in.Close() }()

	srcInfo, err := in.Stat()
	if err != nil {
		return "", time.Time{}, fmt.Errorf("stageDawgCopy: %w", err)
	}
	if srcInfo.Size() == 0 {
		return "", time.Time{}, errEmptyDawgFile
	}
	modTime = srcInfo.ModTime()

	name := stagedDawgName(srcPath)
	tmp, err := os.CreateTemp(stagingDir, name+".incoming-*")
	if err != nil {
		return "", time.Time{}, fmt.Errorf("stageDawgCopy: %w", err)
	}
	tmpName := tmp.Name()

	if _, err = io.Copy(tmp, in); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return "", time.Time{}, fmt.Errorf("stageDawgCopy: %w", err)
	}
	if err = tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return "", time.Time{}, fmt.Errorf("stageDawgCopy: %w", err)
	}

	// Carry the source modification time onto the copy.
	if err = os.Chtimes(tmpName, modTime, modTime); err != nil {
		_ = os.Remove(tmpName)
		return "", time.Time{}, fmt.Errorf("stageDawgCopy: %w", err)
	}

	stagedPath = filepath.Join(stagingDir, name)
	if err = os.Rename(tmpName, stagedPath); err != nil {
		_ = os.Remove(tmpName)
		return "", time.Time{}, fmt.Errorf("stageDawgCopy: %w", err)
	}
	return stagedPath, modTime, nil
}

// prepareDawgStaging clears the DAWG staging directory.
//
// It runs once at startup, before any DAWG is staged. A fresh process holds no
// mappings into the directory, so removing it reclaims copies orphaned by a
// previous process that exited before its mappings were released.
func (edm *DnstapMinimiser) prepareDawgStaging() error {
	stagingDir := filepath.Join(edm.getConfig().DataDir, dawgStagingDirName)
	if err := os.RemoveAll(stagingDir); err != nil {
		return fmt.Errorf("prepareDawgStaging: %w", err)
	}
	return nil
}
