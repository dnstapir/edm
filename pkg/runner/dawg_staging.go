package runner

import (
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
// which decouples the live mapping from later changes to srcPath. The operator
// still delivers srcPath atomically (write a temp file and rename it into the
// configured path) before sending SIGHUP.
//
// The returned time is srcPath's modification time, not the staged copy's. It
// tags the DAWG version that in-flight histogram updates are matched against
// (the data collector discards updates whose dawgModTime no longer matches the
// active snapshot), so it must reflect the source rather than the moment the
// copy was made.
func (edm *DnstapMinimiser) loadDawgFileStaged(srcPath string) (finder dawg.Finder, modTime time.Time, err error) {
	srcInfo, err := edm.deps.FileSystem.Stat(srcPath)
	if err != nil {
		return nil, time.Time{}, err
	}
	if srcInfo.Size() == 0 {
		return nil, time.Time{}, errEmptyDawgFile
	}

	stagingDir := filepath.Join(edm.getConfig().DataDir, dawgStagingDirName)
	staged, err := stageDawgCopy(srcPath, stagingDir)
	if err != nil {
		return nil, time.Time{}, err
	}

	// LoadDawgFile mmaps the staged copy. The previous copy at the same path
	// was unlinked by stageDawgCopy's rename and is kept alive by the prior
	// mapping until the GC reclaims it (see the ignoredQuestions field comment
	// on DnstapMinimiser).
	finder, _, err = edm.deps.DawgLoader.LoadDawgFile(staged)
	if err != nil {
		return nil, time.Time{}, err
	}
	return finder, srcInfo.ModTime(), nil
}

// stageDawgCopy copies srcPath into stagingDir under srcPath's base name and
// returns the staged path.
//
// The copy is written to a temporary file in stagingDir and atomically renamed
// over any existing staged copy, so a reader mapping the previous copy keeps a
// stable file: the rename unlinks the old inode rather than truncating it. The
// staged copy carries the source's modification time so it is unsurprising to
// inspect and preserves the DAWG version stamp.
func stageDawgCopy(srcPath, stagingDir string) (stagedPath string, err error) {
	if err = os.MkdirAll(stagingDir, 0o700); err != nil {
		return "", fmt.Errorf("stageDawgCopy: %w", err)
	}

	in, err := os.Open(srcPath) // #nosec G304 -- configured runtime DAWG path
	if err != nil {
		return "", fmt.Errorf("stageDawgCopy: %w", err)
	}
	defer func() { _ = in.Close() }()

	srcInfo, err := in.Stat()
	if err != nil {
		return "", fmt.Errorf("stageDawgCopy: %w", err)
	}

	base := filepath.Base(srcPath)
	tmp, err := os.CreateTemp(stagingDir, base+".incoming-*")
	if err != nil {
		return "", fmt.Errorf("stageDawgCopy: %w", err)
	}
	tmpName := tmp.Name()

	if _, err = io.Copy(tmp, in); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return "", fmt.Errorf("stageDawgCopy: %w", err)
	}
	if err = tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return "", fmt.Errorf("stageDawgCopy: %w", err)
	}

	// Preserve the source modification time on the copy.
	mtime := srcInfo.ModTime()
	if err = os.Chtimes(tmpName, mtime, mtime); err != nil {
		_ = os.Remove(tmpName)
		return "", fmt.Errorf("stageDawgCopy: %w", err)
	}

	stagedPath = filepath.Join(stagingDir, base)
	if err = os.Rename(tmpName, stagedPath); err != nil {
		_ = os.Remove(tmpName)
		return "", fmt.Errorf("stageDawgCopy: %w", err)
	}
	return stagedPath, nil
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
