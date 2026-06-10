package runner

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"time"
)

// writeRotatedParquet creates tmpName via createFile, invokes write to populate
// it, then atomically renames it to finalName. On any failure between create
// and rename the temp file is removed. label disambiguates concurrent rotations
// in log lines (e.g. "session" or "histogram"). Returns finalName on success.
func (edm *DnstapMinimiser) writeRotatedParquet(label, tmpName, finalName string, write func(io.Writer) error) (string, error) {
	edm.log.Info("writing out "+label+" file", "filename", tmpName)

	outFile, err := edm.createFile(tmpName)
	if err != nil {
		return "", fmt.Errorf("unable to open %s file: %w", label, err)
	}
	fileOpen := true
	writeFailed := false
	defer func() {
		// Closing a *os.File twice returns an error, so only do it if
		// we have not already tried to close it.
		if fileOpen {
			if err := outFile.Close(); err != nil {
				edm.log.Error("unable to do deferred close of "+label+" outFile", "error", err)
			}
		}
		if writeFailed {
			edm.log.Info("cleaning up "+label+" file because write failed", "filename", outFile.Name())
			if err := edm.deps.FileSystem.Remove(outFile.Name()); err != nil {
				edm.log.Error("unable to remove "+label+" outFile", "error", err, "filename", outFile.Name())
			}
		}
	}()

	if err := write(outFile); err != nil {
		writeFailed = true
		return "", fmt.Errorf("writing parquet data failed: %w", err)
	}

	// We need to close the file before renaming it.
	err = outFile.Close()
	// At this point we do not want the defer to close the file for us when returning.
	fileOpen = false
	if err != nil {
		writeFailed = true
		return "", fmt.Errorf("unable to call Close() on file: %w", err)
	}

	// Atomically rename the file to its real name so it can be picked up downstream.
	edm.log.Info("renaming "+label+" file", "from", tmpName, "to", finalName)
	if err := edm.deps.FileSystem.Rename(tmpName, finalName); err != nil {
		return "", fmt.Errorf("unable to rename output file: %w", err)
	}
	return finalName, nil
}

func (edm *DnstapMinimiser) renameFile(src string, dst string) error {
	dstDir := filepath.Dir(dst)

	// We are prepared for the destination directory not existing and will
	// create it if needed and retry the rename in this case.
	for {
		err := edm.deps.FileSystem.Rename(src, dst)
		if err == nil {
			// Rename went well, we are done
			return nil
		}

		if errors.Is(err, fs.ErrNotExist) {
			if _, statErr := edm.deps.FileSystem.Stat(dstDir); statErr == nil {
				return fmt.Errorf("renameFile: unable to rename file, src: %s, dst: %s: %w", src, dst, err)
			} else if !errors.Is(statErr, fs.ErrNotExist) {
				return fmt.Errorf("renameFile: unable to stat destination dir: %s: %w", dstDir, statErr)
			}
			// If the destination directory does not exist we will
			// need to create it and then retry the Rename() in the
			// next iteration of the loop.
			err = edm.deps.FileSystem.MkdirAll(dstDir, 0o750)
			if err != nil {
				return fmt.Errorf("renameFile: unable to create destination dir: %s: %w", dstDir, err)
			}
			edm.log.Info("renameFile: created directory", "dir", dstDir)
		} else {
			// Some other error occured
			return fmt.Errorf("renameFile: unable to rename file, src: %s, dst: %s: %w", src, dst, err)
		}
	}
}

func (edm *DnstapMinimiser) createFile(dst string) (File, error) {
	dstDir := filepath.Dir(dst)

	// Make gosec happy
	dst = filepath.Clean(dst)

	// We are prepared for the destination directory not existing and will
	// create it if needed and retry the creation in this case.
	for {
		outFile, err := edm.deps.FileSystem.Create(dst)
		if err == nil {
			// Creation went well, we are done
			return outFile, nil
		}

		if errors.Is(err, fs.ErrNotExist) {
			// If the destination directory does not exist we will
			// need to create it and then retry the file Create()
			// the next iteration of the loop.
			err = edm.deps.FileSystem.MkdirAll(dstDir, 0o750)
			if err != nil {
				return nil, fmt.Errorf("createFile: unable to create destination dir: %s: %w", dstDir, err)
			}
			edm.log.Info("createFile: created directory", "dir", dstDir)
		} else {
			// Some other error occured
			return nil, fmt.Errorf("createFile: unable to create file, dst: %s: %w", dst, err)
		}
	}
}

func timestampsFromFilename(name string) (startTime time.Time, stopTime time.Time, err error) {
	// expected name format: dns_histogram-2023-11-29T13-50-00Z_2023-11-29T13-51-00Z.parquet
	trimmedName := strings.TrimSuffix(name, ".parquet")
	nameParts := strings.SplitN(trimmedName, "-", 2)
	if len(nameParts) != 2 {
		err = fmt.Errorf("timestampFromFilename: missing '-' separating prefix from timestamps in %q", name)
		return
	}
	times := strings.Split(nameParts[1], "_")
	if len(times) != 2 {
		err = fmt.Errorf("timestampFromFilename: missing '_' separating start and stop timestamps in %q", name)
		return
	}
	startTime, err = time.Parse("2006-01-02T15-04-05Z07:00", times[0])
	if err != nil {
		err = fmt.Errorf("timestampFromFilename: unable to parse startTime: %w", err)
		return
	}
	stopTime, err = time.Parse("2006-01-02T15-04-05Z07:00", times[1])
	if err != nil {
		err = fmt.Errorf("timestampFromFilename: unable to parse stopTime: %w", err)
		return
	}
	return
}

func buildParquetFilenames(baseDir string, baseName string, timeStart time.Time, timeStop time.Time) (string, string) {
	// Use timestamp for files, replace ":" with "-" to not have to escape
	// characters in the shell, e.g: 2009-11-10T23-00-00Z
	startTS := timestampToFileString(timeStart.UTC())
	stopTS := timestampToFileString(timeStop.UTC())
	fileName := fmt.Sprintf("%s-%s_%s.parquet", baseName, startTS, stopTS)

	// Write output to a .tmp file so we can atomically rename it to the real
	// name when the file has been written in full
	tmpFileName := fileName + ".tmp"

	absoluteFileName := filepath.Join(baseDir, fileName)
	absoluteTmpFileName := filepath.Join(baseDir, tmpFileName)

	return absoluteTmpFileName, absoluteFileName
}

func timestampToFileString(ts time.Time) string {
	// Use timestamp for files, replace ":" with "-" to not have to escape
	// characters in the shell, e.g: 2009-11-10T23-00-00Z
	timeString := strings.ReplaceAll(ts.Format(time.RFC3339), ":", "-")

	return timeString
}
