package runner

import (
	"context"
	"errors"
	"io/fs"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const sentHistogramRetention = 24 * time.Hour

func sentHistogramExpired(modTime, now time.Time) bool {
	return now.Sub(modTime) > sentHistogramRetention
}

func (edm *DnstapMinimiser) diskCleaner(ctx context.Context, wg *sync.WaitGroup, sentDir string) {
	// We will scan the directory each tick for sent files to remove.
	defer wg.Done()

	ticker := edm.deps.Clock.NewTicker(edm.deps.DiskCleanerInterval)
	defer ticker.Stop()

timerLoop:
	for {
		select {
		case <-ticker.C():
			dirEntries, err := edm.deps.FileSystem.ReadDir(sentDir)
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					// The directory has not been created yet, this is OK
					continue
				}
				edm.log.Error("histogramSender: unable to read sent dir", "error", err)
				continue
			}
			for _, dirEntry := range dirEntries {
				if dirEntry.IsDir() {
					continue
				}
				if strings.HasPrefix(dirEntry.Name(), histogramFileBase+"-") && strings.HasSuffix(dirEntry.Name(), parquetFileSuffix) {
					fileInfo, err := dirEntry.Info()
					if err != nil {
						edm.log.Error("diskCleaner: unable to get fileInfo for filename", "error", err, "filename", dirEntry.Name())
						continue
					}

					if sentHistogramExpired(fileInfo.ModTime(), edm.deps.Clock.Now()) {
						absPath := filepath.Join(sentDir, dirEntry.Name())
						edm.log.Info("diskCleaner: removing file", "filename", absPath)
						err = edm.deps.FileSystem.Remove(absPath)
						if err != nil {
							edm.log.Error("diskCleaner: unable to remove sent histogram file", "error", err)
						}
					}
				}
			}
		case <-ctx.Done():
			break timerLoop
		}
	}
	edm.log.Info("exiting diskCleaner loop")
}
