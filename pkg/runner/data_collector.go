package runner

import (
	"fmt"
	"sync"
	"time"
)

// runMinimiser generates data and it is collected into datasets here
func (edm *DnstapMinimiser) dataCollector(wg *sync.WaitGroup, wkd *wellKnownDomainsTracker, dawgFile string) {
	defer wg.Done()

	// Keep track of if we have recorded any dnstap packets in session data
	var sessionUpdated bool

	// Start retryer, handles instances where the received update has a
	// dawgModTime that is no longer valid becuase it has been rotated.
	var retryerWg sync.WaitGroup
	retryerWg.Add(1)
	go wkd.updateRetryer(edm, &retryerWg)

	sessions := []*sessionData{}
	sessionIntervalStart := edm.deps.Clock.Now().UTC()
	histogramIntervalStart := sessionIntervalStart

	ticker := edm.deps.Clock.NewTicker(timeUntilNextMinuteFrom(edm.deps.Clock.Now()))
	defer ticker.Stop()

	retryChannelClosed := false

	conf := edm.getConfig()

	hllSettings := getHllDefaults(conf.HistogramHLLExplicitThreshold)

	processSession := func(sd *sessionData) {
		if sd == nil {
			return
		}
		sessions = append(sessions, sd)
		sessionUpdated = true
	}

	flushSessions := func(startTime time.Time, rotationTime time.Time) {
		if !sessionUpdated {
			return
		}
		ps := &prevSessions{
			sessions:     sessions,
			startTime:    startTime,
			rotationTime: rotationTime,
		}
		sessions = []*sessionData{}
		sessionUpdated = false
		edm.sessionWriterCh <- ps
	}

	processWKDUpdate := func(wu wkdUpdate) {
		// It is possible an update sitting in the queue has
		// been created with an outdated dawgModTime due to a
		// call to rotateTracker(). If this is the case we need
		// to do a new lookup against the new dawg to make sure
		// we have the correct index number (or if it is even
		// present in the new dawg).
		if wu.dawgModTime != wkd.snap.Load().dawgModTime {
			if !retryChannelClosed {
				wkd.retryCh <- wu
			} else {
				edm.log.Info("discarding retry of wkd update because we are shutting down")
			}
			return
		}

		if _, exists := wkd.m[wu.dawgIndex]; !exists {
			wkd.m[wu.dawgIndex] = edm.newHistogramData(hllSettings, wu.suffixMatch)
		}

		wkd.m[wu.dawgIndex].OKCount += wu.OKCount
		wkd.m[wu.dawgIndex].NXCount += wu.NXCount
		wkd.m[wu.dawgIndex].FailCount += wu.FailCount
		wkd.m[wu.dawgIndex].ACount += wu.ACount
		wkd.m[wu.dawgIndex].AAAACount += wu.AAAACount
		wkd.m[wu.dawgIndex].MXCount += wu.MXCount
		wkd.m[wu.dawgIndex].NSCount += wu.NSCount
		wkd.m[wu.dawgIndex].OtherTypeCount += wu.OtherTypeCount
		wkd.m[wu.dawgIndex].OtherRcodeCount += wu.OtherRcodeCount
		wkd.m[wu.dawgIndex].NonINCount += wu.NonINCount

		if wu.ip.IsValid() {
			if wu.ip.Unmap().Is4() {
				wkd.m[wu.dawgIndex].v4ClientHLL.AddRaw(wu.hllHash)
			} else {
				wkd.m[wu.dawgIndex].v6ClientHLL.AddRaw(wu.hllHash)
			}
		}
	}

	drainCollectorQueues := func() {
		for {
			select {
			case sd := <-edm.sessionCollectorCh:
				processSession(sd)
			case wu := <-wkd.updateCh:
				processWKDUpdate(wu)
			default:
				return
			}
		}
	}

	rotateCollectedData := func(sessionStart time.Time, histogramStart time.Time, rotationTime time.Time) error {
		flushSessions(sessionStart, rotationTime)

		prevWKD, err := wkd.rotateTracker(edm, dawgFile, histogramStart, rotationTime)
		if err != nil {
			return fmt.Errorf("unable to rotate histogram map: %w", err)
		}

		// Only write out parquet file if there is something to write.
		if len(prevWKD.m) > 0 {
			edm.histogramWriterCh <- prevWKD
		}

		return nil
	}

	flushHistogram := func(startTime time.Time, rotationTime time.Time) {
		if len(wkd.m) == 0 {
			return
		}
		edm.histogramWriterCh <- &wellKnownDomainsData{
			m:            wkd.m,
			startTime:    startTime,
			rotationTime: rotationTime,
			dawgFinder:   wkd.snap.Load().dawgFinder,
		}
		wkd.m = map[int]*histogramData{}
	}

collectorLoop:
	for {
		select {
		case sd := <-edm.sessionCollectorCh:
			processSession(sd)

		case wu := <-wkd.updateCh:
			processWKDUpdate(wu)

		case ts := <-ticker.C():
			// We want to tick at the start of each minute
			ticker.Reset(timeUntilNextMinuteFrom(edm.deps.Clock.Now()))

			err := rotateCollectedData(sessionIntervalStart, histogramIntervalStart, ts)
			// Sessions were already flushed; advance their boundary regardless.
			sessionIntervalStart = ts
			if err != nil {
				edm.log.Error("unable to rotate parquet data", "error", err)
				continue
			}
			histogramIntervalStart = ts

			// See if we need to modify anything based on a config update
			conf = edm.getConfig()

			if conf.HistogramHLLExplicitThreshold != hllSettings.ExplicitThreshold {
				edm.log.Info("updating HLL explicit threshold based on config change", "from", hllSettings.ExplicitThreshold, "to", conf.HistogramHLLExplicitThreshold)
				hllSettings.ExplicitThreshold = conf.HistogramHLLExplicitThreshold
			}

		case req := <-edm.parquetRotationRequestCh:
			edm.log.Info("dataCollector: manual parquet rotation requested", "rotation_time", req.rotationTime)
			drainCollectorQueues()
			err := rotateCollectedData(sessionIntervalStart, histogramIntervalStart, req.rotationTime)
			// Sessions were already flushed; advance their boundary regardless.
			sessionIntervalStart = req.rotationTime
			req.done <- err
			if err != nil {
				edm.log.Error("unable to rotate parquet data", "error", err)
				continue
			}
			// The histogram only rotates on success, so its boundary
			// advances only once rotateTracker has succeeded.
			histogramIntervalStart = req.rotationTime

		case <-wkd.stop:
			// Tell retryer to stop
			edm.log.Info("dataCollector: telling update retryer to stop")
			close(wkd.retryCh)
			retryChannelClosed = true
			// set stop channel to nil so we do not attempt to
			// read from it again in this select statement now that
			// it is closed.
			wkd.stop = nil

		case <-wkd.retryerDone:
			edm.log.Info("dataCollector: update retryer is done")
			drainCollectorQueues()
			shutdownTime := edm.deps.Clock.Now().UTC()
			flushSessions(sessionIntervalStart, shutdownTime)
			flushHistogram(histogramIntervalStart, shutdownTime)
			break collectorLoop
		}
	}

	// Close the channels we write to
	close(edm.sessionWriterCh)
	close(edm.histogramWriterCh)

	edm.log.Info("dataCollector: exiting loop")
}
