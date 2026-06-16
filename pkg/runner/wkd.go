package runner

import (
	"fmt"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/miekg/dns"
	"github.com/smhanov/dawg"
	"github.com/twmb/murmur3"
)

const dawgNotFound = -1

// wkdSnapshot is the read-side view that hot-path callers need: the current
// DAWG finder plus the modtime that goes into wkdUpdate so the collector can
// detect post-rotation refresh. Stored in wellKnownDomainsTracker.snap as
// an atomic.Pointer so lookup() reads it without locking.
type wkdSnapshot struct {
	dawgFinder  dawg.Finder
	dawgModTime time.Time
}

type wellKnownDomainsTracker struct {
	// snap holds the current DAWG + modtime. Replaced atomically when
	// the dawg file rotates; readers in lookup() do a single Load.
	snap atomic.Pointer[wkdSnapshot]

	// m is the per-dawgIndex histogram aggregator. It is read & written
	// only by dataCollector (the same goroutine that calls
	// rotateTracker), so it needs no lock.
	m map[int]*histogramData

	updateCh    chan wkdUpdate
	retryCh     chan wkdUpdate
	stop        chan struct{}
	retryerDone chan struct{}
}

// wellKnownDomainsData is a per-interval histogram batch handed to the
// histogram writer. m is the interval's bucket map and dawgFinder is the DAWG
// used to map bucket indices back to names. It is produced by rotateTracker
// on normal rotation and by the shutdown flush in dataCollector; in both cases
// dawgFinder is the snapshot finder that was active for the data in m.
type wellKnownDomainsData struct {
	m            map[int]*histogramData
	startTime    time.Time
	rotationTime time.Time
	dawgFinder   dawg.Finder
}

func newWellKnownDomainsTracker(dawgFinder dawg.Finder, dawgModTime time.Time) (*wellKnownDomainsTracker, error) {
	wkd := &wellKnownDomainsTracker{
		m:           map[int]*histogramData{},
		updateCh:    make(chan wkdUpdate, 10000),
		retryCh:     make(chan wkdUpdate, 10000),
		stop:        make(chan struct{}),
		retryerDone: make(chan struct{}),
	}
	wkd.snap.Store(&wkdSnapshot{dawgFinder: dawgFinder, dawgModTime: dawgModTime})
	return wkd, nil
}

// Try to find a domain name string match in DAWG data and return the index as
// well as if it was found based on a suffix string or not.
func getDawgIndex(dawgFinder dawg.Finder, name string) (int, bool) {
	// Ignore capitalisation in labels
	name = strings.ToLower(name)

	// Try exact match first
	dawgIndex := dawgFinder.IndexOf(name)

	if dawgIndex == dawgNotFound {
		// Next try to look up suffix matches, so for the name
		// "www.example.com." we will check for the strings
		// ".example.com." and ".com.".
		for index, end := dns.NextLabel(name, 0); !end; index, end = dns.NextLabel(name, index) {
			dawgIndex = dawgFinder.IndexOf(name[index-1:])
			if dawgIndex != dawgNotFound {
				return dawgIndex, true
			}
		}
	}

	return dawgIndex, false
}

type wkdUpdate struct {
	// embed histogramData so we automatically have access to all the
	// fields we may want to increment with an update message.
	histogramData
	dawgIndex   int
	suffixMatch bool
	hllHash     uint64
	ip          netip.Addr
	msg         *dns.Msg
	dawgModTime time.Time
	retry       int
	retryLimit  int
}

func (wkd *wellKnownDomainsTracker) lookup(msg *dns.Msg) (int, bool, time.Time) {
	snap := wkd.snap.Load()
	dawgIndex, suffixMatch := getDawgIndex(snap.dawgFinder, msg.Question[0].Name)
	return dawgIndex, suffixMatch, snap.dawgModTime
}

func (wkd *wellKnownDomainsTracker) updateRetryer(edm *DnstapMinimiser, wg *sync.WaitGroup) {
	defer wg.Done()

	for wu := range wkd.retryCh {
		wu.retry++
		if wu.retry >= wu.retryLimit {
			edm.log.Info("ignoring wkd update since retry counter hit retry limit", "retry", wu.retry, "retry_limit", wu.retryLimit)
			continue
		}

		dawgIndex, suffixMatch, dawgModTime := wkd.lookup(wu.msg)
		if dawgIndex == dawgNotFound {
			edm.log.Info("ignoring wkd update because name does not exist in updated wkd tracker", "update_dawg_modtime", wu.dawgModTime, "wkd_dawg_modtime", dawgModTime)
			continue
		}

		// Refresh the update to match new dawg version
		wu.dawgIndex = dawgIndex
		wu.suffixMatch = suffixMatch
		wu.dawgModTime = dawgModTime

		if edm.debug {
			edm.log.Debug("resending refreshed wkd update", "retry_counter", wu.retry)
		}
		wkd.updateCh <- wu
	}

	edm.log.Info("updateRetryer: exiting loop")
	close(wkd.retryerDone)
}

func (wkd *wellKnownDomainsTracker) sendUpdate(ipBytes []byte, msg *dns.Msg, dawgIndex int, suffixMatch bool, dawgModTime time.Time) {
	wu := wkdUpdate{
		dawgIndex:   dawgIndex,
		suffixMatch: suffixMatch,
		dawgModTime: dawgModTime,
		hllHash:     0,
		retryLimit:  10,
		msg:         msg,
	}

	// Create hash from IP address for use in HLL data
	ip, ok := netip.AddrFromSlice(ipBytes)
	if ok {
		// We use a deterministic seed by design to be able to combine HLL
		// datasets.
		wu.hllHash = murmur3.Sum64(ipBytes)
		wu.ip = ip
	}

	// Counters based on header
	switch msg.Rcode {
	case dns.RcodeSuccess:
		wu.OKCount++
	case dns.RcodeNameError:
		wu.NXCount++
	case dns.RcodeServerFailure:
		wu.FailCount++
	default:
		wu.OtherRcodeCount++
	}

	// Counters based on question class and type
	if msg.Question[0].Qclass == dns.ClassINET {
		switch msg.Question[0].Qtype {
		case dns.TypeA:
			wu.ACount++
		case dns.TypeAAAA:
			wu.AAAACount++
		case dns.TypeMX:
			wu.MXCount++
		case dns.TypeNS:
			wu.NSCount++
		default:
			wu.OtherTypeCount++
		}
	} else {
		wu.NonINCount++
	}

	wkd.updateCh <- wu
}

func (wkd *wellKnownDomainsTracker) rotateTracker(edm *DnstapMinimiser, dawgFile string, startTime time.Time, rotationTime time.Time) (*wellKnownDomainsData, error) {
	dawgFileChanged := false
	var dawgFinder dawg.Finder
	var dawgModTime time.Time

	fileInfo, err := edm.deps.FileSystem.Stat(dawgFile)
	if err != nil {
		return nil, fmt.Errorf("rotateTracker: unable to stat dawgFile '%s': %w", dawgFile, err)
	}

	curSnap := wkd.snap.Load()
	if fileInfo.ModTime() != curSnap.dawgModTime {
		dawgFinder, dawgModTime, err = edm.deps.DawgLoader.LoadDawgFile(dawgFile)
		if err != nil {
			return nil, fmt.Errorf("rotateTracker: DawgLoader.LoadDawgFile(): %w", err)
		}
		dawgFileChanged = true
		edm.log.Info("dawg file modification changed, will reload file", "prev_time", curSnap.dawgModTime, "cur_time", fileInfo.ModTime())
	}

	// rotateTracker runs in the dataCollector goroutine, which is also
	// the only writer of wkd.m (see the case wu := <-wkd.updateCh branch).
	// No lock needed for the map swap. The DAWG snapshot is a separate
	// atomic Store so hot-path lookup() callers see a consistent view.
	prevWKD := &wellKnownDomainsData{
		m:            wkd.m,
		dawgFinder:   curSnap.dawgFinder,
		startTime:    startTime,
		rotationTime: rotationTime,
	}
	wkd.m = map[int]*histogramData{}
	if dawgFileChanged {
		wkd.snap.Store(&wkdSnapshot{
			dawgFinder:  dawgFinder,
			dawgModTime: dawgModTime,
		})
	}

	return prevWKD, nil
}
