package runner

import (
	"bufio"
	"errors"
	"fmt"
	"net/netip"
	"path/filepath"
	"strings"

	dnstap "github.com/dnstap/golang-dnstap"
	"github.com/miekg/dns"
	"github.com/smhanov/dawg"
	"go4.org/netipx"
)

func (edm *DnstapMinimiser) setIgnoredQuestionNames() error {
	conf := edm.getConfig()

	if conf.IgnoredQuestionNamesFile == "" {
		edm.ignoredQuestions.Store(nil)
		edm.log.Info("setIgnoredQuestionNames: DNS question ignore list unset", "filename", conf.IgnoredQuestionNamesFile, "num_names", 0)
		return nil
	}

	dawgFinder, _, err := edm.deps.DawgLoader.LoadDawgFile(conf.IgnoredQuestionNamesFile)
	if err != nil {
		if errors.Is(err, errEmptyDawgFile) {
			// Treat the same as unset filename
			edm.ignoredQuestions.Store(nil)
			edm.log.Info("setIgnoredQuestionNames: DNS question ignore list empty", "filename", conf.IgnoredQuestionNamesFile, "num_names", 0)
			return nil
		}
		return fmt.Errorf("setIgnoredQuestionNames: unable to load dawg file '%s': %w", conf.IgnoredQuestionNamesFile, err)
	}

	// We only use the dawg file if there exists at least one name in it.
	// Atomic-pointer swap; deliberately do NOT Close the old finder here
	// as that would race with hot-path readers still holding it. Reloads
	// leave the old finder for the GC to reclaim once no reader references
	// it (see the ignoredQuestions field comment on DnstapMinimiser).
	if dawgFinder.NumAdded() > 0 {
		edm.ignoredQuestions.Store(&dawgFinderHolder{finder: dawgFinder})
	} else {
		edm.ignoredQuestions.Store(nil)
	}

	if dawgFinder.NumAdded() > 0 {
		edm.log.Info("setIgnoredQuestionNames: DNS question ignore list loaded", "filename", conf.IgnoredQuestionNamesFile, "num_names", dawgFinder.NumAdded())
	} else {
		edm.log.Info("setIgnoredQuestionNames: DNS question ignore list empty, no question names will be ignored", "filename", conf.IgnoredQuestionNamesFile, "num_names", dawgFinder.NumAdded())
	}

	return nil
}

func (edm *DnstapMinimiser) setIgnoredClientIPs() error {
	conf := edm.getConfig()

	if conf.IgnoredClientIPsFile == "" {
		edm.ignoredClientsIPSet.Store(nil)
		edm.ignoredClientCIDRsParsed.Store(0)
		edm.log.Info("setIgnoredClientIPs: DNS client ignore list unset", "filename", conf.IgnoredClientIPsFile, "num_cidrs", 0)
		return nil
	}

	fh, err := edm.deps.FileSystem.Open(filepath.Clean(conf.IgnoredClientIPsFile))
	if err != nil {
		return fmt.Errorf("setIgnoredClientsIPs: unable to open file: %w", err)
	}
	defer func() {
		err := fh.Close()
		if err != nil {
			edm.log.Error("setIgnoredClientIPs: failed closing fh", "filename", conf.IgnoredClientIPsFile, "error", err)
		}
	}()

	var b netipx.IPSetBuilder

	var numCIDRs uint64
	scanner := bufio.NewScanner(fh)
	for scanner.Scan() {
		if scanner.Text() == "" || strings.HasPrefix(scanner.Text(), "#") {
			// Skip empty lines and comments
			continue
		}
		prefix, err := netip.ParsePrefix(scanner.Text())
		if err != nil {
			return fmt.Errorf("setIgnoredClientIPs: unable to parse ignored prefix '%s'", scanner.Text())
		}
		b.AddPrefix(prefix)
		numCIDRs++
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("setIgnoredClientIPs: error reading from '%s': %w", conf.IgnoredClientIPsFile, err)
	}

	// Starts out as nil. We only set it to an initialized IPSet if there is at
	// least one ignored client CIDR present in the input file.
	var ipset *netipx.IPSet
	if numCIDRs > 0 {
		ipset, err = b.IPSet()
		if err != nil {
			return fmt.Errorf("setIgnoredClientIPs: IPSet creation failed: %w", err)
		}
	}

	edm.ignoredClientsIPSet.Store(ipset)
	edm.ignoredClientCIDRsParsed.Store(numCIDRs)

	if ipset != nil {
		edm.log.Info("setIgnoredClientIPs: DNS client ignore list loaded", "filename", conf.IgnoredClientIPsFile, "num_cidrs", numCIDRs)
	} else {
		edm.log.Info("setIgnoredClientIPs: DNS client ignore list empty, no clients will be ignored", "filename", conf.IgnoredClientIPsFile, "num_cidrs", numCIDRs)
	}

	return nil
}

func (edm *DnstapMinimiser) getNumIgnoredClientCIDRs() uint64 {
	return edm.ignoredClientCIDRsParsed.Load()
}

// dawgFinderHolder is a tiny concrete-type wrapper so dawg.Finder (which is
// an interface) can be stored in an atomic.Pointer. Used for the
// ignoredQuestions atomic snapshot.
type dawgFinderHolder struct {
	finder dawg.Finder
}

func (edm *DnstapMinimiser) clientIPIsIgnored(dt *dnstap.Dnstap) bool {
	// Atomic snapshot - no lock on the hot path. Reload writers
	// atomic.Store the new IPSet; readers see either old or new value
	// per Load.
	ipset := edm.ignoredClientsIPSet.Load()
	if ipset == nil {
		return false
	}
	clientIP, ok := netip.AddrFromSlice(dt.Message.QueryAddress)
	if !ok {
		// If we have a list of clients to ignore but are not able to
		// understand the QueryAddress let's err on the side of caution
		// and ignore such packets as well while making noise in logs
		// so it can be investigated.
		edm.log.Error("unable to parse QueryAddress for ignore-checking, ignoring dnstap packet to be safe, please investigate")
		edm.promClientIPIgnoredError.Inc()
		return true
	}
	if ipset.Contains(clientIP) {
		edm.promClientIPIgnored.Inc()
		return true
	}
	return false
}

func (edm *DnstapMinimiser) questionIsIgnored(msg *dns.Msg) bool {
	// Atomic snapshot - no lock on the hot path. See clientIPIsIgnored
	// for the rationale.
	holder := edm.ignoredQuestions.Load()
	if holder == nil {
		return false
	}
	// While uncommon, if there happens to be multiple questions in the
	// packet we consider the message ignored if any of them matches.
	for _, question := range msg.Question {
		dawgIndex, _ := getDawgIndex(holder.finder, question.Name)
		if dawgIndex != dawgNotFound {
			edm.promQuestionNameIgnored.Inc()
			return true
		}
	}
	return false
}
