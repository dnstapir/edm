package runner

import (
	"context"
	"strings"
	"time"

	"github.com/dnstapir/edm/pkg/dnstap"
	"github.com/dnstapir/edm/pkg/protocols"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/miekg/dns"
)

// runMinimiser is the main loop of the program, it reads dnstap from
// inputChannel and decides what further processing to do.
//
// reloadConfigCh delivers config-reload notifications for this worker.
func (edm *DnstapMinimiser) runMinimiser(ctx context.Context, minimiserID int, reloadConfigCh <-chan struct{}, seenQnameLRU *lru.Cache[string, struct{}], seenStore seenQnameStore, labelLimit int, wkdTracker *wellKnownDomainsTracker) {
	dt := dnstap.Message{}

	// startConf is used for things that do not handle reconfiguration at runtime
	startConf := edm.getConfig()

	// conf is meant to be dynamically modified if the config changes at runtime
	conf := edm.getConfig()

minimiserLoop:
	for {
		select {
		case frame := <-edm.inputChannel:
			edm.promDnstapProcessed.Inc()

			if err := dt.Unpack(frame); err != nil {
				edm.log.Error("DnstapMinimiser.runMinimiser: proto.Unpack() failed, skipping frame", "error", err, "minimiser_id", minimiserID)
				continue
			}

			// For now we only care about response type dnstap packets
			if dt.IsQuery {
				continue
			}

			if edm.clientIPIsIgnored(&dt) {
				continue
			}

			// parse DNS message
			msg := edm.parsePacket(&dt)

			// For cases where we were unable to unpack the DNS message we
			// skip parsing.
			if msg == nil {
				edm.promDNSParseError.Inc()
				continue
			}

			if len(msg.Question) == 0 {
				edm.promEmptyQuestionSection.Inc()
				continue
			}

			for _, question := range msg.Question {
				if _, ok := dns.IsDomainName(question.Name); !ok {
					edm.promInvalidQuestionName.Inc()
					continue minimiserLoop
				}
			}

			if edm.questionIsIgnored(msg) {
				continue
			}

			// pseudonymise IPs
			pdt := edm.pseudonymiseIPs(&dt, domainSalt(msg))

			// We pass on the client address for cardinality
			// measurements.
			dawgIndex, suffixMatch, dawgModTime := wkdTracker.lookup(msg)
			if dawgIndex != dawgNotFound {
				wkdTracker.sendUpdate(pdt, msg, dawgIndex, suffixMatch, dawgModTime)
				continue
			}

			// Create a less specific timestamp for data sent to
			// core to make precise tracking harder.
			truncatedTimestamp := dt.Timestamp.Truncate(time.Minute)

			if !edm.qnameSeen(msg, seenQnameLRU, seenStore, conf.PebbleSync) {
				if !startConf.DisableMQTT {
					newQname := protocols.NewQnameEvent(msg, truncatedTimestamp)

					select {
					case edm.newQnamePublisherCh <- &newQname:
						edm.promNewQnameQueued.Inc()
					default:
						// If the publisher channel is full we skip creating an event.
						edm.promNewQnameDiscarded.Inc()
					}
				}
			}

			if !conf.DisableSessionFiles {
				session := edm.newSession(pdt, msg, labelLimit)
				select {
				case edm.sessionCollectorCh <- session:
				case <-ctx.Done():
				}
			}
		case <-reloadConfigCh:
			edm.log.Info("runMinimiser: reloading config", "minimiser_id", minimiserID)
			newConf := edm.getConfig()
			if conf.DisableSessionFiles != newConf.DisableSessionFiles {
				if newConf.DisableSessionFiles {
					edm.log.Info("disabling session files", "minimiser_id", minimiserID)
				} else {
					edm.log.Info("enabling session files", "minimiser_id", minimiserID)
				}
			}

			conf = newConf
		case <-ctx.Done():
			break minimiserLoop
		}
	}
	edm.log.Info("runMinimiser: exiting loop", "minimiser_id", minimiserID)
}

func (edm *DnstapMinimiser) parsePacket(dt *dnstap.Message) *dns.Msg {
	msg := new(dns.Msg)
	err := msg.Unpack(dt.Message)
	if err != nil {
		edm.log.Error("unable to unpack message", "error", err)
		return nil
	}
	return msg
}

// tries to extract up to two labels of the first question and
// creates an FNV-1a hash of the results, if it fails then the
// FNV offset is used as the salt
func domainSalt(msg *dns.Msg) uint64 {
	fnv := uint64(14695981039346656037) // FNV-1a 64-bit offset
	if len(msg.Question) > 0 {
		name := msg.Question[0].Name
		name = strings.TrimSuffix(name, ".")
		name = strings.ToLower(name)
		offset, _ := dns.PrevLabel(name, 2)
		name = name[offset:]
		// FNV-1a 64-bit main loop
		for i := 0; i < len(name); i++ {
			fnv = fnv ^ uint64(name[i])
			fnv = fnv * 1099511628211
		}
	}
	// FNV-1a 64-bit hash
	return fnv
}
