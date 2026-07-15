package runner

import (
	"context"
	"net/netip"
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
// cryptopanCache is the worker-private Crypto-PAn LRU (nil disables
// caching); Run creates it so a creation failure surfaces as a startup
// error instead of a silently dead worker.
func (edm *DnstapMinimiser) runMinimiser(ctx context.Context, minimiserID int, reloadConfigCh <-chan struct{}, cryptopanCache *lru.Cache[netip.Addr, netip.Addr], seenQnameLRU *lru.Cache[string, struct{}], seenStore seenQnameStore, labelLimit int, wkdTracker *wellKnownDomainsTracker) {
	dt := dnstap.Message{}

	// Per-worker scratch buffer for the unpseudonymised client IP we pass
	// to wkdTracker.sendUpdate for HLL hashing. Sized to fit IPv6 and
	// resliced to len(QueryAddress) per frame.
	var dangerScratch [16]byte

	// startConf is used for things that do not handle reconfiguration at runtime
	startConf := edm.getConfig()

	// cryptopanLastGen tracks the last cryptopan-instance generation we
	// saw; when setCryptopan installs a new key it bumps edm.cryptopanGen
	// and we Purge on the next frame, so at most one frame after a key
	// rotation can still return a cached old-key pseudonym before the
	// cache is cleared.
	cryptopanLastGen := edm.cryptopanGen.Load()

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

			// Keep around the unpseudonymised client IP for HLL
			// data, be careful with logging or otherwise handling
			// this IP as it is sensitive. Borrow the per-worker
			// scratch buffer, falling back to allocation only for an
			// address longer than the scratch buffer (IPv4 and IPv6
			// both fit).
			dangerRealClientIP := append(dangerScratch[:0], dt.QueryAddr.AsSlice()...)

			// Detect cryptopan key rotation; purge our local cache so
			// no IPs anonymised under the old key bleed through.
			if gen := edm.cryptopanGen.Load(); gen != cryptopanLastGen {
				if cryptopanCache != nil {
					cryptopanCache.Purge()
				}
				cryptopanLastGen = gen
			}
			edm.pseudonymiseDnstap(&dt, edm.cryptopan.Load(), cryptopanCache)

			msg := edm.parsePacket(&dt)

			// Create a less specific timestamp for data sent to
			// core to make precise tracking harder.
			truncatedTimestamp := dt.Timestamp.Truncate(time.Minute)

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

			// We pass on the client address for cardinality
			// measurements.
			dawgIndex, suffixMatch, dawgModTime := wkdTracker.lookup(msg)
			if dawgIndex != dawgNotFound {
				wkdTracker.sendUpdate(dangerRealClientIP, msg, dawgIndex, suffixMatch, dawgModTime)
				continue
			}

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
				session := edm.newSession(&dt, msg, labelLimit)
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
		edm.log.Error("unable to unpack message", "error", err, "query_address", dt.QueryAddr, "query_port", dt.QueryPort, "response_address", dt.ResponseAddr, "response_port", dt.ResponsePort)
		return nil
	}
	return msg
}
