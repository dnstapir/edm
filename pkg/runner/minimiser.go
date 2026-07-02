package runner

import (
	"context"
	"math"
	"net/netip"
	"strconv"
	"strings"
	"time"

	dnstap "github.com/dnstap/golang-dnstap"
	"github.com/dnstapir/edm/pkg/protocols"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/miekg/dns"
	"google.golang.org/protobuf/proto"
)

// runMinimiser is the main loop of the program, it reads dnstap from
// inputChannel and decides what further processing to do.
//
// reloadConfigCh delivers config-reload notifications for this worker.
// cryptopanCache is the worker-private Crypto-PAn LRU (nil disables
// caching); Run creates it so a creation failure surfaces as a startup
// error instead of a silently dead worker.
func (edm *DnstapMinimiser) runMinimiser(ctx context.Context, minimiserID int, reloadConfigCh <-chan struct{}, cryptopanCache *lru.Cache[netip.Addr, netip.Addr], seenQnameLRU *lru.Cache[string, struct{}], seenStore seenQnameStore, debugDnstapFile fsFile, labelLimit int, wkdTracker *wellKnownDomainsTracker) {
	dt := &dnstap.Dnstap{}

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
			if err := proto.Unmarshal(frame, dt); err != nil {
				edm.log.Error("DnstapMinimiser.runMinimiser: proto.Unmarshal() failed, skipping frame", "error", err, "minimiser_id", minimiserID)
				continue
			}
			// Guard the hot-path dereferences (here and below: QueryAddress,
			// clientIPIsIgnored) so a partially populated frame is dropped
			// rather than panicking before parsePacket's own nil checks. A
			// Message present but missing its required Type is already rejected
			// by proto.Unmarshal above; the Type check here is belt-and-braces.
			if dt.Message == nil || dt.Message.Type == nil {
				edm.log.Error("DnstapMinimiser.runMinimiser: dnstap message or type missing, skipping frame", "minimiser_id", minimiserID)
				edm.promDNSParseError.Inc()
				continue
			}

			// Keep in mind that this outputs the unmodified dnstap
			// data, so it contains sensitive information.
			if debugDnstapFile != nil {
				out, ok := dnstap.JSONFormat(dt)
				if !ok {
					edm.log.Error("unable to format dnstap debug log")
				} else {
					_, err := debugDnstapFile.Write(out)
					if err != nil {
						edm.log.Error("unable to write to dnstap debug file", "error", err, "filename", debugDnstapFile.Name(), "minimiser_id", minimiserID)
					}
				}
			}

			isQuery := strings.HasSuffix(dnstap.Message_Type_name[int32(dt.Message.GetType())], "_QUERY")

			// For now we only care about response type dnstap packets
			if isQuery {
				continue
			}

			if edm.clientIPIsIgnored(dt) {
				continue
			}

			// Keep around the unpseudonymised client IP for HLL
			// data, be careful with logging or otherwise handling
			// this IP as it is sensitive. Borrow the per-worker
			// scratch buffer, falling back to allocation only for an
			// address longer than the scratch buffer (IPv4 and IPv6
			// both fit).
			n := len(dt.Message.QueryAddress)
			var dangerRealClientIP []byte
			if n <= len(dangerScratch) {
				dangerRealClientIP = dangerScratch[:n]
			} else {
				dangerRealClientIP = make([]byte, n)
			}
			copy(dangerRealClientIP, dt.Message.QueryAddress)

			// Detect cryptopan key rotation; purge our local cache so
			// no IPs anonymised under the old key bleed through.
			if gen := edm.cryptopanGen.Load(); gen != cryptopanLastGen {
				if cryptopanCache != nil {
					cryptopanCache.Purge()
				}
				cryptopanLastGen = gen
			}
			edm.pseudonymiseDnstap(dt, edm.cryptopan.Load(), cryptopanCache)

			msg, timestamp := edm.parsePacket(dt, isQuery)

			// Create a less specific timestamp for data sent to
			// core to make precise tracking harder.
			truncatedTimestamp := timestamp.Truncate(time.Minute)

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
				session := edm.newSession(dt, msg, isQuery, labelLimit, timestamp)
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

func (edm *DnstapMinimiser) parsePacket(dt *dnstap.Dnstap, isQuery bool) (*dns.Msg, time.Time) {
	var err error

	if dt.Message == nil {
		edm.log.Error("parsePacket: dnstap message is missing")
		return nil, time.Unix(0, 0).UTC()
	}

	msg := new(dns.Msg)
	if isQuery {
		err = msg.Unpack(dt.Message.QueryMessage)
		if err != nil {
			edm.log.Error("unable to unpack query message", "error", err, "query_address", formatDnstapEndpoint(dt.Message.QueryAddress, dt.Message.QueryPort), "response_address", formatDnstapEndpoint(dt.Message.ResponseAddress, dt.Message.ResponsePort))
			msg = nil
		}
		t := edm.dnstapTimestamp(dt.Message.QueryTimeSec, dt.Message.QueryTimeNsec, "dt.Message.QueryTimeSec")
		return msg, t
	}

	err = msg.Unpack(dt.Message.ResponseMessage)
	if err != nil {
		edm.log.Error("unable to unpack response message", "error", err, "query_address", formatDnstapEndpoint(dt.Message.QueryAddress, dt.Message.QueryPort), "response_address", formatDnstapEndpoint(dt.Message.ResponseAddress, dt.Message.ResponsePort))
		msg = nil
	}
	t := edm.dnstapTimestamp(dt.Message.ResponseTimeSec, dt.Message.ResponseTimeNsec, "dt.Message.ResponseTimeSec")
	return msg, t
}

func formatDnstapEndpoint(ipBytes []byte, port *uint32) string {
	ip, ok := netip.AddrFromSlice(ipBytes)
	if ok && port != nil {
		return ip.String() + ":" + strconv.FormatUint(uint64(*port), 10)
	}
	if ok {
		return ip.String() + ":?"
	}
	if port != nil {
		return "?:" + strconv.FormatUint(uint64(*port), 10)
	}
	return "?"
}

func (edm *DnstapMinimiser) dnstapTimestamp(sec *uint64, nsec *uint32, fieldName string) time.Time {
	if sec == nil {
		edm.log.Error(fieldName + " is missing, setting time to 0")
		return time.Unix(0, 0).UTC()
	}
	if *sec > math.MaxInt64 {
		edm.log.Error(fieldName+" is too large for int64, setting time to 0", "value", *sec)
		return time.Unix(0, 0).UTC()
	}

	var nsecValue uint32
	if nsec != nil {
		nsecValue = *nsec
	}

	return time.Unix(int64(*sec), int64(nsecValue)).UTC() // #nosec G115 -- sec is checked above and nsec is uint32.
}
