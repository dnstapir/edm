package runner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/miekg/dns"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	"github.com/segmentio/go-hll"
)

type edmStatusBits uint64

func (dsb *edmStatusBits) String() string {
	if *dsb >= edmStatusMax {
		return fmt.Sprintf("unknown flags in status: %b", *dsb)
	}

	switch *dsb {
	case edmStatusWellKnownExact:
		return "well-known-exact"
	case edmStatusWellKnownWildcard:
		return "well-known-wildcard"
	}

	var flags []string
	for flag := edmStatusWellKnownExact; flag < edmStatusMax; flag <<= 1 {
		if *dsb&flag != 0 {
			flags = append(flags, flag.String())
		}
	}
	return strings.Join(flags, "|")
}

func (dsb *edmStatusBits) set(flag edmStatusBits) {
	*dsb = *dsb | flag
}

const (
	edmStatusWellKnownExact    edmStatusBits = 1 << iota // 1
	edmStatusWellKnownWildcard                           // 2

	// Always leave max at the end to signal unused bits
	edmStatusMax
)

// Histogram parquet files are named histogramFileBase + "-<start>_<stop>" +
// parquetFileSuffix (see [buildParquetFilenames]); the same pair is used to
// recognize histogram files in the outbox and sent directories.
const (
	histogramFileBase = "dns_histogram"
	parquetFileSuffix = ".parquet"
)

// Histogram struct implementing description at https://github.com/dnstapir/datasets/blob/main/HistogramReport.md
type histogramData struct {
	StartTime int64 `parquet:"start_time,timestamp(microsecond)"`
	dnsLabels
	// The time we started collecting the data contained in the histogram
	ACount          uint64 `parquet:"a_count"`
	AAAACount       uint64 `parquet:"aaaa_count"`
	MXCount         uint64 `parquet:"mx_count"`
	NSCount         uint64 `parquet:"ns_count"`
	OtherTypeCount  uint64 `parquet:"other_type_count"`
	NonINCount      uint64 `parquet:"non_in_count"`
	OKCount         uint64 `parquet:"ok_count"`
	NXCount         uint64 `parquet:"nx_count"`
	FailCount       uint64 `parquet:"fail_count"`
	OtherRcodeCount uint64 `parquet:"other_rcode_count"`
	EDMStatusBits   uint64 `parquet:"edm_status_bits"`
	// The hll.Hll structs are not expected to be included in the output
	// parquet file, and thus do not need to be exported
	v4ClientHLL hll.Hll
	v6ClientHLL hll.Hll

	// V4ClientCount/V6ClientCount always contain the cardinality
	// calculation result
	V4ClientCount uint64 `parquet:"v4client_count"`
	V6ClientCount uint64 `parquet:"v6client_count"`

	// These fields are NULL when HLL uses explicit storage, otherwise
	// contain the probabilistic HLL bytes
	V4ClientCountHLLBytes []byte `parquet:"v4client_count_hll,optional"`
	V6ClientCountHLLBytes []byte `parquet:"v6client_count_hll,optional"`
}

func getHllDefaults(explicitThreshold int) hll.Settings {
	return hll.Settings{
		Log2m:             10,
		Regwidth:          4,
		ExplicitThreshold: explicitThreshold,
		SparseEnabled:     true,
	}
}

func (edm *DnstapMinimiser) sendHistogram(ctx context.Context, prevWellKnownDomainsData *wellKnownDomainsData, labelLimit int, buf *bytes.Buffer) error {
	if prevWellKnownDomainsData == nil {
		return nil
	}
	// gather data
	startTime := intervalStartFromTimes(prevWellKnownDomainsData.startTime, prevWellKnownDomainsData.rotationTime).UTC() // TODO verify
	duration := prevWellKnownDomainsData.rotationTime.Sub(startTime).Round(time.Second)                                  // TODO verify
	// marshal histogram
	err := edm.writeHistogramParquet(buf, startTime, prevWellKnownDomainsData, labelLimit)
	if err != nil {
		return fmt.Errorf("sendHistogram: %w", err)
	}
	// send histogram
	err = edm.aggregSend(ctx, buf, startTime, duration)
	if err != nil {
		return fmt.Errorf("sendHistogram: %w", err)
	}
	return nil
}

func (edm *DnstapMinimiser) aggregSend(ctx context.Context, buf *bytes.Buffer, ts time.Time, duration time.Duration) error {
	// Make a copy of the struct under lock
	// so the network communication from
	// send() does not block aggregSender
	// management.
	edm.aggregSenderMutex.RLock()
	as := edm.aggregSender
	edm.aggregSenderMutex.RUnlock()
	if as == nil {
		return errors.New("aggregSend: aggregate sender is not initialized")
	}
	// send
	return as.Send(ctx, buf, ts, duration)
}

func (edm *DnstapMinimiser) histogramSender(ctx context.Context, labelLimit int) {
	edm.log.Info("histogramSender: starting")

	var buf bytes.Buffer

	// load configuration
	disableHistogramSender := edm.getConfig().DisableHistogramSender

	// log state
	switch disableHistogramSender {
	case true:
		edm.log.Info("histogramSender: starting", "state", "disabled")
	case false:
		edm.log.Info("histogramSender: starting", "state", "enabled")
	}

	// retry ticker
	retryTicker := time.Tick(edm.deps.HistogramSenderRetryInterval)

loop:
	for {
		select {
		case prevWellKnownDomainsData, ok := <-edm.histogramWriterCh:
			// handle primary queue

			// if channel closed => exit
			if !ok {
				break loop
			}
			// if sender disabled => discard
			if disableHistogramSender {
				continue loop
			}
			// reset shared buffer
			buf.Reset()
			// try sending the histogram, on error => try adding to retry queue
			err := edm.sendHistogram(ctx, prevWellKnownDomainsData, labelLimit, &buf)
			if err != nil {
				// try queuing it for a retry
				select {
				case edm.histogramRetryWriterCh <- prevWellKnownDomainsData:
				default:
				}
				// log
				edm.log.Error("histogramSender", "error", err.Error())
			}
		case <-retryTicker:
			// handle retry queue
		retryLoop:
			for {
				select {
				case prevWellKnownDomainsData := <-edm.histogramRetryWriterCh:
					// if sender disabled => discard
					if disableHistogramSender {
						continue retryLoop
					}
					// reset shared buffer
					buf.Reset()
					// try sending the histogram, on error => discard
					err := edm.sendHistogram(ctx, prevWellKnownDomainsData, labelLimit, &buf)
					if err != nil {
						edm.log.Error("histogramSender", "error", err.Error(), "retry", true)
					}
				default:
					// return to normal activites if retry queue is empty
					break retryLoop
				}
			}
		case <-edm.reloadHistogramSenderConfigCh:
			// handle configuration change
			edm.log.Info("histogramSender: reloading config")
			newDisableHistogramSender := edm.getConfig().DisableHistogramSender

			if disableHistogramSender != newDisableHistogramSender {
				switch newDisableHistogramSender {
				case true:
					edm.log.Info("histogramSender: disabling histogram sender")
				case false:
					edm.log.Info("histogramSender: enabling histogram sender")
				}
				disableHistogramSender = newDisableHistogramSender
			}
		}
	}

	edm.log.Info("histogramSender: exiting loop")
}

// Unfortunately the hll library does not expose what format
// the HLL is being stored in so figure things out manually.
//
// The format of the bytes are documented at
// https://github.com/aggregateknowledge/hll-storage-spec
//
// See https://github.com/segmentio/go-hll/issues/8 for a request to make this easier.
//
// BEGIN: Code manually based on https://github.com/segmentio/go-hll/blob/main/hll.go

// hllStorageType is an enum whose values match the type values in the hll
// storage spec. In the spec, the "dense" value is referred to as "full". We
// use the name dense because we find it to be more descriptive.
type hllStorageType int

const (
	hllUndefined hllStorageType = iota
	hllEmpty
	hllExplicit
	hllSparse
	hllDense
)

// END: Code manually based on https://github.com/segmentio/go-hll/blob/main/hll.go

const (
	supportedHLLVersion = 1
	hllVersionShift     = 4
	hllTypeMask         = 0xF
)

func parseHllStorageType(hllBytes []byte) (hllStorageType, error) {
	if len(hllBytes) == 0 {
		return 0, fmt.Errorf("parseHLLStorageType: empty HLL byte slice")
	}
	version := hllBytes[0] >> hllVersionShift
	// Verify the HLL format is at the expected version so we do
	// not make the wrong assumptions about the meaning of the bytes
	if version != supportedHLLVersion {
		return 0, fmt.Errorf("parseHllStorageType: unexpected version: %d", version)
	}
	storageType := hllStorageType(hllBytes[0] & hllTypeMask)

	return storageType, nil
}

func (edm *DnstapMinimiser) writeHistogramParquet(output io.Writer, startTime time.Time, prevWellKnownDomainsData *wellKnownDomainsData, labelLimit int) error {
	// The previous DAWG finder is intentionally NOT closed here. lookup() reads
	// the well-known-domains finder lock-free via wkd.snap, so a minimiser
	// worker may still hold a rotated-out finder; closing (munmapping) it would
	// race with that reader and risk a use-after-free. The old finder is left
	// for the GC to reclaim once no reader references it, matching the
	// ignoredQuestions/ignoredClients atomic-reload policy.

	snappyCodec := parquet.LookupCompressionCodec(format.Snappy)
	parquetWriter := parquet.NewGenericWriter[histogramData](output, parquet.Compression(snappyCodec))

	startTimeMicro := startTime.UnixMicro()

	for index, hGramData := range prevWellKnownDomainsData.m {
		domain, err := prevWellKnownDomainsData.dawgFinder.AtIndex(index)
		if err != nil {
			return fmt.Errorf("writeHistogramParquet: unable to find DAWG index %d: %w", index, err)
		}

		labels := dns.SplitDomainName(domain)

		// Setting the labels now when we are out of the hot path.
		edm.setLabels(labels, labelLimit, &hGramData.dnsLabels)
		hGramData.StartTime = startTimeMicro

		hGramData.V4ClientCount = hGramData.v4ClientHLL.Cardinality()
		hGramData.V6ClientCount = hGramData.v6ClientHLL.Cardinality()

		v4HLLBytes := hGramData.v4ClientHLL.ToBytes()
		v4HLLType, err := parseHllStorageType(v4HLLBytes)
		if err != nil {
			return fmt.Errorf("writeHistogramParquet: IPv4 HLL parsing failed: %w", err)
		}

		v6HLLBytes := hGramData.v6ClientHLL.ToBytes()
		v6HLLType, err := parseHllStorageType(v6HLLBytes)
		if err != nil {
			return fmt.Errorf("writeHistogramParquet: IPv6 HLL parsing failed: %w", err)
		}

		// Include bytes from our hll data structures if they are stored with a probabilistic storage type
		if v4HLLType == hllSparse || v4HLLType == hllDense {
			hGramData.V4ClientCountHLLBytes = v4HLLBytes
		}
		if v6HLLType == hllSparse || v6HLLType == hllDense {
			hGramData.V6ClientCountHLLBytes = v6HLLBytes
		}

		_, err = parquetWriter.Write([]histogramData{*hGramData})
		if err != nil {
			return fmt.Errorf("writeHistogramParquet: unable to call Write() on parquet writer: %w", err)
		}
	}

	err := parquetWriter.Close()
	if err != nil {
		return fmt.Errorf("writeHistogramParquet: unable to call Close() on parquet writer: %w", err)
	}

	return nil
}

func (edm *DnstapMinimiser) newHistogramData(hllSettings hll.Settings, suffixMatch bool) *histogramData {
	// We leave the label0-9 fields set to nil here. Since this is in
	// the hot path of dealing with dnstap packets the less work we do the
	// better. They are filled in prior to writing out the parquet file.
	hd := &histogramData{}

	var err error
	hd.v4ClientHLL, err = hll.NewHll(hllSettings)
	if err != nil {
		edm.log.Error("unable to initialize IPv4 HLL", "error", err)
		// This is never expected to happen
		panic(err)
	}

	hd.v6ClientHLL, err = hll.NewHll(hllSettings)
	if err != nil {
		edm.log.Error("unable to initialize IPv6 HLL", "error", err)
		// This is never expected to happen
		panic(err)
	}

	esb := new(edmStatusBits)
	if suffixMatch {
		esb.set(edmStatusWellKnownWildcard)
	} else {
		esb.set(edmStatusWellKnownExact)
	}
	hd.EDMStatusBits = uint64(*esb)

	return hd
}
