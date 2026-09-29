package runner

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net/netip"
	"path/filepath"
	"strings"
	"time"

	"github.com/dnstapir/edm/pkg/dnstap"
	"github.com/miekg/dns"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

type dnsLabels struct {
	// Store label fields as pointers so we can signal them being unset as
	// opposed to an empty string
	Label0 *string `parquet:"label0"`
	Label1 *string `parquet:"label1"`
	Label2 *string `parquet:"label2"`
	Label3 *string `parquet:"label3"`
	Label4 *string `parquet:"label4"`
	Label5 *string `parquet:"label5"`
	Label6 *string `parquet:"label6"`
	Label7 *string `parquet:"label7"`
	Label8 *string `parquet:"label8"`
	Label9 *string `parquet:"label9"`
}

type sessionData struct {
	dnsLabels
	ServerID     []byte  `parquet:"server_id"`
	QueryTime    *int64  `parquet:"query_time,timestamp(microsecond)"`
	ResponseTime *int64  `parquet:"response_time,timestamp(microsecond)"`
	SourceIPv4   *uint32 `parquet:"source_ipv4"`
	DestIPv4     *uint32 `parquet:"dest_ipv4"`
	// IPv6 addresses are split up into a network and host part, for one thing go does not have native uint128 types
	SourceIPv6Network *uint64 `parquet:"source_ipv6_network"`
	SourceIPv6Host    *uint64 `parquet:"source_ipv6_host"`
	DestIPv6Network   *uint64 `parquet:"dest_ipv6_network"`
	DestIPv6Host      *uint64 `parquet:"dest_ipv6_host"`
	SourcePort        *uint16 `parquet:"source_port"`
	DestPort          *uint16 `parquet:"dest_port"`
	DNSProtocol       *uint8  `parquet:"dns_protocol"`
	QueryMessage      []byte  `parquet:"query_message"`
	ResponseMessage   []byte  `parquet:"response_message"`
}

type prevSessions struct {
	sessions     []*sessionData
	startTime    time.Time
	rotationTime time.Time
}

func (edm *DnstapMinimiser) setLabels(labels []string, labelLimit int, l *dnsLabels) {
	// If labels is nil (the "." zone) we can depend on the zero type of
	// the label fields being nil, so nothing to do
	if labels == nil {
		return
	}

	reverseLabels := edm.reverseLabelsBounded(labels, labelLimit)

	for index := range reverseLabels {
		switch index {
		case 0:
			l.Label0 = &reverseLabels[index]
		case 1:
			l.Label1 = &reverseLabels[index]
		case 2:
			l.Label2 = &reverseLabels[index]
		case 3:
			l.Label3 = &reverseLabels[index]
		case 4:
			l.Label4 = &reverseLabels[index]
		case 5:
			l.Label5 = &reverseLabels[index]
		case 6:
			l.Label6 = &reverseLabels[index]
		case 7:
			l.Label7 = &reverseLabels[index]
		case 8:
			l.Label8 = &reverseLabels[index]
		case 9:
			l.Label9 = &reverseLabels[index]
		}
	}
}

func (edm *DnstapMinimiser) reverseLabelsBounded(labels []string, maxLen int) []string {
	// If labels is nil (the "." zone) there is nothing to do
	if labels == nil {
		return nil
	}

	boundedReverseLabels := []string{}

	remainderElems := 0
	if len(labels) > maxLen {
		remainderElems = len(labels) - maxLen
	}

	// Append all labels except the last one
	for i := len(labels) - 1; i > remainderElems; i-- {
		boundedReverseLabels = append(boundedReverseLabels, labels[i])
	}

	// If the labels fit inside maxLen then just append the last remaining
	// label as-is
	if len(labels) <= maxLen {
		boundedReverseLabels = append(boundedReverseLabels, labels[0])
	} else {
		// If there are more labels than maxLen we need to concatenate
		// them before appending the last element
		if remainderElems > 0 {
			remainderLabels := []string{}
			for i := remainderElems; i >= 0; i-- {
				remainderLabels = append(remainderLabels, labels[i])
			}

			boundedReverseLabels = append(boundedReverseLabels, strings.Join(remainderLabels, "."))
		}
	}
	return boundedReverseLabels
}

func (edm *DnstapMinimiser) newSession(dt *dnstap.Message, msg *dns.Msg, labelLimit int) *sessionData {
	sd := &sessionData{}

	if dt.HasFlags(dnstap.ValidQueryPort) {
		sd.SourcePort = new(dt.QueryPort)
	}

	if dt.HasFlags(dnstap.ValidResponsePort) {
		sd.DestPort = new(dt.ResponsePort)
	}

	edm.setLabels(dns.SplitDomainName(msg.Question[0].Name), labelLimit, &sd.dnsLabels)

	if dt.IsQuery {
		sd.QueryMessage = bytes.Clone(dt.Message)
		sd.QueryTime = new(dt.Timestamp.UnixMicro())
	} else {
		sd.ResponseMessage = bytes.Clone(dt.Message)
		sd.ResponseTime = new(dt.Timestamp.UnixMicro())
	}

	if len(dt.Identity) != 0 {
		sd.ServerID = []byte(dt.Identity)
	}

	if dt.HasFlags(dnstap.ValidQueryAddr) {
		switch {
		case dt.QueryAddr.Is4():
			sourceIPInt, err := ipBytesToInt(dt.QueryAddr.AsSlice())
			if err != nil {
				edm.log.Error("unable to create uint32 from dt.QueryAddr", "error", err)
			} else {
				sd.SourceIPv4 = new(sourceIPInt)
			}
		case dt.QueryAddr.Is6():
			sourceIPIntNetwork, sourceIPIntHost, err := ip6BytesToInt(dt.QueryAddr.AsSlice())
			if err != nil {
				edm.log.Error("unable to create uint64 variables from dt.QueryAddr", "error", err)
			} else {
				sd.SourceIPv6Network = new(sourceIPIntNetwork)
				sd.SourceIPv6Host = new(sourceIPIntHost)
			}
		}
	}

	if dt.HasFlags(dnstap.ValidResponseAddr) {
		switch {
		case dt.ResponseAddr.Is4():
			destIPInt, err := ipBytesToInt(dt.ResponseAddr.AsSlice())
			if err != nil {
				edm.log.Error("unable to create uint32 from dt.ResponseAddr", "error", err)
			} else {
				sd.DestIPv4 = new(destIPInt)
			}
		case dt.ResponseAddr.Is6():
			dipIntNetwork, dipIntHost, err := ip6BytesToInt(dt.ResponseAddr.AsSlice())
			if err != nil {
				edm.log.Error("unable to create uint64 variables from dt.ResponseAddr", "error", err)
			} else {
				sd.DestIPv6Network = new(dipIntNetwork)
				sd.DestIPv6Host = new(dipIntHost)
			}
		}
	}

	if dt.HasFlags(dnstap.ValidSocketProtocol) {
		sd.DNSProtocol = new(dt.SocketProtocol)
	}

	return sd
}

func (edm *DnstapMinimiser) createSessionFile(ps *prevSessions, dataDir string) (string, error) {
	// Write session file to a sessions dir where it can be read by other tools
	sessionsDir := filepath.Join(dataDir, "parquet", "sessions")

	startTime := intervalStartFromTimes(ps.startTime, ps.rotationTime)

	absoluteTmpFileName, absoluteFileName := buildParquetFilenames(sessionsDir, "dns_session_block", startTime, ps.rotationTime)

	absoluteTmpFileName = filepath.Clean(absoluteTmpFileName) // Make gosec happy

	name, err := edm.writeRotatedParquet("session", absoluteTmpFileName, absoluteFileName, func(w io.Writer) error {
		return edm.writeSessionParquet(w, ps)
	})
	if err != nil {
		return "", fmt.Errorf("createSessionFile: %w", err)
	}
	return name, nil
}

func (edm *DnstapMinimiser) sessionWriter(dataDir string) {
	edm.log.Info("sessionWriter: starting")

	for ps := range edm.sessionWriterCh {
		_, err := edm.createSessionFile(ps, dataDir)
		if err != nil {
			edm.log.Error("sessionWriter", "error", err.Error())
		}
	}

	edm.log.Info("sessionWriter: exiting loop")
}

func ipBytesToInt(ip4Bytes []byte) (uint32, error) {
	ip, ok := netip.AddrFromSlice(ip4Bytes)
	if !ok {
		return 0, fmt.Errorf("ipBytesToInt: unable to parse bytes")
	}
	ip = ip.Unmap()
	if !ip.Is4() {
		return 0, fmt.Errorf("ipBytesToInt: address is not IPv4: %s", ip)
	}

	// Make sure we are dealing with 4 byte IPv4 address data (and deal with IPv4-in-IPv6 addresses)
	ip4 := ip.As4()

	ipInt := binary.BigEndian.Uint32(ip4[:])

	return ipInt, nil
}

func ip6BytesToInt(ip6Bytes []byte) (uint64, uint64, error) {
	ip, ok := netip.AddrFromSlice(ip6Bytes)
	if !ok {
		return 0, 0, fmt.Errorf("ip6BytesToInt: unable to parse bytes")
	}

	ip16 := ip.As16()

	ipIntNetwork := binary.BigEndian.Uint64(ip16[:8])
	ipIntHost := binary.BigEndian.Uint64(ip16[8:])

	return ipIntNetwork, ipIntHost, nil
}

func (edm *DnstapMinimiser) writeSessionParquet(output io.Writer, ps *prevSessions) error {
	snappyCodec := parquet.LookupCompressionCodec(format.Snappy)
	parquetWriter := parquet.NewGenericWriter[sessionData](output, parquet.Compression(snappyCodec))

	for _, sd := range ps.sessions {
		_, err := parquetWriter.Write([]sessionData{*sd})
		if err != nil {
			return fmt.Errorf("writeSessionParquet: unable to call Write() on parquet writer: %w", err)
		}
	}

	err := parquetWriter.Close()
	if err != nil {
		return fmt.Errorf("writeSessionParquet: unable to call Close() on parquet writer: %w", err)
	}

	return nil
}
