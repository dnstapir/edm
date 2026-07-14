package runner

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net/netip"
	"path/filepath"
	"strings"
	"time"

	dnstap "github.com/dnstap/golang-dnstap"
	"github.com/miekg/dns"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// We need to create the session data schema by hand instead of basing it of
// the sessionData struct directly because we have uint16 fields for ports and
// these are not currently supported, see:
// https://github.com/parquet-go/parquet-go/pull/122
//
// One drawback of writing out the schema like this is due to the use of a map
// in the parquet.Group we can not control the ordering of the fields, they are
// sorted however, see:
// Issue regarding order:
// https://github.com/parquet-go/parquet-go/issues/43
// Commit that makes the map sorted:
// https://github.com/parquet-go/parquet-go/commit/035e69db6792fdc9089e238084bebe39e26c74b0
var sessionDataSchema = parquet.NewSchema(
	"sessionData",
	parquet.Group{
		"label0":              parquet.Optional(parquet.String()),
		"label1":              parquet.Optional(parquet.String()),
		"label2":              parquet.Optional(parquet.String()),
		"label3":              parquet.Optional(parquet.String()),
		"label4":              parquet.Optional(parquet.String()),
		"label5":              parquet.Optional(parquet.String()),
		"label6":              parquet.Optional(parquet.String()),
		"label7":              parquet.Optional(parquet.String()),
		"label8":              parquet.Optional(parquet.String()),
		"label9":              parquet.Optional(parquet.String()),
		"server_id":           parquet.Optional(parquet.Leaf(parquet.ByteArrayType)),
		"query_time":          parquet.Optional(parquet.Timestamp(parquet.Microsecond)),
		"response_time":       parquet.Optional(parquet.Timestamp(parquet.Microsecond)),
		"source_ipv4":         parquet.Optional(parquet.Uint(32)),
		"dest_ipv4":           parquet.Optional(parquet.Uint(32)),
		"source_ipv6_network": parquet.Optional(parquet.Uint(64)),
		"source_ipv6_host":    parquet.Optional(parquet.Uint(64)),
		"dest_ipv6_network":   parquet.Optional(parquet.Uint(64)),
		"dest_ipv6_host":      parquet.Optional(parquet.Uint(64)),
		"source_port":         parquet.Optional(parquet.Uint(16)),
		"dest_port":           parquet.Optional(parquet.Uint(16)),
		"dns_protocol":        parquet.Optional(parquet.Uint(8)),
		"query_message":       parquet.Optional(parquet.Leaf(parquet.ByteArrayType)),
		"response_message":    parquet.Optional(parquet.Leaf(parquet.ByteArrayType)),
	},
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
	ServerID     *string `parquet:"server_id"`
	QueryTime    *int64  `parquet:"query_time"`
	ResponseTime *int64  `parquet:"response_time"`
	SourceIPv4   *int32  `parquet:"source_ipv4"`
	DestIPv4     *int32  `parquet:"dest_ipv4"`
	// IPv6 addresses are split up into a network and host part, for one thing go does not have native uint128 types
	SourceIPv6Network *int64  `parquet:"source_ipv6_network"`
	SourceIPv6Host    *int64  `parquet:"source_ipv6_host"`
	DestIPv6Network   *int64  `parquet:"dest_ipv6_network"`
	DestIPv6Host      *int64  `parquet:"dest_ipv6_host"`
	SourcePort        *int32  `parquet:"source_port"`
	DestPort          *int32  `parquet:"dest_port"`
	DNSProtocol       *int32  `parquet:"dns_protocol"`
	QueryMessage      *string `parquet:"query_message"`
	ResponseMessage   *string `parquet:"response_message"`
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

func (edm *DnstapMinimiser) newSession(dt *dnstap.Dnstap, msg *dns.Msg, isQuery bool, labelLimit int, timestamp time.Time) *sessionData {
	sd := &sessionData{}

	if dt.Message.QueryPort != nil {
		if *dt.Message.QueryPort > math.MaxInt32 {
			edm.log.Error("dt.Message.QueryPort is too large for int32, setting port to 0", "value", *dt.Message.QueryPort)
			var qp int32
			sd.SourcePort = &qp
		} else {
			qp := int32(*dt.Message.QueryPort) // #nosec G115 -- QueryPort is defined as 16-bit number and is used in parquet field with type=INT32, convertedType=UINT_16, https://github.com/securego/gosec/issues/1212#issuecomment-2739574884
			sd.SourcePort = &qp
		}
	}

	if dt.Message.ResponsePort != nil {
		if *dt.Message.ResponsePort > math.MaxInt32 {
			edm.log.Error("dt.Message.ResponsePort is too large for int32, setting port to 0", "value", *dt.Message.ResponsePort)
			var rp int32
			sd.DestPort = &rp
		} else {
			rp := int32(*dt.Message.ResponsePort) // #nosec G115 -- ResponsePort is defined as 16-bit number and is used in parquet field with type=INT32, convertedType=UINT_16, https://github.com/securego/gosec/issues/1212#issuecomment-2739574884
			sd.DestPort = &rp
		}
	}

	edm.setLabels(dns.SplitDomainName(msg.Question[0].Name), labelLimit, &sd.dnsLabels)

	if isQuery {
		qms := string(dt.Message.QueryMessage)
		sd.QueryMessage = &qms

		ms := timestamp.UnixMicro()
		sd.QueryTime = &ms
	} else {
		rms := string(dt.Message.ResponseMessage)
		sd.ResponseMessage = &rms

		ms := timestamp.UnixMicro()
		sd.ResponseTime = &ms
	}

	if len(dt.Identity) != 0 {
		sID := string(dt.Identity)
		sd.ServerID = &sID
	}

	switch dt.Message.GetSocketFamily() {
	case dnstap.SocketFamily_INET:
		if dt.Message.QueryAddress != nil {
			sourceIPInt, err := ipBytesToInt(dt.Message.QueryAddress)
			if err != nil {
				edm.log.Error("unable to create uint32 from dt.Message.QueryAddress", "error", err)
			} else {
				i32SourceIPInt := int32(sourceIPInt) // #nosec G115 -- Used in parquet struct with convertedType=UINT_32
				sd.SourceIPv4 = &i32SourceIPInt
			}
		}

		if dt.Message.ResponseAddress != nil {
			destIPInt, err := ipBytesToInt(dt.Message.ResponseAddress)
			if err != nil {
				edm.log.Error("unable to create uint32 from dt.Message.ResponseAddress", "error", err)
			} else {
				i32DestIPInt := int32(destIPInt) // #nosec G115 -- Used in parquet struct with convertedType=UINT_32
				sd.DestIPv4 = &i32DestIPInt
			}
		}
	case dnstap.SocketFamily_INET6:
		if dt.Message.QueryAddress != nil {
			sourceIPIntNetwork, sourceIPIntHost, err := ip6BytesToInt(dt.Message.QueryAddress)
			if err != nil {
				edm.log.Error("unable to create uint64 variables from dt.Message.QueryAddress", "error", err)
			} else {
				i64SourceIntNetwork := int64(sourceIPIntNetwork) // #nosec G115 -- Used in parquet struct with convertedType=UINT_64
				i64SourceIntHost := int64(sourceIPIntHost)       // #nosec G115 -- Used in parquet struct with convertedType=UINT_64
				sd.SourceIPv6Network = &i64SourceIntNetwork
				sd.SourceIPv6Host = &i64SourceIntHost
			}
		}

		if dt.Message.ResponseAddress != nil {
			dipIntNetwork, dipIntHost, err := ip6BytesToInt(dt.Message.ResponseAddress)
			if err != nil {
				edm.log.Error("unable to create uint64 variables from dt.Message.ResponseAddress", "error", err)
			} else {
				i64dIntNetwork := int64(dipIntNetwork) // #nosec G115 -- Used in parquet struct with convertedType=UINT_64
				i64dIntHost := int64(dipIntHost)       // #nosec G115 -- Used in parquet struct with convertedType=UINT_64
				sd.DestIPv6Network = &i64dIntNetwork
				sd.DestIPv6Host = &i64dIntHost
			}
		}
	case 0:
		// SocketFamily not set: tolerate partial metadata and leave the IP
		// fields nil rather than logging an error for every such packet.
	default:
		edm.log.Error("packet is neither INET or INET6")
	}

	if dt.Message.SocketProtocol != nil {
		dnsProtocol := int32(dt.Message.GetSocketProtocol())
		sd.DNSProtocol = &dnsProtocol
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
	parquetWriter := parquet.NewGenericWriter[sessionData](output, sessionDataSchema, parquet.Compression(snappyCodec))

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
