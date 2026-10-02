package runner

import (
	"bytes"
	"fmt"
	"io"
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
	ServerID     []byte `parquet:"server_id"`
	QueryTime    *int64 `parquet:"query_time,timestamp(microsecond)"`
	ResponseTime *int64 `parquet:"response_time,timestamp(microsecond)"`

	SourceIdentifier     uint64          `parquet:"source_identifier"`
	DestIdentifier       *uint64         `parquet:"dest_identifier"`
	SourceIdentifierType IdentifierType  `parquet:"source_identifier_type"`
	DestIdentifierType   *IdentifierType `parquet:"dest_identifier_type"`

	SourcePort      *uint16 `parquet:"source_port"`
	DestPort        *uint16 `parquet:"dest_port"`
	DNSProtocol     *uint8  `parquet:"dns_protocol"`
	QueryMessage    []byte  `parquet:"query_message"`
	ResponseMessage []byte  `parquet:"response_message"`
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

func (edm *DnstapMinimiser) newSession(pdt pseudonymised, msg *dns.Msg, labelLimit int) *sessionData {
	sd := &sessionData{}

	if pdt.HasFlags(dnstap.ValidQueryPort) {
		sd.SourcePort = new(pdt.QueryPort)
	}

	if pdt.HasFlags(dnstap.ValidResponsePort) {
		sd.DestPort = new(pdt.ResponsePort)
	}

	edm.setLabels(dns.SplitDomainName(msg.Question[0].Name), labelLimit, &sd.dnsLabels)

	if pdt.IsQuery {
		sd.QueryMessage = bytes.Clone(pdt.Message.Message)
		sd.QueryTime = new(pdt.Timestamp.UnixMicro())
	} else {
		sd.ResponseMessage = bytes.Clone(pdt.Message.Message)
		sd.ResponseTime = new(pdt.Timestamp.UnixMicro())
	}

	if len(pdt.Identity) != 0 {
		sd.ServerID = []byte(pdt.Identity)
	}

	sd.SourceIdentifier = pdt.QueryAddrAsIdentifier()
	sd.SourceIdentifierType = pdt.QueryAddrType
	sd.DestIdentifier = new(pdt.ResponseAddrAsIdentifier())
	sd.DestIdentifierType = new(pdt.ResponseAddrType)

	if pdt.HasFlags(dnstap.ValidSocketProtocol) {
		sd.DNSProtocol = new(pdt.SocketProtocol)
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
