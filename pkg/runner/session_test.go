package runner

import (
	"bytes"
	"log/slog"
	"math"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	extdnstap "github.com/dnstap/golang-dnstap"
	"github.com/dnstapir/edm/pkg/dnstap"
	"github.com/miekg/dns"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

func BenchmarkSetLabels(b *testing.B) {
	b.ReportAllocs()
	labels := []string{"label0", "label1", "label2", "label3", "label4", "label5", "label6", "label7", "label8", "label9"}
	edm := &DnstapMinimiser{}
	l := dnsLabels{}

	for i := 0; i < b.N; i++ {
		edm.setLabels(labels, 10, &l)
	}
}

func TestSetSessionLabels(t *testing.T) {
	// The reason the labels are "backwards" is because we define "label0"
	// in the struct as the rightmost DNS label, e.g. "com", "net" etc.
	labels := []string{"label9", "label8", "label7", "label6", "label5", "label4", "label3", "label2", "label1", "label0"}
	edm := &DnstapMinimiser{}
	sd := &sessionData{}

	edm.setLabels(labels, 10, &sd.dnsLabels)

	if *sd.Label0 != labels[9] {
		t.Fatalf("have: %s, want: %s", *sd.Label0, labels[9])
	}
	if *sd.Label1 != labels[8] {
		t.Fatalf("have: %s, want: %s", *sd.Label1, labels[8])
	}
	if *sd.Label2 != labels[7] {
		t.Fatalf("have: %s, want: %s", *sd.Label2, labels[7])
	}
	if *sd.Label3 != labels[6] {
		t.Fatalf("have: %s, want: %s", *sd.Label3, labels[6])
	}
	if *sd.Label4 != labels[5] {
		t.Fatalf("have: %s, want: %s", *sd.Label4, labels[5])
	}
	if *sd.Label5 != labels[4] {
		t.Fatalf("have: %s, want: %s", *sd.Label5, labels[4])
	}
	if *sd.Label6 != labels[3] {
		t.Fatalf("have: %s, want: %s", *sd.Label6, labels[3])
	}
	if *sd.Label7 != labels[2] {
		t.Fatalf("have: %s, want: %s", *sd.Label7, labels[2])
	}
	if *sd.Label8 != labels[1] {
		t.Fatalf("have: %s, want: %s", *sd.Label8, labels[1])
	}
	if *sd.Label9 != labels[0] {
		t.Fatalf("have: %s, want: %s", *sd.Label9, labels[0])
	}
}

func BenchmarkSessionWriter(b *testing.B) {
	b.ReportAllocs()

	var buf bytes.Buffer
	snappyCodec := parquet.LookupCompressionCodec(format.Snappy)
	parquetWriter := parquet.NewGenericWriter[sessionData](&buf, parquet.Compression(snappyCodec))

	identifier := new(uint64(123456789))

	sd := sessionData{
		dnsLabels: dnsLabels{
			Label0: new("com"),
			Label1: new("example"),
			Label2: new("www"),
		},
		ServerID:     []byte("serverID"),
		QueryTime:    new(int64(10)),
		ResponseTime: new(int64(10)),

		SourceIdentifier: *identifier,
		DestIdentifier:   identifier,

		SourcePort:      new(uint16(1337)),
		DestPort:        new(uint16(1337)),
		DNSProtocol:     new(uint8(1)),
		QueryMessage:    []byte("query message"),
		ResponseMessage: []byte("response message"),
	}

	for b.Loop() {
		_, err := parquetWriter.Write([]sessionData{sd})
		if err != nil {
			b.Fatalf("unable to call Write() on parquet writer: %s", err)
		}
	}
	err := parquetWriter.Close()
	if err != nil {
		b.Fatalf("unable to call WriteStop() on parquet writer: %s", err)
	}
}

func TestSessionWriter(t *testing.T) {
	var buf bytes.Buffer

	snappyCodec := parquet.LookupCompressionCodec(format.Snappy)
	parquetWriter := parquet.NewGenericWriter[sessionData](&buf, parquet.Compression(snappyCodec))

	identifier := new(uint64(123456789))

	sd := sessionData{
		dnsLabels: dnsLabels{
			Label0: new("com"),
			Label1: new("example"),
			Label2: new("www"),
		},
		ServerID:     []byte("serverID"),
		QueryTime:    new(int64(10)),
		ResponseTime: new(int64(10)),

		SourceIdentifier: *identifier,
		DestIdentifier:   identifier,

		SourcePort:      new(uint16(1337)),
		DestPort:        new(uint16(1337)),
		DNSProtocol:     new(uint8(1)),
		QueryMessage:    []byte("query message"),
		ResponseMessage: []byte("response message"),
	}

	_, err := parquetWriter.Write([]sessionData{sd})
	if err != nil {
		t.Fatalf("unable to call Write() on parquet writer: %s", err)
	}

	err = parquetWriter.Close()
	if err != nil {
		t.Fatalf("unable to call Close() on parquet writer: %s", err)
	}

	if *writeParquet {
		f, err := os.Create(filepath.Join(t.TempDir(), "generated-session.parquet"))
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			err := f.Close()
			if err != nil {
				t.Fatal(err)
			}
		}()

		_, err = buf.WriteTo(f)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestSetLabelsNilAndBoundedReverse(t *testing.T) {
	edm := &DnstapMinimiser{}

	labels := edm.reverseLabelsBounded(nil, 10)
	if labels != nil {
		t.Fatalf("nil labels = %#v", labels)
	}

	dl := &dnsLabels{}
	edm.setLabels(nil, 10, dl)
	if dl.Label0 != nil {
		t.Fatalf("nil labels set Label0 = %q", *dl.Label0)
	}

	got := edm.reverseLabelsBounded([]string{"a", "b", "c"}, 10)
	want := []string{"c", "b", "a"}
	if !slices.Equal(got, want) {
		t.Fatalf("reverseLabelsBounded = %#v, want %#v", got, want)
	}
}

func TestSessionParquetAndSessionConstruction(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	packed := packedDNSMsg(t, "www.example.com.", dns.TypeA, dns.RcodeSuccess)
	dt := testUnpackedDnstapMessage(t, extdnstap.Message_CLIENT_RESPONSE, extdnstap.SocketFamily_INET, packed)

	msg := edm.parsePacket(dt)
	if msg == nil {
		t.Fatal("parsePacket returned nil msg")
	}
	if !dt.Timestamp.Equal(time.Unix(1_700_000_001, 456).UTC()) {
		t.Fatalf("response timestamp = %v", dt.Timestamp)
	}

	sd := edm.newSession(edm.pseudonymiseIPs(dt), msg, defaultLabelLimit)
	if sd.ResponseTime == nil || sd.ResponseMessage == nil || sd.ServerID == nil {
		t.Fatalf("session missing response fields: %#v", sd)
	}
	if *sd.ResponseTime != time.Unix(1_700_000_001, 456).UTC().UnixMicro() {
		t.Fatalf("incorrect session response timestamp = %v", dt.Timestamp)
	}
	if sd.SourceIdentifier != 198_051_100_020 || sd.DestIdentifier == nil ||
		sd.DNSProtocol == nil {
		t.Fatalf("session missing network fields: %#v", sd)
	}
	if sd.SourceIdentifierType != IdentifierIPv4 || *sd.DestIdentifierType != IdentifierIPv4 {
		t.Fatalf("incorrect identifiers: %#v", sd)
	}

	var buf bytes.Buffer
	if err := edm.writeSessionParquet(&buf, &prevSessions{sessions: []*sessionData{sd}}); err != nil {
		t.Fatal(err)
	}
	rows, err := parquet.Read[sessionData](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ServerID == nil || string(rows[0].ServerID) != "server-1" {
		t.Fatalf("unexpected session rows: %#v", rows)
	}

	queryDT := testUnpackedDnstapMessage(t, extdnstap.Message_CLIENT_QUERY, extdnstap.SocketFamily_INET6, packed)
	queryMsg := edm.parsePacket(queryDT)
	querySession := edm.newSession(edm.pseudonymiseIPs(queryDT), queryMsg, defaultLabelLimit)
	if querySession.QueryTime == nil || querySession.QueryMessage == nil ||
		querySession.SourceIdentifier != 0x2001_0db8_0000_0000 || querySession.DestIdentifier == nil {
		t.Fatalf("query session missing fields: %#v", querySession)
	}
	if querySession.SourceIdentifierType != IdentifierIPv6 || *querySession.DestIdentifierType != IdentifierIPv6 {
		t.Fatalf("incorrect identifiers: %#v", querySession)
	}
}

// TestNewSessionBranches covers some cases when newSession doesn't
// set some fields due to invalid input data
func TestNewSessionBranches(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	packed := packedDNSMsg(t, "www.example.com.", dns.TypeA, dns.RcodeSuccess)

	t.Run("port overflow does not set port", func(t *testing.T) {
		dt := testUnpackedDnstapMessage(t, extdnstap.Message_CLIENT_RESPONSE, extdnstap.SocketFamily_INET, packed, func(dt *extdnstap.Dnstap) {
			big := uint32(math.MaxInt32) + 1
			dt.Message.QueryPort = &big
			dt.Message.ResponsePort = &big
		})
		msg := edm.parsePacket(dt)
		sd := edm.newSession(edm.pseudonymiseIPs(dt), msg, defaultLabelLimit)
		if sd.SourcePort != nil {
			t.Fatalf("SourcePort = %v, want nil", *sd.SourcePort)
		}
		if sd.DestPort != nil {
			t.Fatalf("DestPort = %v, want nil", *sd.DestPort)
		}
	})

	t.Run("bad INET address bytes", func(t *testing.T) {
		dt := testUnpackedDnstapMessage(t, extdnstap.Message_CLIENT_RESPONSE, extdnstap.SocketFamily_INET, packed, func(dt *extdnstap.Dnstap) {
			dt.Message.QueryAddress = []byte{1, 2, 3}
			dt.Message.ResponseAddress = []byte{4, 5, 6}
		})
		msg := edm.parsePacket(dt)
		sd := edm.newSession(edm.pseudonymiseIPs(dt), msg, defaultLabelLimit)
		if sd.SourceIdentifier != 0x01_02_03_0000000000 || sd.DestIdentifier == nil {
			t.Fatalf("SourceIdentifier and DestIdentifier should be set")
		}
		if sd.SourceIdentifierType != IdentifierOther || *sd.DestIdentifierType != IdentifierOther {
			t.Fatalf("incorrect identifiers: %#v", sd)
		}
	})

	t.Run("mismatched IPv6 address bytes with INET family leaves IPv4/IPv6 nil", func(t *testing.T) {
		dt := testUnpackedDnstapMessage(t, extdnstap.Message_CLIENT_RESPONSE, extdnstap.SocketFamily_INET, packed, func(dt *extdnstap.Dnstap) {
			dt.Message.QueryAddress = netip.MustParseAddr("2001:db8::20").AsSlice()
			dt.Message.ResponseAddress = netip.MustParseAddr("2001:db8::53").AsSlice()
		})
		msg := edm.parsePacket(dt)
		sd := edm.newSession(edm.pseudonymiseIPs(dt), msg, defaultLabelLimit)
		if sd.SourceIdentifier != 0x2001_0db8_0000_0000 || sd.DestIdentifier == nil {
			t.Fatalf("SourceIdentifier and DestIdentifier should be set")
		}
		if sd.SourceIdentifierType != IdentifierOther || *sd.DestIdentifierType != IdentifierOther {
			t.Fatalf("incorrect identifiers: %#v", sd)
		}
	})

	t.Run("bad INET6 address bytes", func(t *testing.T) {
		dt := testUnpackedDnstapMessage(t, extdnstap.Message_CLIENT_RESPONSE, extdnstap.SocketFamily_INET6, packed, func(dt *extdnstap.Dnstap) {
			dt.Message.QueryAddress = []byte{1, 2, 3}
			dt.Message.ResponseAddress = []byte{4, 5, 6}
		})
		msg := edm.parsePacket(dt)
		sd := edm.newSession(edm.pseudonymiseIPs(dt), msg, defaultLabelLimit)
		if sd.SourceIdentifier != 0x01_02_03_0000000000 || sd.DestIdentifier == nil {
			t.Fatalf("SourceIdentifier and DestIdentifier should be set")
		}
		if sd.SourceIdentifierType != IdentifierOther || *sd.DestIdentifierType != IdentifierOther {
			t.Fatalf("incorrect identifiers: %#v", sd)
		}
	})

	t.Run("unknown socket family leaves IPs nil", func(t *testing.T) {
		dt := testUnpackedDnstapMessage(t, extdnstap.Message_CLIENT_RESPONSE, extdnstap.SocketFamily_INET, packed, func(dt *extdnstap.Dnstap) {
			unknown := extdnstap.SocketFamily(99)
			dt.Message.SocketFamily = &unknown
		})
		msg := edm.parsePacket(dt)
		sd := edm.newSession(edm.pseudonymiseIPs(dt), msg, defaultLabelLimit)
		if sd.SourceIdentifier != 198_051_100_020 || sd.DestIdentifier == nil {
			t.Fatalf("SourceIdentifier and DestIdentifier should be set")
		}
		if sd.SourceIdentifierType != IdentifierOther || *sd.DestIdentifierType != IdentifierOther {
			t.Fatalf("incorrect identifiers: %#v", sd)
		}
	})

	t.Run("empty identity leaves ServerID nil", func(t *testing.T) {
		dt := testUnpackedDnstapMessage(t, extdnstap.Message_CLIENT_RESPONSE, extdnstap.SocketFamily_INET, packed, func(dt *extdnstap.Dnstap) {
			dt.Identity = nil
		})
		msg := edm.parsePacket(dt)
		sd := edm.newSession(edm.pseudonymiseIPs(dt), msg, defaultLabelLimit)
		if sd.ServerID != nil {
			t.Fatalf("ServerID should be nil for empty identity, got %s", string(sd.ServerID))
		}
	})
}

// TestSessionWriterLogsCreateError verifies the sessionWriter worker logs and
// keeps running when createSessionFile fails. The failure is injected through
// FileSystem.Create so writeSessionParquet is never reached.
func TestSessionWriterLogsCreateError(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	var buf bytes.Buffer
	edm.log = slog.New(slog.NewJSONHandler(&buf, nil))

	edm.deps.FileSystem = faultingFileSystem{fileSystem: edm.deps.FileSystem, create: func(string) (fsFile, error) { return nil, errInjected }}

	edm.sessionWriterCh <- &prevSessions{rotationTime: time.Now()}
	close(edm.sessionWriterCh)

	var wg sync.WaitGroup
	wg.Go(func() { edm.sessionWriter(t.TempDir()) })
	// waitForWaitGroup blocks until wg.Done(), establishing happens-before for
	// the buffer read below (the worker's last write precedes its Done()).
	waitForWaitGroup(t, &wg, 5*time.Second, "sessionWriter did not exit")

	if !strings.Contains(buf.String(), `"level":"ERROR"`) || !strings.Contains(buf.String(), "sessionWriter") {
		t.Fatalf("expected error log from sessionWriter, got: %q", buf.String())
	}
}

func TestNewSessionAllowsMissingSocketMetadata(t *testing.T) {
	edm := discardEDM()
	msg := new(dns.Msg)
	msg.SetQuestion("example.com.", dns.TypeA)

	dt := dnstap.Message{
		Timestamp: time.Unix(0, 0).UTC(),
	}
	sd := edm.newSession(edm.pseudonymiseIPs(&dt), msg, defaultLabelLimit)

	if sd.DNSProtocol != nil {
		t.Fatalf("DNSProtocol should be nil when SocketProtocol is missing, have: %d", *sd.DNSProtocol)
	}
	if sd.SourceIdentifier != 0 {
		t.Fatalf("SourceIdentifier should be set to zero(we don't have a pseudonymiser key set)")
	}
	if sd.DestIdentifier == nil {
		t.Fatalf("DestIdentifier should be set")
	}
	if sd.SourceIdentifierType != IdentifierOther || *sd.DestIdentifierType != IdentifierOther {
		t.Fatalf("incorrect identifiers: %#v", sd)
	}
}
