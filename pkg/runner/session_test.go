package runner

import (
	"bytes"
	"encoding/binary"
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

func TestEDMIPv4ToInt(t *testing.T) {
	ip4Addr := netip.MustParseAddr("198.51.100.15")

	ip4Int, err := ipv4ToInt(ip4Addr)
	if err != nil {
		t.Fatalf("unable to create uint32 variable from IPv4 test address '%s': %s", ip4Addr, err)
	}

	// Go back to IPv4 data
	constructedV4Data := []byte{}
	constructedV4Data = binary.BigEndian.AppendUint32(constructedV4Data, ip4Int)

	constructedIP4Addr, ok := netip.AddrFromSlice(constructedV4Data)
	if !ok {
		t.Fatalf("unable to create netip from from constructed IPv4 bytes: %b", constructedV4Data)
	}

	if ip4Addr != constructedIP4Addr {
		t.Fatalf("have: %s, want: %s", constructedIP4Addr, ip4Addr)
	}
}

func TestEDMIPv6ToInt(t *testing.T) {
	ip6Addr := netip.MustParseAddr("2001:db8:1122:3344:5566:7788:99aa:bbcc")

	ip6Network, ip6Host, err := ipv6ToInt(ip6Addr)
	if err != nil {
		t.Fatalf("unable to create uint64 variables from IPv6 test address '%s': %s", ip6Addr, err)
	}

	// Go back to complete IPv6 data
	constructedV6Data := []byte{}
	constructedV6Data = binary.BigEndian.AppendUint64(constructedV6Data, ip6Network)
	constructedV6Data = binary.BigEndian.AppendUint64(constructedV6Data, ip6Host)

	constructedIP6Addr, ok := netip.AddrFromSlice(constructedV6Data)
	if !ok {
		t.Fatalf("unable to create netip from from constructed IPv6 bytes: %b", constructedV6Data)
	}

	if ip6Addr != constructedIP6Addr {
		t.Fatalf("have: %s, want: %s", constructedIP6Addr, ip6Addr)
	}
}

func BenchmarkSessionWriter(b *testing.B) {
	b.ReportAllocs()

	var buf bytes.Buffer
	snappyCodec := parquet.LookupCompressionCodec(format.Snappy)
	parquetWriter := parquet.NewGenericWriter[sessionData](&buf, parquet.Compression(snappyCodec))

	ipInt, err := ipv4ToInt(netip.MustParseAddr("198.51.100.20"))
	if err != nil {
		b.Fatalf("unable to create uint32 from address: %s", err)
	}
	i32IPInt := int32(ipInt) // #nosec G115 -- Used in parquet struct with logical type uint32

	ip6NetworkUint, ip6HostUint, err := ipv6ToInt(netip.MustParseAddr("2001:db8:1122:3344:5566:7788:99aa:bbcc"))
	if err != nil {
		b.Fatalf("unable to create uint64 from ipv6 address: %s", err)
	}
	ip6NetworkInt := int64(ip6NetworkUint) // #nosec G115 -- Used in parquet struct with logical type uint64
	ip6HostInt := int64(ip6HostUint)       // #nosec G115 -- Used in parquet struct with logical type uint64

	sd := sessionData{
		dnsLabels: dnsLabels{
			Label0: new("com"),
			Label1: new("example"),
			Label2: new("www"),
		},
		ServerID:          new("serverID"),
		QueryTime:         new(int64(10)),
		ResponseTime:      new(int64(10)),
		SourceIPv4:        &i32IPInt,
		DestIPv4:          &i32IPInt,
		SourceIPv6Network: &ip6NetworkInt,
		SourceIPv6Host:    &ip6HostInt,
		DestIPv6Network:   &ip6NetworkInt,
		DestIPv6Host:      &ip6HostInt,
		SourcePort:        new(int32(1337)),
		DestPort:          new(int32(1337)),
		DNSProtocol:       new(int32(1)),
		QueryMessage:      new("query message"),
		ResponseMessage:   new("response message"),
	}

	for b.Loop() {
		_, err = parquetWriter.Write([]sessionData{sd})
		if err != nil {
			b.Fatalf("unable to call Write() on parquet writer: %s", err)
		}
	}
	err = parquetWriter.Close()
	if err != nil {
		b.Fatalf("unable to call WriteStop() on parquet writer: %s", err)
	}
}

func TestSessionWriter(t *testing.T) {
	var buf bytes.Buffer

	snappyCodec := parquet.LookupCompressionCodec(format.Snappy)
	parquetWriter := parquet.NewGenericWriter[sessionData](&buf, sessionDataSchema, parquet.Compression(snappyCodec))

	ipInt, err := ipv4ToInt(netip.MustParseAddr("198.51.100.20"))
	if err != nil {
		t.Fatalf("unable to create uint32 from address: %s", err)
	}
	i32IPInt := int32(ipInt) // #nosec G115 -- Used in parquet struct with logical type uint64

	ip6NetworkUint, ip6HostUint, err := ipv6ToInt(netip.MustParseAddr("2001:db8:1122:3344:5566:7788:99aa:bbcc"))
	if err != nil {
		t.Fatalf("unable to create uint64 from ipv6 address: %s", err)
	}

	ip6NetworkInt := int64(ip6NetworkUint) // #nosec G115 -- Used in parquet struct with logical type uint64
	ip6HostInt := int64(ip6HostUint)       // #nosec G115 -- Used in parquet struct with logical type uint64

	sd := sessionData{
		dnsLabels: dnsLabels{
			Label0: new("com"),
			Label1: new("example"),
			Label2: new("www"),
		},
		ServerID:          new("serverID"),
		QueryTime:         new(int64(10)),
		ResponseTime:      new(int64(10)),
		SourceIPv4:        &i32IPInt,
		SourceIPv6Network: &ip6NetworkInt,
		SourceIPv6Host:    &ip6HostInt,
		DestIPv6Network:   &ip6NetworkInt,
		DestIPv6Host:      &ip6HostInt,
		DestIPv4:          &i32IPInt,
		SourcePort:        new(int32(1337)),
		DestPort:          new(int32(1337)),
		DNSProtocol:       new(int32(1)),
		QueryMessage:      new("query message"),
		ResponseMessage:   new("response message"),
	}

	_, err = parquetWriter.Write([]sessionData{sd})
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
	sd := edm.newSession(dt, msg, defaultLabelLimit)
	if sd.ResponseTime == nil || sd.ResponseMessage == nil || sd.ServerID == nil {
		t.Fatalf("session missing response fields: %#v", sd)
	}
	if *sd.ResponseTime != time.Unix(1_700_000_001, 456).UTC().UnixMicro() {
		t.Fatalf("incorrect session response timestamp = %v", dt.Timestamp)
	}
	if sd.SourceIPv4 == nil || sd.DestIPv4 == nil || sd.DNSProtocol == nil {
		t.Fatalf("session missing network fields: %#v", sd)
	}

	var buf bytes.Buffer
	if err := edm.writeSessionParquet(&buf, &prevSessions{sessions: []*sessionData{sd}}); err != nil {
		t.Fatal(err)
	}
	rows, err := parquet.Read[sessionData](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ServerID == nil || *rows[0].ServerID != "server-1" {
		t.Fatalf("unexpected session rows: %#v", rows)
	}

	queryDT := testUnpackedDnstapMessage(t, extdnstap.Message_CLIENT_QUERY, extdnstap.SocketFamily_INET6, packed)
	queryMsg := edm.parsePacket(queryDT)
	querySession := edm.newSession(queryDT, queryMsg, defaultLabelLimit)
	if querySession.QueryTime == nil || querySession.QueryMessage == nil || querySession.SourceIPv6Network == nil || querySession.DestIPv6Host == nil {
		t.Fatalf("query session missing fields: %#v", querySession)
	}
}

// TestNewSessionBranches covers newSession arms that
// TestSessionParquetAndSessionConstruction (basic INET/INET6 happy paths)
// does not reach: port overflow, ipBytesToInt error from bad address
// bytes, ipBytesToInt error from IPv6 bytes carried on an INET family,
// ip6BytesToInt error from bad address bytes, and the unknown
// SocketFamily default arm.
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
		sd := edm.newSession(dt, msg, defaultLabelLimit)
		if sd.SourcePort != nil {
			t.Fatalf("SourcePort = %v, want nil", *sd.SourcePort)
		}
		if sd.DestPort != nil {
			t.Fatalf("DestPort = %v, want nil", *sd.DestPort)
		}
	})

	t.Run("bad INET address bytes logs but does not panic", func(t *testing.T) {
		dt := testUnpackedDnstapMessage(t, extdnstap.Message_CLIENT_RESPONSE, extdnstap.SocketFamily_INET, packed, func(dt *extdnstap.Dnstap) {
			dt.Message.QueryAddress = []byte{1, 2, 3}
			dt.Message.ResponseAddress = []byte{4, 5, 6}
		})
		msg := edm.parsePacket(dt)
		sd := edm.newSession(dt, msg, defaultLabelLimit)
		if sd.SourceIPv4 != nil {
			t.Fatalf("SourceIPv4 should be nil for bad addr bytes, got %v", *sd.SourceIPv4)
		}
		if sd.DestIPv4 != nil {
			t.Fatalf("DestIPv4 should be nil for bad addr bytes, got %v", *sd.DestIPv4)
		}
	})

	t.Run("mismatched IPv6 address bytes with INET family leaves IPv4 nil", func(t *testing.T) {
		dt := testUnpackedDnstapMessage(t, extdnstap.Message_CLIENT_RESPONSE, extdnstap.SocketFamily_INET, packed, func(dt *extdnstap.Dnstap) {
			dt.Message.QueryAddress = netip.MustParseAddr("2001:db8::20").AsSlice()
			dt.Message.ResponseAddress = netip.MustParseAddr("2001:db8::53").AsSlice()
		})
		msg := edm.parsePacket(dt)
		sd := edm.newSession(dt, msg, defaultLabelLimit)
		if sd.SourceIPv4 != nil {
			t.Fatalf("SourceIPv4 should be nil for IPv6 bytes with INET family, got %d", *sd.SourceIPv4)
		}
		if sd.DestIPv4 != nil {
			t.Fatalf("DestIPv4 should be nil for IPv6 bytes with INET family, got %d", *sd.DestIPv4)
		}
	})

	t.Run("bad INET6 address bytes logs but does not panic", func(t *testing.T) {
		dt := testUnpackedDnstapMessage(t, extdnstap.Message_CLIENT_RESPONSE, extdnstap.SocketFamily_INET6, packed, func(dt *extdnstap.Dnstap) {
			dt.Message.QueryAddress = []byte{1, 2, 3}
			dt.Message.ResponseAddress = []byte{4, 5, 6}
		})
		msg := edm.parsePacket(dt)
		sd := edm.newSession(dt, msg, defaultLabelLimit)
		if sd.SourceIPv6Network != nil {
			t.Fatalf("SourceIPv6Network should be nil for bad addr bytes")
		}
		if sd.DestIPv6Network != nil {
			t.Fatalf("DestIPv6Network should be nil for bad addr bytes")
		}
	})

	t.Run("unknown socket family logs and leaves IPs nil", func(t *testing.T) {
		dt := testUnpackedDnstapMessage(t, extdnstap.Message_CLIENT_RESPONSE, extdnstap.SocketFamily_INET, packed, func(dt *extdnstap.Dnstap) {
			unknown := extdnstap.SocketFamily(99)
			dt.Message.SocketFamily = &unknown
		})
		msg := edm.parsePacket(dt)
		sd := edm.newSession(dt, msg, defaultLabelLimit)
		if sd.SourceIPv4 != nil || sd.SourceIPv6Network != nil {
			t.Fatal("expected no IP fields populated for unknown family")
		}
	})

	t.Run("empty identity leaves ServerID nil", func(t *testing.T) {
		dt := testUnpackedDnstapMessage(t, extdnstap.Message_CLIENT_RESPONSE, extdnstap.SocketFamily_INET, packed, func(dt *extdnstap.Dnstap) {
			dt.Identity = nil
		})
		msg := edm.parsePacket(dt)
		sd := edm.newSession(dt, msg, defaultLabelLimit)
		if sd.ServerID != nil {
			t.Fatalf("ServerID should be nil for empty identity, got %q", *sd.ServerID)
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
	sd := edm.newSession(&dt, msg, defaultLabelLimit)

	if sd.DNSProtocol != nil {
		t.Fatalf("DNSProtocol should be nil when SocketProtocol is missing, have: %d", *sd.DNSProtocol)
	}
	if sd.SourceIPv4 != nil || sd.DestIPv4 != nil ||
		sd.SourceIPv6Network != nil || sd.SourceIPv6Host != nil ||
		sd.DestIPv6Network != nil || sd.DestIPv6Host != nil {
		t.Fatalf("IP fields should stay nil when SocketFamily is missing: %#v", sd)
	}
}
