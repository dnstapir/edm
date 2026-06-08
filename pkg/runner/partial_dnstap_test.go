package runner

import (
	"net/netip"
	"testing"
	"time"

	dnstap "github.com/dnstap/golang-dnstap"
	"github.com/miekg/dns"
)

func TestParsePacketMissingTimestamps(t *testing.T) {
	edm := discardEDM()
	wire := packedDNSMsg(t, "example.com.", dns.TypeA, dns.RcodeSuccess)
	epoch := time.Unix(0, 0).UTC()

	tests := []struct {
		name    string
		isQuery bool
		dt      *dnstap.Dnstap
	}{
		{
			name:    "query timestamp missing",
			isQuery: true,
			dt: &dnstap.Dnstap{
				Message: &dnstap.Message{QueryMessage: wire},
			},
		},
		{
			name:    "response timestamp missing",
			isQuery: false,
			dt: &dnstap.Dnstap{
				Message: &dnstap.Message{ResponseMessage: wire},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg, got := edm.parsePacket(tc.dt, tc.isQuery)
			if msg == nil {
				t.Fatal("parsePacket returned nil DNS message")
			}
			if !got.Equal(epoch) {
				t.Fatalf("timestamp have: %s, want: %s", got, epoch)
			}
		})
	}
}

func TestParsePacketMissingMessage(t *testing.T) {
	edm := discardEDM()
	msg, got := edm.parsePacket(&dnstap.Dnstap{}, false)
	if msg != nil {
		t.Fatalf("parsePacket should return nil DNS message when dnstap message is missing, have: %#v", msg)
	}
	if want := time.Unix(0, 0).UTC(); !got.Equal(want) {
		t.Fatalf("timestamp have: %s, want: %s", got, want)
	}
}

func TestNewSessionAllowsMissingSocketMetadata(t *testing.T) {
	edm := discardEDM()
	msg := new(dns.Msg)
	msg.SetQuestion("example.com.", dns.TypeA)

	sd := edm.newSession(&dnstap.Dnstap{
		Message: &dnstap.Message{},
	}, msg, false, defaultLabelLimit, time.Unix(0, 0).UTC())

	if sd.DNSProtocol != nil {
		t.Fatalf("DNSProtocol should be nil when SocketProtocol is missing, have: %d", *sd.DNSProtocol)
	}
	if sd.SourceIPv4 != nil || sd.DestIPv4 != nil ||
		sd.SourceIPv6Network != nil || sd.SourceIPv6Host != nil ||
		sd.DestIPv6Network != nil || sd.DestIPv6Host != nil {
		t.Fatalf("IP fields should stay nil when SocketFamily is missing: %#v", sd)
	}
}

func TestFormatDnstapEndpoint(t *testing.T) {
	addr := netip.MustParseAddr("198.51.100.20").AsSlice()
	port := uint32(12345)

	tests := []struct {
		name    string
		ipBytes []byte
		port    *uint32
		want    string
	}{
		{name: "address and port", ipBytes: addr, port: &port, want: "198.51.100.20:12345"},
		{name: "address without port", ipBytes: addr, port: nil, want: "198.51.100.20:?"},
		{name: "port without address", ipBytes: nil, port: &port, want: "?:12345"},
		{name: "neither address nor port", ipBytes: nil, port: nil, want: "?"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := formatDnstapEndpoint(tc.ipBytes, tc.port); got != tc.want {
				t.Fatalf("endpoint have: %s, want: %s", got, tc.want)
			}
		})
	}
}
