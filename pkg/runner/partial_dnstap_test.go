package runner

import (
	"io"
	"log/slog"
	"testing"
	"time"

	dnstap "github.com/dnstap/golang-dnstap"
	"github.com/miekg/dns"
)

func TestParsePacketMissingTimestamps(t *testing.T) {
	edm := newPartialDnstapTestMinimiser()
	wire := packedDNSMessage(t, "example.com.")
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

func TestNewSessionAllowsMissingSocketMetadata(t *testing.T) {
	edm := newPartialDnstapTestMinimiser()
	msg := new(dns.Msg)
	msg.SetQuestion("example.com.", dns.TypeA)

	sd := edm.newSession(&dnstap.Dnstap{
		Message: &dnstap.Message{},
	}, msg, false, defaultLabelLimit, time.Unix(0, 0).UTC())

	if sd.DNSProtocol != nil {
		t.Fatalf("DNSProtocol should be nil when SocketProtocol is missing, have: %d", *sd.DNSProtocol)
	}
	if sd.SourceIPv4 != nil || sd.DestIPv4 != nil || sd.SourceIPv6Network != nil || sd.DestIPv6Network != nil {
		t.Fatalf("IP fields should stay nil when SocketFamily is missing: %#v", sd)
	}
}

func TestFormatDnstapEndpointPortWithoutAddress(t *testing.T) {
	port := uint32(12345)
	got := formatDnstapEndpoint(nil, &port)
	if got != "?:12345" {
		t.Fatalf("endpoint have: %s, want: ?:12345", got)
	}
}

func newPartialDnstapTestMinimiser() *dnstapMinimiser {
	return &dnstapMinimiser{
		log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func packedDNSMessage(t *testing.T, qname string) []byte {
	t.Helper()

	msg := new(dns.Msg)
	msg.SetQuestion(qname, dns.TypeA)
	msg.Response = true
	wire, err := msg.Pack()
	if err != nil {
		t.Fatalf("dns message Pack: %s", err)
	}
	return wire
}
