package runner

import (
	"io"
	"log/slog"
	"net/netip"
	"testing"
	"time"

	dnstap "github.com/dnstap/golang-dnstap"
	"github.com/miekg/dns"
)

func TestNewSessionIgnoresMismatchedIPv4FamilyAddress(t *testing.T) {
	edm := &dnstapMinimiser{
		log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	msg := new(dns.Msg)
	msg.SetQuestion("example.com.", dns.TypeA)

	socketFamily := dnstap.SocketFamily_INET
	socketProtocol := dnstap.SocketProtocol_UDP
	sd := edm.newSession(&dnstap.Dnstap{
		Message: &dnstap.Message{
			SocketFamily:   &socketFamily,
			SocketProtocol: &socketProtocol,
			QueryAddress:   netip.MustParseAddr("2001:db8::1").AsSlice(),
		},
	}, msg, false, defaultLabelLimit, time.Unix(0, 0).UTC())

	if sd.SourceIPv4 != nil {
		t.Fatalf("SourceIPv4 should stay nil for IPv6 bytes with INET socket family, have: %d", *sd.SourceIPv4)
	}
}
