package runner

import (
	"encoding/binary"
	"net/netip"
	"testing"
	"time"

	dnstap "github.com/dnstap/golang-dnstap"
	"github.com/miekg/dns"
)

func TestParsePacketStopsAfterQuestions(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	wire := packedDNSMsg(t, "example.com.", dns.TypeA, dns.RcodeSuccess)
	binary.BigEndian.PutUint16(wire[6:8], 1)
	dt := testDnstapMessage(t, dnstap.Message_CLIENT_RESPONSE, dnstap.SocketFamily_INET, wire)

	msg, _ := edm.parsePacket(dt, false)
	if msg == nil {
		t.Fatal("parsePacket rejected a valid header and question")
	}
	if msg.Header.AnswerCount != 1 {
		t.Fatalf("AnswerCount = %d, want 1", msg.Header.AnswerCount)
	}
}

// TestParsePacketAddressFormattingBranches drives the address-formatting
// arms of parsePacket that the addr+port-present canary in
// TestSessionParquetAndSessionConstruction does not reach: the
// addr-without-port and all-nil fallbacks, plus the response-parse-error
// path. The port-without-address branch is covered directly by
// TestFormatDnstapEndpoint.
func TestParsePacketAddressFormattingBranches(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	packed := packedDNSMsg(t, "www.example.com.", dns.TypeA, dns.RcodeSuccess)

	t.Run("addr without ports", func(t *testing.T) {
		dt := testDnstapMessage(t, dnstap.Message_CLIENT_RESPONSE, dnstap.SocketFamily_INET, packed)
		dt.Message.QueryPort = nil
		dt.Message.ResponsePort = nil
		if msg, _ := edm.parsePacket(dt, false); msg == nil {
			t.Fatal("parsePacket returned nil msg")
		}
	})

	t.Run("no addresses at all", func(t *testing.T) {
		dt := testDnstapMessage(t, dnstap.Message_CLIENT_RESPONSE, dnstap.SocketFamily_INET, packed)
		dt.Message.QueryAddress = nil
		dt.Message.ResponseAddress = nil
		dt.Message.QueryPort = nil
		dt.Message.ResponsePort = nil
		if msg, _ := edm.parsePacket(dt, false); msg == nil {
			t.Fatal("parsePacket returned nil msg")
		}
	})

	t.Run("response parse error", func(t *testing.T) {
		dt := &dnstap.Dnstap{
			Message: &dnstap.Message{
				ResponseMessage:  []byte{1, 2, 3},
				ResponseTimeSec:  new(uint64(0)),
				ResponseTimeNsec: new(uint32(0)),
			},
		}
		badMsg, _ := edm.parsePacket(dt, false)
		if badMsg != nil {
			t.Fatal("bad response packet returned non-nil message")
		}
	})
}

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
