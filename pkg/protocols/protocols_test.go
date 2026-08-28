package protocols

import (
	"testing"
	"time"

	"codeberg.org/miekg/dns"
	legacydns "github.com/miekg/dns"
)

func TestBitsFromMsgAllFlags(t *testing.T) {
	msg := &dns.Msg{
		MsgHeader: dns.MsgHeader{
			Response:           true,
			Opcode:             dns.OpcodeStatus,
			Authoritative:      true,
			Truncated:          true,
			RecursionDesired:   true,
			RecursionAvailable: true,
			Zero:               true,
			AuthenticatedData:  true,
			CheckingDisabled:   true,
			Rcode:              dns.RcodeNameError,
		},
	}

	got := bitsFromMsg(msg)
	want := uint16(_QR | (dns.OpcodeStatus << 11) | _AA | _TC | _RD | _RA | _Z | _AD | _CD | dns.RcodeNameError)
	if got != want {
		t.Fatalf("bitsFromMsg() = %016b, want %016b", got, want)
	}
}

func TestNewQnameEvent(t *testing.T) {
	msg := dns.NewMsg("example.com.", dns.TypeAAAA)
	msg.RecursionDesired = true
	ts := time.Date(2026, 5, 28, 12, 13, 14, 15, time.UTC)

	got := NewQnameEvent(msg, ts)

	if got.Type != NewQnameJSONType {
		t.Fatalf("Type = %q, want %q", got.Type, NewQnameJSONType)
	}
	if got.Version != NewQnameJSONVersion {
		t.Fatalf("Version = %d, want %d", got.Version, NewQnameJSONVersion)
	}
	if got.Qname != "example.com." {
		t.Fatalf("Qname = %q", got.Qname)
	}
	if got.Qtype == nil || *got.Qtype != int(dns.TypeAAAA) {
		t.Fatalf("Qtype = %v, want %d", got.Qtype, dns.TypeAAAA)
	}
	if got.Qclass == nil || *got.Qclass != int(dns.ClassINET) {
		t.Fatalf("Qclass = %v, want %d", got.Qclass, dns.ClassINET)
	}
	if got.Timestamp == nil || !got.Timestamp.Equal(ts) {
		t.Fatalf("Timestamp = %v, want %v", got.Timestamp, ts)
	}
	if got.Flags == nil || *got.Flags != int(_RD) {
		t.Fatalf("Flags = %v, want %d", got.Flags, _RD)
	}
}

func TestNewQnameEventDomainNameEncoding(t *testing.T) {
	tests := map[string]string{
		`a\.b.example.`:                 "a.b.example.",
		`a\046b.example.`:               "a.b.example.",
		`a\092b.example.`:               "a\\b.example.",
		`\000\007\009\010\031.`:         "\x00\x07\x09\x0a\x1f.",
		`\127\128\173\239\255.example.`: "\x7f\x80\xad\xef\xff.example.",
	}
	for name, want := range tests {
		t.Run(name, func(t *testing.T) {
			legacy := legacydns.Msg{}
			legacy.SetQuestion(name, legacydns.TypeA)
			wire, err := legacy.Pack()
			if err != nil {
				t.Fatal(err)
			}

			msg := new(dns.Msg)
			msg.Data = wire
			msg.Options = dns.MsgOptionUnpackQuestion
			if err := msg.Unpack(); err != nil {
				t.Fatal(err)
			}

			if event := NewQnameEvent(msg, time.Time{}); event.Qname != want {
				t.Fatalf("Qname = %q, want %q", event.Qname, want)
			}
		})
	}
}

func FuzzNewQnameEventDomainNameEncoding(f *testing.F) {
	for _, name := range []string{
		".",
		"example.com.",
		`a\.b.example.`,
		`a\046b.example.`,
		`a\092b.example.`,
		`\000\007\009\010\031.`,
		`\127\128\173\239\255.example.`,
	} {
		f.Add(name)
	}

	f.Fuzz(func(t *testing.T, name string) {
		if _, ok := legacydns.IsDomainName(name); !ok {
			t.Skip()
		}

		legacy := legacydns.Msg{}
		legacy.SetQuestion(name, legacydns.TypeA)
		wire, err := legacy.Pack()
		if err != nil {
			t.Skip()
		}

		msg := new(dns.Msg)
		msg.Data = wire
		msg.Options = dns.MsgOptionUnpackQuestion
		if err := msg.Unpack(); err != nil {
			t.Fatalf("Unpack() = %v", err)
		}
		if len(msg.Question) != 1 {
			t.Fatalf("Question count = %d, want 1", len(msg.Question))
		}

		event := NewQnameEvent(msg, time.Time{})
		if event.Qname != msg.Question[0].Header().Name {
			t.Fatalf("Qname = %q, want %q", event.Qname, msg.Question[0].Header().Name)
		}
	})
}

func TestNewQnameEventEmptyQuestion(t *testing.T) {
	msg := new(dns.Msg)

	ts := time.Date(2026, 4, 29, 12, 0, 0, 0, time.UTC)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("NewQnameEvent panicked with empty Question section: %v", r)
		}
	}()

	event := NewQnameEvent(msg, ts)

	if event.Qname != "" {
		t.Fatalf("Qname have: %q want: %q", event.Qname, "")
	}
	if event.Qtype != nil {
		t.Fatalf("Qtype have: %v want: nil", event.Qtype)
	}
	if event.Qclass != nil {
		t.Fatalf("Qclass have: %v want: nil", event.Qclass)
	}
	if event.Type != NewQnameJSONType {
		t.Fatalf("Type have: %q want: %q", event.Type, NewQnameJSONType)
	}
	if event.Version != NewQnameJSONVersion {
		t.Fatalf("Version have: %d want: %d", event.Version, NewQnameJSONVersion)
	}
	if event.Timestamp == nil || !event.Timestamp.Equal(ts) {
		t.Fatalf("Timestamp have: %v want: %v", event.Timestamp, ts)
	}
	if event.Flags == nil {
		t.Fatal("Flags have: nil want: non-nil")
	}
}
