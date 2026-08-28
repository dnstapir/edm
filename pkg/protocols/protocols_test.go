package protocols

import (
	"strings"
	"testing"
	"time"

	"codeberg.org/miekg/dns"
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
	tests := []struct {
		name   string
		labels [][]byte
		want   string
	}{
		{name: "root", want: "."},
		{name: "embedded dot", labels: [][]byte{[]byte("a.b"), []byte("example")}, want: `a\.b.example.`},
		{name: "backslash", labels: [][]byte{[]byte(`a\b`), []byte("example")}, want: `a\\b.example.`},
		{name: "special ASCII", labels: [][]byte{[]byte("a b'@;()\"")}, want: `a\ b\'\@\;\(\)\".`},
		{name: "control octets", labels: [][]byte{{0, 7, 9, 10, 31}}, want: `\000\007\009\010\031.`},
		{name: "high octets", labels: [][]byte{{127, 128, 173, 239, 255}, []byte("example")}, want: `\127\128\173\239\255.example.`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := &dns.Msg{Data: packedQuestion(tc.labels...)}
			msg.Options = dns.MsgOptionUnpackQuestion
			if err := msg.Unpack(); err != nil {
				t.Fatal(err)
			}

			if event := NewQnameEvent(msg, time.Time{}); event.Qname != tc.want {
				t.Fatalf("Qname = %q, want %q", event.Qname, tc.want)
			}
		})
	}
}

func FuzzNewQnameEventDomainNameEncoding(f *testing.F) {
	for _, labels := range []struct {
		first  []byte
		second []byte
	}{
		{first: []byte("example"), second: []byte("com")},
		{first: []byte("a.b"), second: []byte("example")},
		{first: []byte(`a\b`), second: []byte("example")},
		{first: []byte{0, 7, 9, 10, 31}, second: []byte{127, 128, 173, 239, 255}},
	} {
		f.Add(labels.first, labels.second)
	}

	f.Fuzz(func(t *testing.T, first, second []byte) {
		if len(first) == 0 || len(first) > 63 || len(second) == 0 || len(second) > 63 {
			t.Skip()
		}

		msg := &dns.Msg{Data: packedQuestion(first, second)}
		msg.Options = dns.MsgOptionUnpackQuestion
		if err := msg.Unpack(); err != nil {
			t.Fatalf("Unpack() = %v", err)
		}
		if len(msg.Question) != 1 {
			t.Fatalf("Question count = %d, want 1", len(msg.Question))
		}

		event := NewQnameEvent(msg, time.Time{})
		want := presentationName(first, second)
		if event.Qname != want {
			t.Fatalf("Qname = %q, want %q", event.Qname, want)
		}
	})
}

func packedQuestion(labels ...[]byte) []byte {
	data := make([]byte, dns.MsgHeaderSize)
	data[5] = 1
	for _, label := range labels {
		if len(label) > 63 {
			panic("DNS label exceeds 63 octets")
		}
		data = append(data, byte(len(label))) // #nosec G115 -- length is bounded by the DNS label limit above
		data = append(data, label...)
	}
	return append(data, 0, 0, byte(dns.TypeA), 0, byte(dns.ClassINET))
}

func presentationName(labels ...[]byte) string {
	if len(labels) == 0 {
		return "."
	}

	// RFC 1035 section 5.1 uses \X for special characters and three-digit
	// decimal \DDD for octets outside display ASCII.
	var name strings.Builder
	for _, label := range labels {
		for _, b := range label {
			switch b {
			case '.', ' ', '\'', '@', ';', '(', ')', '"', '\\':
				name.WriteByte('\\')
				name.WriteByte(b)
			default:
				if b < ' ' || b > '~' {
					name.WriteByte('\\')
					name.WriteByte('0' + b/100)
					name.WriteByte('0' + b/10%10)
					name.WriteByte('0' + b%10)
				} else {
					name.WriteByte(b)
				}
			}
		}
		name.WriteByte('.')
	}
	return name.String()
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
