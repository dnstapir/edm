package protocols

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dnstapir/dnswire"
	"github.com/miekg/dns"
)

func TestNewQnameEvent(t *testing.T) {
	msg := &dnswire.Message{
		Header: dnswire.Header{
			Flags:         1 << 8,
			QuestionCount: 1,
		},
		Question: dnswire.Question{
			Name:  "example.com.",
			Type:  dns.TypeAAAA,
			Class: dns.ClassINET,
		},
	}
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
	if got.Flags == nil || *got.Flags != 1<<8 {
		t.Fatalf("Flags = %v, want %d", got.Flags, 1<<8)
	}
}

func TestNewQnameEventPreservesFlags(t *testing.T) {
	msg := &dnswire.Message{Header: dnswire.Header{Flags: 0xffff}}

	event := NewQnameEvent(msg, time.Time{})
	if event.Flags == nil || *event.Flags != 0xffff {
		t.Fatalf("Flags = %v, want %d", event.Flags, 0xffff)
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
		{
			name: "maximum length",
			labels: [][]byte{
				bytes.Repeat([]byte{0}, 63),
				bytes.Repeat([]byte{0}, 63),
				bytes.Repeat([]byte{0}, 63),
				bytes.Repeat([]byte{0}, 61),
			},
			want: strings.Repeat(`\000`, 63) + "." +
				strings.Repeat(`\000`, 63) + "." +
				strings.Repeat(`\000`, 63) + "." +
				strings.Repeat(`\000`, 61) + ".",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := decodedMessage(t, questionPacket(wireName(tc.labels...)), tc.want)
			event := NewQnameEvent(&msg, time.Time{})
			if event.Qname != tc.want {
				t.Fatalf("Qname = %q, want %q", event.Qname, tc.want)
			}
		})
	}
}

func TestDomainNameCompressionEncoding(t *testing.T) {
	data := compressedQuestionPair(wireName([]byte("a.b"), []byte{0, 255}), []byte("prefix"))
	msg, err := dnswire.Parse(data)
	if err != nil {
		t.Fatal(err)
	}

	want := []string{`a\.b.\000\255.`, `prefix.a\.b.\000\255.`}
	i := 0
	for question := range msg.Questions {
		if i == len(want) {
			t.Fatalf("decoded more than %d questions", len(want))
		}
		if question.Name != want[i] {
			t.Errorf("Questions[%d].Name = %q, want %q", i, question.Name, want[i])
		}
		i++
	}
	if i != len(want) {
		t.Fatalf("decoded %d questions, want %d", i, len(want))
	}
}

func TestHistoricBitStringDomainNameEncoding(t *testing.T) {
	tests := []struct {
		name  string
		count byte
		data  []byte
		want  string
	}{
		{name: "fourteen bits", count: 14, data: []byte{0xd0, 0x74}, want: `\[xd074/14].`},
		{name: "padding bits ignored", count: 1, data: []byte{0xff}, want: `\[x8/1].`},
		{name: "256 bits", data: make([]byte, 32), want: `\[x` + strings.Repeat("0", 64) + `/256].`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			name := append([]byte{0x41, tc.count}, tc.data...)
			name = append(name, 0)
			msg := decodedMessage(t, questionPacket(name), tc.want)
			event := NewQnameEvent(&msg, time.Time{})
			if event.Qname != tc.want {
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
		{first: []byte("a.b"), second: []byte(`a\b`)},
		{first: []byte{0, 7, 31}, second: []byte{127, 128, 255}},
		{},
	} {
		f.Add(labels.first, labels.second)
	}

	f.Fuzz(func(t *testing.T, first, second []byte) {
		if len(first) > 63 || len(second) > 63 {
			t.Skip()
		}
		labels := make([][]byte, 0, 2)
		if len(first) != 0 {
			labels = append(labels, first)
		}
		if len(second) != 0 {
			labels = append(labels, second)
		}

		name := wireName(labels...)
		want := presentationName(labels...)
		for _, data := range [][]byte{questionPacket(name), compressedQuestionPair(name, nil)} {
			msg := decodedMessage(t, data, want)
			if event := NewQnameEvent(&msg, time.Time{}); event.Qname != want {
				t.Fatalf("Qname = %q, want %q", event.Qname, want)
			}
		}
	})
}

func FuzzHistoricBitStringDomainNameEncoding(f *testing.F) {
	f.Add(uint8(14), []byte{0xd0, 0x74})
	f.Add(uint8(1), []byte{0xff})
	f.Add(uint8(0), make([]byte, 32))

	f.Fuzz(func(t *testing.T, count uint8, input []byte) {
		bits := int(count)
		if bits == 0 {
			bits = 256
		}
		payload := make([]byte, (bits+7)/8)
		copy(payload, input)
		name := append([]byte{0x41, count}, payload...)
		name = append(name, 7, 'e', 'x', 'a', 'm', 'p', 'l', 'e', 0)
		want := bitStringPresentation(payload, bits) + "example."

		msg := decodedMessage(t, compressedQuestionPair(name, nil), want)
		if event := NewQnameEvent(&msg, time.Time{}); event.Qname != want {
			t.Fatalf("Qname = %q, want %q", event.Qname, want)
		}
	})
}

func TestNewQnameEventEmptyQuestion(t *testing.T) {
	msg := new(dnswire.Message)
	ts := time.Date(2026, 4, 29, 12, 0, 0, 0, time.UTC)

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

func decodedMessage(t testing.TB, data []byte, want string) dnswire.Message {
	t.Helper()

	msg, err := dnswire.Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	questions := 0
	for question := range msg.Questions {
		questions++
		if question.Name != want {
			t.Fatalf("decoded name = %q, want %q", question.Name, want)
		}
	}
	if questions != int(msg.Header.QuestionCount) {
		t.Fatalf("decoded %d questions, want %d", questions, msg.Header.QuestionCount)
	}
	return msg
}

func questionPacket(name []byte) []byte {
	data := make([]byte, dnswire.HeaderSize)
	binary.BigEndian.PutUint16(data[4:6], 1)
	data = append(data, name...)
	data = binary.BigEndian.AppendUint16(data, dns.TypeA)
	return binary.BigEndian.AppendUint16(data, dns.ClassINET)
}

func compressedQuestionPair(name, prefix []byte) []byte {
	data := make([]byte, dnswire.HeaderSize)
	binary.BigEndian.PutUint16(data[4:6], 2)
	data = append(data, name...)
	data = binary.BigEndian.AppendUint16(data, dns.TypeA)
	data = binary.BigEndian.AppendUint16(data, dns.ClassINET)
	if len(prefix) != 0 {
		if len(prefix) > 63 {
			panic("invalid test label length")
		}
		data = append(data, byte(len(prefix))) // #nosec G115 -- prefix length is bounded by the DNS label limit above.
		data = append(data, prefix...)
	}
	data = append(data, 0xc0, dnswire.HeaderSize)
	data = binary.BigEndian.AppendUint16(data, dns.TypeA)
	return binary.BigEndian.AppendUint16(data, dns.ClassINET)
}

func wireName(labels ...[]byte) []byte {
	var data []byte
	for _, label := range labels {
		if len(label) == 0 || len(label) > 63 {
			panic("invalid test label length")
		}
		data = append(data, byte(len(label))) // #nosec G115 -- label length is bounded by the DNS label limit above.
		data = append(data, label...)
	}
	return append(data, 0)
}

// presentationName renders the RFC 1035 section 5.1 form independently of dnswire.
func presentationName(labels ...[]byte) string {
	if len(labels) == 0 {
		return "."
	}

	var name strings.Builder
	for _, label := range labels {
		for _, value := range label {
			switch value {
			case '.', ' ', '\'', '@', ';', '(', ')', '"', '\\':
				name.WriteByte('\\')
				name.WriteByte(value)
			default:
				if value < ' ' || value > '~' {
					name.WriteByte('\\')
					name.WriteByte('0' + value/100)
					name.WriteByte('0' + value/10%10)
					name.WriteByte('0' + value%10)
				} else {
					name.WriteByte(value)
				}
			}
		}
		name.WriteByte('.')
	}
	return name.String()
}

// bitStringPresentation renders the RFC 2673 section 3.2 hexadecimal form independently of dnswire.
func bitStringPresentation(data []byte, bits int) string {
	canonical := bytes.Clone(data)
	if unusedBits := len(canonical)*8 - bits; unusedBits != 0 {
		canonical[len(canonical)-1] &= ^byte((1 << unusedBits) - 1)
	}
	digits := hex.EncodeToString(canonical)
	digits = digits[:(bits+3)/4]
	return `\[x` + digits + `/` + strconv.Itoa(bits) + `].`
}
