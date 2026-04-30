package protocols

import (
	"testing"
	"time"

	"github.com/miekg/dns"
)

func TestNewQnameEventValid(t *testing.T) {
	msg := new(dns.Msg)
	msg.SetQuestion("example.com.", dns.TypeA)

	ts := time.Date(2026, 4, 29, 12, 0, 0, 0, time.UTC)
	event := NewQnameEvent(msg, ts)

	if event.Qname != "example.com." {
		t.Fatalf("Qname have: %q want: %q", event.Qname, "example.com.")
	}
	if event.Qtype == nil || *event.Qtype != int(dns.TypeA) {
		t.Fatalf("Qtype have: %v want: %d", event.Qtype, dns.TypeA)
	}
	if event.Qclass == nil || *event.Qclass != int(dns.ClassINET) {
		t.Fatalf("Qclass have: %v want: %d", event.Qclass, dns.ClassINET)
	}
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
}
