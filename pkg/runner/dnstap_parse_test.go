package runner

import (
	"testing"

	"github.com/dnstapir/edm/pkg/dnstap"
)

func TestParsePacket(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)

	t.Run("nil packet", func(t *testing.T) {
		msg := edm.parsePacket(&dnstap.Message{})
		if msg != nil {
			t.Fatalf("parsePacket should return nil DNS message when dnstap message is missing, have: %#v", msg)
		}
	})

	t.Run("invalid packet", func(t *testing.T) {
		dt := &dnstap.Message{
			Message: []byte{0x00},
		}
		badMsg := edm.parsePacket(dt)
		if badMsg != nil {
			t.Fatal("bad response packet returned non-nil message")
		}
	})
}
