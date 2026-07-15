package runner

import (
	"bytes"
	"testing"

	extdnstap "github.com/dnstap/golang-dnstap"
)

func TestParsePacket(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)

	t.Run("nil packet", func(t *testing.T) {
		dt := testUnpackedMinimalDnstapMessage(t, false)
		if dt.Message != nil {
			t.Fatal("incorrect message")
		}
		msg := edm.parsePacket(dt)
		if msg != nil {
			t.Fatalf("parsePacket should return nil DNS message when dnstap message is missing, have: %#v", msg)
		}
	})

	t.Run("invalid packet", func(t *testing.T) {
		dt := testUnpackedMinimalDnstapMessage(t, false, func(dt *extdnstap.Dnstap) {
			dt.Message.ResponseMessage = []byte{0x00}
		})
		if !bytes.Equal(dt.Message, []byte{0x00}) {
			t.Fatal("incorrect message")
		}
		badMsg := edm.parsePacket(dt)
		if badMsg != nil {
			t.Fatal("bad response packet returned non-nil message")
		}
	})
}
