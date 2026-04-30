package runner

import (
	"testing"

	"github.com/cockroachdb/pebble"
)

func TestSeenQnameWriteOptions(t *testing.T) {
	if got := seenQnameWriteOptions(config{}); got != pebble.NoSync {
		t.Fatalf("default seen-qname write option = %p, want %p", got, pebble.NoSync)
	}

	if got := seenQnameWriteOptions(config{PebbleSync: true}); got != pebble.Sync {
		t.Fatalf("pebble-sync seen-qname write option = %p, want %p", got, pebble.Sync)
	}
}
