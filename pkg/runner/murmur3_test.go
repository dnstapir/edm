package runner

import (
	"net/netip"
	"testing"

	"github.com/twmb/murmur3"
)

// TestMurmur3GoldenOutputs pins murmur3.Sum64 outputs for representative
// inputs. The hash feeds HLL sketches that are combined across dnstapir
// components (see the deterministic-seed comment in sendUpdate in wkd.go),
// so its output is a cross-component contract that must never change —
// including across hash library swaps.
//
// Production only hashes 4-byte (IPv4) and 16-byte (IPv6) netip address
// slices; the additional length spread covers the block and tail handling
// paths of the MurmurHash3 x64-128 algorithm.
func TestMurmur3GoldenOutputs(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
		want uint64
	}{
		{"empty", nil, 0x0},
		{"ipv4 192.0.2.1", netip.MustParseAddr("192.0.2.1").AsSlice(), 0x596a50a5a9831dff},
		{"ipv4 198.51.100.20", netip.MustParseAddr("198.51.100.20").AsSlice(), 0x50cc36f917736c57},
		{"ipv4 203.0.113.255", netip.MustParseAddr("203.0.113.255").AsSlice(), 0xd8046d7f364db971},
		{"ipv6 2001:db8::1", netip.MustParseAddr("2001:db8::1").AsSlice(), 0x6abaaee0764bf3c5},
		{"ipv6 2001:db8::20", netip.MustParseAddr("2001:db8::20").AsSlice(), 0x8d2fbf75b678d034},
		{"ipv6 fe80::1", netip.MustParseAddr("fe80::1").AsSlice(), 0x87e7ddf30d74c981},
		{"len1", []byte("a"), 0x85555565f6597889},
		{"len7", []byte("abcdefg"), 0xa6cd2f9fc09ee499},
		{"len8", []byte("abcdefgh"), 0xcc8a0ab037ef8c02},
		{"len15", []byte("abcdefghijklmno"), 0x8abe2451890c2ffb},
		{"len16", []byte("abcdefghijklmnop"), 0xc4ca3ca3224cb723},
		{"len17", []byte("abcdefghijklmnopq"), 0x7564747f88bda657},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := murmur3.Sum64(tc.in); got != tc.want {
				t.Fatalf("murmur3.Sum64(%q) = %#016x, want %#016x", tc.in, got, tc.want)
			}
		})
	}
}
