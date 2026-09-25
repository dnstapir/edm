package runner

import (
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"

	extdnstap "github.com/dnstap/golang-dnstap"
	"github.com/miekg/dns"
)

// The tests in this file exercise the lock-free reload paths across the
// runner subsystems:
// ignored-IP and ignored-question lookups read
// atomic.Pointer snapshots on the hot path with no mutex, and reload
// writers atomic.Store fresh values. They are designed to fail under
// `go test -race` if a future change accidentally reintroduces unsynchronised
// access - for example, by replacing the atomic.Pointer with a bare
// pointer field.
//
// They do *not* try to assert what value a reader sees mid-reload (that
// is intentionally racy at the value level, just not at the memory-model
// level); they only assert that the readers and the writer can run
// concurrently without panicking and without the race detector flagging
// the access.

// TestConcurrentIgnoredClientIPsReload reloads the ignored client IP set
// while a fleet of readers calls clientIPIsIgnored. Each reader uses a
// mix of IPv4 and IPv6 addresses, including some that may or may not be
// in the set depending on which reload was most recent.
//
// Run under -race to catch any unsynchronised access to the IPSet pointer
// or the CIDR count.
func TestConcurrentIgnoredClientIPsReload(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)

	// Prime the set so readers don't all hit the early-return nil path.
	edm.conf.IgnoredClientIPsFile = "testdata/ignored-client-ips.valid1"
	if err := edm.setIgnoredClientIPs(); err != nil {
		t.Fatalf("initial setIgnoredClientIPs: %s", err)
	}

	// We alternate between two valid files plus the empty file (which
	// stores nil), so readers exercise both the populated- and nil-
	// snapshot paths.
	files := []string{
		"testdata/ignored-client-ips.valid1",
		"testdata/ignored-client-ips.valid2",
		"testdata/ignored-client-ips.empty",
	}

	addrs := []netip.Addr{
		netip.MustParseAddr("127.0.0.1"),
		netip.MustParseAddr("127.0.0.3"),
		netip.MustParseAddr("10.10.8.5"),
		netip.MustParseAddr("198.51.100.10"),
		netip.MustParseAddr("::1"),
		netip.MustParseAddr("::3"),
		netip.MustParseAddr("2001:db8:0010:0011::10"),
	}

	var (
		stop atomic.Bool
		wg   sync.WaitGroup
	)

	// Start readers. Each reader spins clientIPIsIgnored across the
	// address mix. We discard the result - what matters is that the call
	// returns and -race observes no unsynchronised reads.
	const numReaders = 8
	wg.Add(numReaders)
	for r := range numReaders {
		go func(seed int) {
			defer wg.Done()
			i := seed
			for !stop.Load() {
				addr := addrs[i%len(addrs)]
				dt := testUnpackedMinimalDnstapMessage(t, addr.Is6(), func(dt *extdnstap.Dnstap) {
					dt.Message.QueryAddress = addr.AsSlice()
				})
				_ = edm.clientIPIsIgnored(dt)
				i++
			}
		}(r)
	}

	// Single writer: rotate the configured file and call
	// setIgnoredClientIPs. We do a fixed number of rotations rather than
	// running for a wall-clock duration so the test is deterministic
	// under load and slow CI runners.
	const rotations = 200
	for i := range rotations {
		edm.conf.IgnoredClientIPsFile = files[i%len(files)]
		if err := edm.setIgnoredClientIPs(); err != nil {
			stop.Store(true)
			wg.Wait()
			t.Fatalf("rotation %d: setIgnoredClientIPs(%s): %s", i, edm.conf.IgnoredClientIPsFile, err)
		}
	}

	stop.Store(true)
	wg.Wait()
}

// TestConcurrentIgnoredQuestionsReload mirrors the IP test above but for
// the DAWG-backed ignored-question set, which is stored in an
// atomic.Pointer[dawgFinderHolder]. The wrapper exists because dawg.Finder
// is an interface and atomic.Pointer wants a concrete type - see the
// design note on the DnstapMinimiser struct.
//
// As with the IP test the assertion is purely "no race, no panic". A
// future change that, say, reintroduced ignoredQuestionsMutex without
// updating readers would either deadlock (test would time out) or race
// (race detector would fail).
func TestConcurrentIgnoredQuestionsReload(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)

	// Prime so readers exercise the non-nil snapshot branch initially.
	edm.conf.IgnoredQuestionNamesFile = "testdata/ignored-question-names.valid1.dawg"
	if err := edm.setIgnoredQuestionNames(); err != nil {
		t.Fatalf("initial setIgnoredQuestionNames: %s", err)
	}

	files := []string{
		"testdata/ignored-question-names.valid1.dawg",
		"testdata/ignored-question-names.valid2.dawg",
		"testdata/ignored-question-names.empty.dawg", // empty maps to nil holder
	}

	questions := []string{
		"example.com.",
		"www.example.net.",
		"www.example.org.",
		"www.example.edu.",
		"unrelated.invalid.",
	}

	var (
		stop atomic.Bool
		wg   sync.WaitGroup
	)

	const numReaders = 8
	wg.Add(numReaders)
	for r := range numReaders {
		go func(seed int) {
			defer wg.Done()
			i := seed
			for !stop.Load() {
				m := new(dns.Msg)
				m.SetQuestion(questions[i%len(questions)], dns.TypeA)
				_ = edm.questionIsIgnored(m)
				i++
			}
		}(r)
	}

	const rotations = 200
	for i := range rotations {
		edm.conf.IgnoredQuestionNamesFile = files[i%len(files)]
		if err := edm.setIgnoredQuestionNames(); err != nil {
			stop.Store(true)
			wg.Wait()
			t.Fatalf("rotation %d: setIgnoredQuestionNames(%s): %s", i, edm.conf.IgnoredQuestionNamesFile, err)
		}
	}

	stop.Store(true)
	wg.Wait()
}

// TestQuestionIsIgnoredMultipleQuestions documents the explicit "any
// matches" policy in questionIsIgnored when a DNS message carries more
// than one question. The minimiser code states: "if there happens to
// be multiple questions in the packet we consider the message ignored if
// any of them matches" - but no existing test exercises a multi-question
// message, so a future refactor that, say, only inspected msg.Question[0]
// would silently regress with no test failure.
//
// In practice DNS messages with QDCOUNT > 1 are extremely rare and most
// recursors reject them, but the code intentionally handles the case;
// this test pins the behaviour.
func TestQuestionIsIgnoredMultipleQuestions(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)

	edm.conf.IgnoredQuestionNamesFile = "testdata/ignored-question-names.valid1.dawg"
	if err := edm.setIgnoredQuestionNames(); err != nil {
		t.Fatalf("setIgnoredQuestionNames: %s", err)
	}

	// example.com. is in valid1.dawg as an exact match (see existing
	// TestIgnoredQuestionNamesValid). We pair it with a name that is NOT
	// ignored, in both orders, to make sure the loop scans past
	// non-matching questions and does not short-circuit on the first
	// entry.
	tests := []struct {
		name      string
		questions []string
		want      bool
	}{
		{
			name:      "single non-matching question",
			questions: []string{"unrelated.invalid."},
			want:      false,
		},
		{
			name:      "matching question first",
			questions: []string{"example.com.", "unrelated.invalid."},
			want:      true,
		},
		{
			name:      "matching question second",
			questions: []string{"unrelated.invalid.", "example.com."},
			want:      true,
		},
		{
			name:      "no matches in any of multiple questions",
			questions: []string{"unrelated.invalid.", "another.invalid."},
			want:      false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := new(dns.Msg)
			// SetQuestion only handles a single question; build the
			// slice directly to model an unusual multi-question packet.
			m.Question = make([]dns.Question, len(tc.questions))
			for i, q := range tc.questions {
				m.Question[i] = dns.Question{
					Name:   q,
					Qtype:  dns.TypeA,
					Qclass: dns.ClassINET,
				}
			}

			if got := edm.questionIsIgnored(m); got != tc.want {
				t.Fatalf("questionIsIgnored(%v) have: %t, want: %t", tc.questions, got, tc.want)
			}
		})
	}
}
