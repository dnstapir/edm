package runner

import (
	"io"
	"log/slog"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"

	dnstap "github.com/dnstap/golang-dnstap"
	"github.com/miekg/dns"
)

// The tests in this file exercise the lock-free reload paths introduced in
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
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	edm, err := newDnstapMinimiser(logger, defaultTC)
	if err != nil {
		t.Fatalf("newDnstapMinimiser: %s", err)
	}
	t.Cleanup(func() { cleanupTestMinimiser(edm) })

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
				dt := &dnstap.Dnstap{
					Message: &dnstap.Message{
						QueryAddress: addr.AsSlice(),
					},
				}
				_ = edm.clientIPIsIgnored(dt)
				_ = edm.getNumIgnoredClientCIDRs()
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
// design note on the dnstapMinimiser struct in runner.go.
//
// As with the IP test the assertion is purely "no race, no panic". A
// future change that, say, reintroduced ignoredQuestionsMutex without
// updating readers would either deadlock (test would time out) or race
// (race detector would fail).
func TestConcurrentIgnoredQuestionsReload(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	edm, err := newDnstapMinimiser(logger, defaultTC)
	if err != nil {
		t.Fatalf("newDnstapMinimiser: %s", err)
	}
	t.Cleanup(func() { cleanupTestMinimiser(edm) })

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

// TestConcurrentSetCryptopanReload exercises the lock-free Crypto-PAn
// rotation path: workers Load the cryptopan pointer and the generation
// counter on the hot path, while a writer rotates the key. The atomic
// store for the pointer plus the atomic add for the generation must
// synchronise so a worker that sees the new generation also observes the
// new pointer (otherwise it would Purge its cache and then immediately
// re-fill it from the *old* cryptopan, defeating the rotation).
//
// We don't assert the exact ordering here - that is what -race plus the
// memory-model guarantees of atomic.Store/atomic.Add are for. What we
// do assert is the strict invariant: every observed generation bump
// corresponds to a non-nil cryptopan, and the pointer never reverts to
// nil mid-run.
func TestConcurrentSetCryptopanReload(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	edm, err := newDnstapMinimiser(logger, defaultTC)
	if err != nil {
		t.Fatalf("newDnstapMinimiser: %s", err)
	}
	t.Cleanup(func() { cleanupTestMinimiser(edm) })

	var (
		stop atomic.Bool
		wg   sync.WaitGroup
	)

	const numReaders = 4
	wg.Add(numReaders)
	for range numReaders {
		go func() {
			defer wg.Done()
			for !stop.Load() {
				cpn := edm.cryptopan.Load()
				gen := edm.cryptopanGen.Load()
				// The minimiser construction installs an initial
				// instance and the writer below only ever stores
				// non-nil pointers via setCryptopan - so observing nil
				// at any point is a contract violation.
				if cpn == nil {
					t.Errorf("cryptopan pointer observed as nil at gen=%d", gen)
					return
				}
			}
		}()
	}

	// Rotate the cryptopan instance several times. setCryptopan is
	// expensive (argon2 KDF, ~100–200ms per call) so we keep the count
	// modest - the readers still spin tens of thousands of Loads in
	// that window, which is plenty for the race detector to catch any
	// regression. The salt stays constant; only the key changes so each
	// call installs a distinct instance.
	const rotations = 20
	for i := range rotations {
		key := "rotation-key-"
		// Avoid a strconv import dependency for this test by composing
		// short keys. The exact value doesn't matter - only that it
		// changes each iteration.
		key += string(rune('a' + (i % 26)))
		key += string(rune('a' + ((i / 26) % 26)))
		if err := edm.setCryptopan(key, defaultTC.CryptopanKeySalt, defaultTC.CryptopanAddressEntries); err != nil {
			stop.Store(true)
			wg.Wait()
			t.Fatalf("rotation %d: setCryptopan: %s", i, err)
		}
	}

	stop.Store(true)
	wg.Wait()

	// After the writer has finished the generation must reflect every
	// successful rotation - strictly monotonic, no skipped/dropped
	// increments. The +1 accounts for the setCryptopan call inside
	// newDnstapMinimiser.
	wantGen := uint64(rotations + 1)
	if got := edm.cryptopanGen.Load(); got != wantGen {
		t.Fatalf("final cryptopanGen have: %d, want: %d", got, wantGen)
	}
}

// TestQuestionIsIgnoredMultipleQuestions documents the explicit "any
// matches" policy in questionIsIgnored when a DNS message carries more
// than one question. The runner.go comment states: "if there happens to
// be multiple questions in the packet we consider the message ignored if
// any of them matches" - but no existing test exercises a multi-question
// message, so a future refactor that, say, only inspected msg.Question[0]
// would silently regress with no test failure.
//
// In practice DNS messages with QDCOUNT > 1 are extremely rare and most
// recursors reject them, but the code intentionally handles the case;
// this test pins the behaviour.
func TestQuestionIsIgnoredMultipleQuestions(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	edm, err := newDnstapMinimiser(logger, defaultTC)
	if err != nil {
		t.Fatalf("newDnstapMinimiser: %s", err)
	}
	t.Cleanup(func() { cleanupTestMinimiser(edm) })

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

// TestClientIPIsIgnoredEmptyQueryAddress documents the deliberate
// "fail-closed" behaviour for unparseable QueryAddress slices when an
// ignore list is active: the packet is treated as ignored and an error
// counter is incremented. Existing TestIgnoredClientIPsInvalidClient
// covers a 5-byte slice; this complements it with the nil and empty-slice
// cases, which take the same code path through netip.AddrFromSlice but
// are easy to overlook in future refactors that try to "optimise" the
// nil check.
//
// The behaviour matters because production EDM applies the ignore list
// before any further parsing - silently allowing a packet with no
// QueryAddress through would defeat operator policy.
func TestClientIPIsIgnoredEmptyQueryAddress(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	edm, err := newDnstapMinimiser(logger, defaultTC)
	if err != nil {
		t.Fatalf("newDnstapMinimiser: %s", err)
	}
	t.Cleanup(func() { cleanupTestMinimiser(edm) })

	// Active list - exercise the fail-closed path.
	edm.conf.IgnoredClientIPsFile = "testdata/ignored-client-ips.valid1"
	if err := edm.setIgnoredClientIPs(); err != nil {
		t.Fatalf("setIgnoredClientIPs: %s", err)
	}

	cases := []struct {
		name string
		addr []byte
	}{
		{"nil QueryAddress", nil},
		{"zero-length QueryAddress", []byte{}},
		// already covered by TestIgnoredClientIPsInvalidClient but
		// included here for symmetry / regression-safety in this file.
		{"odd-length QueryAddress", make([]byte, 7)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := &dnstap.Dnstap{Message: &dnstap.Message{QueryAddress: tc.addr}}
			if got := edm.clientIPIsIgnored(dt); !got {
				t.Fatalf("with active ignore list: have: %t, want: true (fail-closed)", got)
			}
		})
	}

	// With NO active list, an unparseable QueryAddress must NOT be
	// treated as ignored - the early-return on a nil ipset is what
	// allows the rest of the pipeline to handle (or log) the malformed
	// packet itself rather than silently dropping it.
	edm.conf.IgnoredClientIPsFile = "testdata/ignored-client-ips.empty"
	if err := edm.setIgnoredClientIPs(); err != nil {
		t.Fatalf("setIgnoredClientIPs(empty): %s", err)
	}
	for _, tc := range cases {
		t.Run("inactive/"+tc.name, func(t *testing.T) {
			dt := &dnstap.Dnstap{Message: &dnstap.Message{QueryAddress: tc.addr}}
			if got := edm.clientIPIsIgnored(dt); got {
				t.Fatalf("with no ignore list: have: %t, want: false", got)
			}
		})
	}
}

