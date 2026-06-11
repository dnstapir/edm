package runner

import (
	"net/netip"
	"path/filepath"
	"testing"

	dnstap "github.com/dnstap/golang-dnstap"
	"github.com/miekg/dns"
)

func TestIgnoredClientIPsValid(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)

	testdataFile1 := "testdata/ignored-client-ips.valid1"
	testdataFile2 := "testdata/ignored-client-ips.valid2"

	edm.conf.IgnoredClientIPsFile = testdataFile1
	err := edm.setIgnoredClientIPs()
	if err != nil {
		t.Fatalf("unable to parse testdata: %s", err)
	}
	numCIDRs := edm.getNumIgnoredClientCIDRs()

	// Magic value counted by hand
	var expectedNumCIDRs uint64 = 6

	if numCIDRs != expectedNumCIDRs {
		t.Fatalf("unexpected number of CIDRs parsed from '%s': have: %d, want: %d", testdataFile1, numCIDRs, expectedNumCIDRs)
	}

	ipLookupTests := []struct {
		name    string
		ip      netip.Addr
		ignored bool
	}{
		{
			name:    "ignored IPv4 /32 #1",
			ip:      netip.MustParseAddr("127.0.0.1"),
			ignored: true,
		},
		{
			name:    "ignored IPv4 /32 #2",
			ip:      netip.MustParseAddr("127.0.0.2"),
			ignored: true,
		},
		{
			name:    "ignored IPv4 /8 #1",
			ip:      netip.MustParseAddr("10.10.8.5"),
			ignored: true,
		},
		{
			name:    "ignored IPv6 /128 #1",
			ip:      netip.MustParseAddr("::1"),
			ignored: true,
		},
		{
			name:    "ignored IPv6 /128 #2",
			ip:      netip.MustParseAddr("::2"),
			ignored: true,
		},
		{
			name:    "ignored IPv6 /32 #2",
			ip:      netip.MustParseAddr("2001:db8:0010:0011::10"),
			ignored: true,
		},
		{
			name:    "monitored IPv4 #1",
			ip:      netip.MustParseAddr("127.0.0.3"),
			ignored: false,
		},
		{
			name:    "monitored IPv4 #2",
			ip:      netip.MustParseAddr("198.51.100.10"),
			ignored: false,
		},
		{
			name:    "monitored IPv6 #1",
			ip:      netip.MustParseAddr("::3"),
			ignored: false,
		},
		{
			name:    "monitored IPv6 #2",
			ip:      netip.MustParseAddr("3fff:0010:0011::10"),
			ignored: false,
		},
	}

	for _, test := range ipLookupTests {
		dt := &dnstap.Dnstap{
			Message: &dnstap.Message{
				QueryAddress: test.ip.AsSlice(),
			},
		}
		ignored := edm.clientIPIsIgnored(dt)

		if ignored != test.ignored {
			t.Fatalf("%s: (lookup for '%s'), have: %t, want: %t", test.name, test.ip, ignored, test.ignored)
		}
	}

	// Load a new file and make sure older ignored IPs are no longer ignored
	edm.conf.IgnoredClientIPsFile = testdataFile2
	err = edm.setIgnoredClientIPs()
	if err != nil {
		t.Fatalf("unable to parse testdata: %s", err)
	}
	numCIDRs = edm.getNumIgnoredClientCIDRs()

	if numCIDRs != expectedNumCIDRs {
		t.Fatalf("unexpected number of CIDRs parsed from '%s': have: %d, want: %d", testdataFile2, numCIDRs, expectedNumCIDRs)
	}

	ipLookupTests2 := []struct {
		name    string
		ip      netip.Addr
		ignored bool
	}{
		{
			name:    "ignored IPv4 /32 #1",
			ip:      netip.MustParseAddr("127.0.0.1"),
			ignored: false,
		},
		{
			name:    "ignored IPv4 /32 #2",
			ip:      netip.MustParseAddr("127.0.0.2"),
			ignored: false,
		},
		{
			name:    "ignored IPv4 /8 #1",
			ip:      netip.MustParseAddr("10.10.8.5"),
			ignored: false,
		},
		{
			name:    "ignored IPv6 /128 #1",
			ip:      netip.MustParseAddr("::1"),
			ignored: false,
		},
		{
			name:    "ignored IPv6 /128 #2",
			ip:      netip.MustParseAddr("::2"),
			ignored: false,
		},
		{
			name:    "ignored IPv6 /32 #2",
			ip:      netip.MustParseAddr("2001:db8:0010:0011::10"),
			ignored: false,
		},
		{
			name:    "monitored IPv4 #1",
			ip:      netip.MustParseAddr("127.0.0.3"),
			ignored: true,
		},
		{
			name:    "monitored IPv4 #2",
			ip:      netip.MustParseAddr("198.51.100.10"),
			ignored: true,
		},
		{
			name:    "monitored IPv6 #1",
			ip:      netip.MustParseAddr("::3"),
			ignored: true,
		},
		{
			name:    "monitored IPv6 #1",
			ip:      netip.MustParseAddr("::4"),
			ignored: true,
		},
		{
			name:    "monitored IPv6 #2",
			ip:      netip.MustParseAddr("3fff:0010:0011::10"),
			ignored: true,
		},
	}

	for _, test := range ipLookupTests2 {
		dt := &dnstap.Dnstap{
			Message: &dnstap.Message{
				QueryAddress: test.ip.AsSlice(),
			},
		}
		ignored := edm.clientIPIsIgnored(dt)

		if ignored != test.ignored {
			t.Fatalf("%s: (lookup for '%s'), have: %t, want: %t", test.name, test.ip, ignored, test.ignored)
		}
	}
}

func TestIgnoredClientIPsEmptyLinesComments(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)

	testdataFile := "testdata/ignored-client-ips.empty-lines-and-comments"

	edm.conf.IgnoredClientIPsFile = testdataFile
	err := edm.setIgnoredClientIPs()
	if err != nil {
		t.Fatalf("unable to parse testdata: %s", err)
	}
	numCIDRs := edm.getNumIgnoredClientCIDRs()

	// Magic value counted by hand
	var expectedNumCIDRs uint64 = 2

	if numCIDRs != expectedNumCIDRs {
		t.Fatalf("unexpected number of CIDRs parsed from '%s': have: %d, want: %d", testdataFile, numCIDRs, expectedNumCIDRs)
	}

	ipLookupTests := []struct {
		name    string
		ip      netip.Addr
		ignored bool
	}{
		{
			name:    "commented out IPv4 /32",
			ip:      netip.MustParseAddr("127.0.0.1"),
			ignored: false,
		},
		{
			name:    "commented out IPv6 /128",
			ip:      netip.MustParseAddr("::2"),
			ignored: false,
		},
		{
			name:    "ignored IPv4 /32",
			ip:      netip.MustParseAddr("127.0.0.2"),
			ignored: true,
		},
		{
			name:    "ignored IPv6 /128",
			ip:      netip.MustParseAddr("::1"),
			ignored: true,
		},
	}

	for _, test := range ipLookupTests {
		dt := &dnstap.Dnstap{
			Message: &dnstap.Message{
				QueryAddress: test.ip.AsSlice(),
			},
		}
		ignored := edm.clientIPIsIgnored(dt)

		if ignored != test.ignored {
			t.Fatalf("%s: (lookup for '%s'), have: %t, want: %t", test.name, test.ip, ignored, test.ignored)
		}
	}
}

func TestIgnoredClientIPsEmpty(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)

	testdataFile := "testdata/ignored-client-ips.valid1"
	// To make sure reading an empty file resets stuff as expected first read in a file with content
	edm.conf.IgnoredClientIPsFile = testdataFile
	err := edm.setIgnoredClientIPs()
	if err != nil {
		t.Fatalf("unable to parse testdata: %s", err)
	}

	// Magic value counted by hand
	expectedValidNumCIDRs := 2

	// Make sure we actually got anything loaded from the file with content
	if edm.ignoredClientsIPSet.Load() == nil {
		t.Fatalf("edm.ignoredClientsIPSet parsed from '%s' should not be nil", testdataFile)
	}
	if edm.getNumIgnoredClientCIDRs() < 1 {
		t.Fatalf("unexpected number of CIDRs parsed from '%s': have: %d, want: %d", testdataFile, edm.getNumIgnoredClientCIDRs(), expectedValidNumCIDRs)
	}

	testdataFile = "testdata/ignored-client-ips.empty"
	edm.conf.IgnoredClientIPsFile = testdataFile
	err = edm.setIgnoredClientIPs()
	if err != nil {
		t.Fatalf("unable to parse testdata: %s", err)
	}

	// Magic value counted by hand
	var expectedNumCIDRs uint64

	if edm.getNumIgnoredClientCIDRs() != expectedNumCIDRs {
		t.Fatalf("unexpected number of CIDRs parsed from '%s': have: %d, want: %d", testdataFile, edm.getNumIgnoredClientCIDRs(), expectedNumCIDRs)
	}

	if got := edm.ignoredClientsIPSet.Load(); got != nil {
		t.Fatalf("edm.ignoredClientsIPSet should be nil, have: %#v", got)
	}

	ipLookupTests := []struct {
		name    string
		ip      netip.Addr
		ignored bool
	}{
		{
			name:    "monitored IPv4 #1",
			ip:      netip.MustParseAddr("127.0.0.1"),
			ignored: false,
		},
		{
			name:    "monitored IPv4 #2",
			ip:      netip.MustParseAddr("127.0.0.2"),
			ignored: false,
		},
		{
			name:    "monitored IPv6 #1",
			ip:      netip.MustParseAddr("::1"),
			ignored: false,
		},
		{
			name:    "monitored IPv6 #2",
			ip:      netip.MustParseAddr("::2"),
			ignored: false,
		},
	}

	for _, test := range ipLookupTests {
		dt := &dnstap.Dnstap{
			Message: &dnstap.Message{
				QueryAddress: test.ip.AsSlice(),
			},
		}
		ignored := edm.clientIPIsIgnored(dt)

		if ignored != test.ignored {
			t.Fatalf("%s: (lookup for '%s'), have: %t, want: %t", test.name, test.ip, ignored, test.ignored)
		}
	}
}

func TestIgnoredClientIPsUnset(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)

	// To make sure unsetting the filename used for ignored client IPs
	// resets stuff as expected first read in a file with content
	edm.conf.IgnoredClientIPsFile = "testdata/ignored-client-ips.valid1"
	err := edm.setIgnoredClientIPs()
	if err != nil {
		t.Fatalf("unable to parse testdata: %s", err)
	}

	// Now run the function with an empty filename
	edm.conf.IgnoredClientIPsFile = ""
	err = edm.setIgnoredClientIPs()
	if err != nil {
		t.Fatalf("unable to set empty filename: %s", err)
	}
	numCIDRs := edm.getNumIgnoredClientCIDRs()

	// Magic value counted by hand
	var expectedNumCIDRs uint64

	if numCIDRs != expectedNumCIDRs {
		t.Fatalf("unexpected number of CIDRs parsed from '%s': have: %d, want: %d", "", numCIDRs, expectedNumCIDRs)
	}

	ipLookupTests := []struct {
		name    string
		ip      netip.Addr
		ignored bool
	}{
		{
			name:    "monitored IPv4 #1",
			ip:      netip.MustParseAddr("127.0.0.1"),
			ignored: false,
		},
		{
			name:    "monitored IPv4 #2",
			ip:      netip.MustParseAddr("127.0.0.2"),
			ignored: false,
		},
		{
			name:    "monitored IPv6 #1",
			ip:      netip.MustParseAddr("::1"),
			ignored: false,
		},
		{
			name:    "monitored IPv6 #2",
			ip:      netip.MustParseAddr("::2"),
			ignored: false,
		},
	}

	for _, test := range ipLookupTests {
		dt := &dnstap.Dnstap{
			Message: &dnstap.Message{
				QueryAddress: test.ip.AsSlice(),
			},
		}
		ignored := edm.clientIPIsIgnored(dt)

		if ignored != test.ignored {
			t.Fatalf("%s: (lookup for '%s'), have: %t, want: %t", test.name, test.ip, ignored, test.ignored)
		}
	}
}

func TestIgnoredClientIPsInvalidClient(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)

	// Even if we are testing invalid data we still need to have loaded a
	// IP file with at least one valid entry in it to even inspect the
	// value.
	edm.conf.IgnoredClientIPsFile = "testdata/ignored-client-ips.valid1"
	err := edm.setIgnoredClientIPs()
	if err != nil {
		t.Fatalf("unable to parse testdata: %s", err)
	}

	// Create QueryAddress that is neither 4 or 16 bytes as expected by
	// netip.AddrFromSlice() inside edm.clientIPIsIgnored(dt). This broken
	// content should result in the function returning "true" when the
	// IPSet is populated.
	dt := &dnstap.Dnstap{
		Message: &dnstap.Message{
			QueryAddress: make([]byte, 5),
		},
	}
	ignored := edm.clientIPIsIgnored(dt)
	if ignored != true {
		t.Fatalf("invalid QueryAddress:, have: %t, want: %t", ignored, true)
	}

	// Also verify that if we load an empty list this means we are not
	// inspecting client addresses at all so not even broken client
	// addresses are ignored in this case.
	edm.conf.IgnoredClientIPsFile = "testdata/ignored-client-ips.empty"
	err = edm.setIgnoredClientIPs()
	if err != nil {
		t.Fatalf("unable to parse testdata: %s", err)
	}

	ignored = edm.clientIPIsIgnored(dt)
	if ignored != false {
		t.Fatalf("invalid QueryAddress:, have: %t, want: %t", ignored, false)
	}
}

func TestIgnoredQuestionNamesValid(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)

	testdataFile1 := "testdata/ignored-question-names.valid1.dawg"
	testdataFile2 := "testdata/ignored-question-names.valid2.dawg"

	// Magic value counted by hand
	expectedNumNames := 2

	edm.conf.IgnoredQuestionNamesFile = testdataFile1
	err := edm.setIgnoredQuestionNames()
	if err != nil {
		t.Fatalf("unable to parse testdata: %s", err)
	}

	if edm.ignoredQuestions.Load().finder.NumAdded() != expectedNumNames {
		t.Fatalf("unexpected number of names parsed from '%s': have: %d, want: %d", testdataFile1, edm.ignoredQuestions.Load().finder.NumAdded(), expectedNumNames)
	}

	questionLookupTests := []struct {
		name     string
		question string
		ignored  bool
	}{
		{
			name:     "exact match found",
			question: "example.com.",
			ignored:  true,
		},
		{
			name:     "exact match found, case insensitive",
			question: "eXample.com.",
			ignored:  true,
		},
		{
			name:     "exact match not found",
			question: "www.example.com.",
			ignored:  false,
		},
		{
			name:     "suffix match",
			question: "www.example.net.",
			ignored:  true,
		},
		{
			name:     "suffix match",
			question: "wWw.example.net.",
			ignored:  true,
		},
		{
			name:     "more nested suffix match",
			question: "example.www.example.net.",
			ignored:  true,
		},
		{
			name:     "more nested suffix match, case insensitive",
			question: "eXample.www.example.net.",
			ignored:  true,
		},
		{
			name:     "suffix not matched",
			question: "example.net.",
			ignored:  false,
		},
	}

	for _, test := range questionLookupTests {
		m := new(dns.Msg)
		m.SetQuestion(test.question, dns.TypeA)
		ignored := edm.questionIsIgnored(m)

		if ignored != test.ignored {
			t.Fatalf("%s: (lookup for '%s'), have: %t, want: %t", test.name, test.question, ignored, test.ignored)
		}
	}

	// Load a new file and make sure older ignored IPs are no longer ignored
	edm.conf.IgnoredQuestionNamesFile = testdataFile2
	err = edm.setIgnoredQuestionNames()
	if err != nil {
		t.Fatalf("unable to parse testdata: %s", err)
	}

	if edm.ignoredQuestions.Load().finder.NumAdded() != expectedNumNames {
		t.Fatalf("unexpected number of names parsed from '%s': have: %d, want: %d", testdataFile2, edm.ignoredQuestions.Load().finder.NumAdded(), expectedNumNames)
	}

	questionLookupTests2 := []struct {
		name     string
		question string
		ignored  bool
	}{
		{
			name:     "exact match no longer found",
			question: "example.com.",
			ignored:  false,
		},
		{
			name:     "suffix match no longer found",
			question: "www.example.net.",
			ignored:  false,
		},
		{
			name:     "more nested suffix match no longer found",
			question: "example.www.example.net.",
			ignored:  false,
		},
		{
			name:     "exact match found",
			question: "example.org.",
			ignored:  true,
		},
		{
			name:     "exact match not found",
			question: "www.example.org.",
			ignored:  false,
		},
		{
			name:     "suffix match",
			question: "www.example.edu.",
			ignored:  true,
		},
		{
			name:     "more nested suffix match",
			question: "example.www.example.edu.",
			ignored:  true,
		},
		{
			name:     "suffix not matched",
			question: "example.edu.",
			ignored:  false,
		},
	}

	for _, test := range questionLookupTests2 {
		m := new(dns.Msg)
		m.SetQuestion(test.question, dns.TypeA)
		ignored := edm.questionIsIgnored(m)

		if ignored != test.ignored {
			t.Fatalf("%s: (lookup for '%s'), have: %t, want: %t", test.name, test.question, ignored, test.ignored)
		}
	}
}

func TestIgnoredQuestionNamesEmpty(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)

	// To make sure reading an empty file resets stuff as expected first read in a file with content
	testdataFile := "testdata/ignored-question-names.valid1.dawg"
	edm.conf.IgnoredQuestionNamesFile = "testdata/ignored-question-names.valid1.dawg"
	err := edm.setIgnoredQuestionNames()
	if err != nil {
		t.Fatalf("unable to parse testdata: %s", err)
	}

	// Magic value counted by hand
	expectedNumNames := 2

	if edm.ignoredQuestions.Load().finder.NumAdded() != expectedNumNames {
		t.Fatalf("unexpected number of names parsed from '%s': have: %d, want: %d", testdataFile, edm.ignoredQuestions.Load().finder.NumAdded(), expectedNumNames)
	}

	testdataFile = "testdata/ignored-question-names.empty.dawg"
	edm.conf.IgnoredQuestionNamesFile = testdataFile
	err = edm.setIgnoredQuestionNames()
	if err != nil {
		t.Fatalf("unable to parse testdata: %s", err)
	}

	if edm.ignoredQuestions.Load() != nil {
		t.Fatalf("edm.ignoredQuestions should be nil: have: %#v", edm.ignoredQuestions.Load())
	}

	// Try to look for things that was present in the initial valid data
	// that was loaded, none of it should be considered ignored now.
	questionLookupTests := []struct {
		name     string
		question string
		ignored  bool
	}{
		{
			name:     "previous exact match should not be ignored",
			question: "example.com.",
			ignored:  false,
		},
		{
			name:     "previous exact match miss should still be ignored",
			question: "www.example.com.",
			ignored:  false,
		},
		{
			name:     "previous suffix match should not be ignored",
			question: "www.example.net.",
			ignored:  false,
		},
		{
			name:     "previous more nested suffix match should not be ignored",
			question: "example.www.example.net.",
			ignored:  false,
		},
		{
			name:     "previous suffix match misss still ignored",
			question: "example.net.",
			ignored:  false,
		},
	}

	for _, test := range questionLookupTests {
		m := new(dns.Msg)
		m.SetQuestion(test.question, dns.TypeA)
		ignored := edm.questionIsIgnored(m)

		if ignored != test.ignored {
			t.Fatalf("%s: (lookup for '%s'), have: %t, want: %t", test.name, test.question, ignored, test.ignored)
		}
	}
}

func TestIgnoredQuestionNamesUnset(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)

	// To make sure unsetting the filename used for ignored question names
	// resets stuff as expected first read in a file with content
	testdataFile := "testdata/ignored-question-names.valid1.dawg"
	edm.conf.IgnoredQuestionNamesFile = testdataFile
	err := edm.setIgnoredQuestionNames()
	if err != nil {
		t.Fatalf("unable to parse testdata: %s", err)
	}

	// Magic value counted by hand
	expectedNumNames := 2

	if edm.ignoredQuestions.Load().finder.NumAdded() != expectedNumNames {
		t.Fatalf("unexpected number of names parsed from '%s': have: %d, want: %d", testdataFile, edm.ignoredQuestions.Load().finder.NumAdded(), expectedNumNames)
	}

	// Now set an empty filename
	edm.conf.IgnoredQuestionNamesFile = ""
	err = edm.setIgnoredQuestionNames()
	if err != nil {
		t.Fatalf("unable to parse testdata: %s", err)
	}

	if edm.ignoredQuestions.Load() != nil {
		t.Fatalf("edm.ignoredQuestions should be nil: have: %#v", edm.ignoredQuestions.Load())
	}

	// Try to look for things that was present in the initial valid data
	// that was loaded, none of it should be considered ignored now.
	questionLookupTests := []struct {
		name     string
		question string
		ignored  bool
	}{
		{
			name:     "previous exact match should not be ignored",
			question: "example.com.",
			ignored:  false,
		},
		{
			name:     "previous exact match miss should still be ignored",
			question: "www.example.com.",
			ignored:  false,
		},
		{
			name:     "previous suffix match should not be ignored",
			question: "www.example.net.",
			ignored:  false,
		},
		{
			name:     "previous more nested suffix match should not be ignored",
			question: "example.www.example.net.",
			ignored:  false,
		},
		{
			name:     "previous suffix match misss still ignored",
			question: "example.net.",
			ignored:  false,
		},
	}

	for _, test := range questionLookupTests {
		m := new(dns.Msg)
		m.SetQuestion(test.question, dns.TypeA)
		ignored := edm.questionIsIgnored(m)

		if ignored != test.ignored {
			t.Fatalf("%s: (lookup for '%s'), have: %t, want: %t", test.name, test.question, ignored, test.ignored)
		}
	}
}

func TestIgnoredFileErrors(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)

	edm.conf.IgnoredClientIPsFile = filepath.Join(t.TempDir(), "missing")
	if err := edm.setIgnoredClientIPs(); err == nil {
		t.Fatal("missing ignored-client file succeeded")
	}

	edm.conf.IgnoredClientIPsFile = writeTempFile(t, "bad-cidr", []byte("not-a-prefix\n"))
	if err := edm.setIgnoredClientIPs(); err == nil {
		t.Fatal("bad CIDR succeeded")
	}

	edm.conf.IgnoredQuestionNamesFile = filepath.Join(t.TempDir(), "missing.dawg")
	if err := edm.setIgnoredQuestionNames(); err == nil {
		t.Fatal("missing ignored-question file succeeded")
	}
}

// TestSetIgnoredQuestionNamesBranches drives the three code paths in
// setIgnoredQuestionNames that the basic missing-file test in
// TestIgnoredFileErrors does not reach.
func TestSetIgnoredQuestionNamesBranches(t *testing.T) {
	t.Run("empty filename closes existing list", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		// Pre-load a finder so the empty-filename branch has something
		// to close on replace.
		edm.conf.IgnoredQuestionNamesFile = testDawgFile(t, "ignore.example.")
		if err := edm.setIgnoredQuestionNames(); err != nil {
			t.Fatalf("initial load: %v", err)
		}
		if edm.ignoredQuestions.Load() == nil {
			t.Fatal("expected finder loaded; got nil")
		}

		// Unset the filename and reload — the close-on-replace branch
		// fires and ignoredQuestions returns to nil.
		edm.conf.IgnoredQuestionNamesFile = ""
		if err := edm.setIgnoredQuestionNames(); err != nil {
			t.Fatalf("unset reload: %v", err)
		}
		if edm.ignoredQuestions.Load() != nil {
			t.Fatal("expected finder cleared after unset; got non-nil")
		}
	})

	t.Run("empty-byte dawg file treated as unset", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		// Pre-load to verify the close-on-replace branch inside the
		// errEmptyDawgFile arm fires.
		edm.conf.IgnoredQuestionNamesFile = testDawgFile(t, "ignore.example.")
		if err := edm.setIgnoredQuestionNames(); err != nil {
			t.Fatalf("initial load: %v", err)
		}

		// LoadDawgFile returns errEmptyDawgFile for a zero-byte file
		// (dawg.Load would panic), which setIgnoredQuestionNames treats
		// as "unset". Expect nil error and a cleared finder.
		edm.conf.IgnoredQuestionNamesFile = writeTempFile(t, "empty.dawg", nil)
		if err := edm.setIgnoredQuestionNames(); err != nil {
			t.Fatalf("empty file: %v", err)
		}
		if edm.ignoredQuestions.Load() != nil {
			t.Fatal("expected finder cleared for empty file; got non-nil")
		}
	})

	t.Run("dawg with zero names clears finder", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		// A dawg file that's non-empty on disk but has NumAdded()==0
		// goes through dawg.Load successfully and then takes the
		// "else: ignoredQuestions = nil" arm.
		edm.conf.IgnoredQuestionNamesFile = testDawgFile(t)
		if err := edm.setIgnoredQuestionNames(); err != nil {
			t.Fatalf("zero-name dawg: %v", err)
		}
		if edm.ignoredQuestions.Load() != nil {
			t.Fatal("expected nil finder for zero-name dawg; got non-nil")
		}
	})
}
