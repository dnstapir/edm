package runner

import (
	"bytes"
	"encoding/binary"
	"flag"
	"io"
	"log/slog"
	"net/netip"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	dnstap "github.com/dnstap/golang-dnstap"
	"github.com/miekg/dns"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	"github.com/segmentio/go-hll"
	"github.com/smhanov/dawg"
	"github.com/spaolacci/murmur3"
)

var (
	testDawg     = flag.Bool("test-dawg", false, "perform tests requiring a well-known-domains.dawg file")
	writeParquet = flag.Bool("write-parquet", false, "make parquet tests write out files in testdata directory")
	defaultTC    = testConfiger{
		CryptopanKey:            "key1",
		CryptopanKeySalt:        "aabbccddeeffgghh",
		CryptopanAddressEntries: 10,
		Debug:                   false,
		DisableHistogramSender:  false,
		DisableMQTT:             false,
	}
)

func BenchmarkWKDTLookup(b *testing.B) {
	if !*testDawg {
		b.Skip("skipping benchmark needing well-known-domains.dawg")
	}

	dawgFile := "well-known-domains.dawg"

	_, err := os.Stat(dawgFile)
	if err != nil {
		b.Fatal(err)
	}

	dawgFinder, err := dawg.Load(dawgFile)
	if err != nil {
		b.Error(err)
	}

	wkdTracker, err := newWellKnownDomainsTracker(dawgFinder, time.Time{})
	if err != nil {
		b.Fatal(err)
	}

	m := new(dns.Msg)
	m.SetQuestion("google.com.", dns.TypeA)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		wkdTracker.lookup(m)
	}
}

func BenchmarkSetLabels(b *testing.B) {
	b.ReportAllocs()
	labels := []string{"label0", "label1", "label2", "label3", "label4", "label5", "label6", "label7", "label8", "label9"}
	edm := &dnstapMinimiser{}
	l := dnsLabels{}

	for i := 0; i < b.N; i++ {
		edm.setLabels(labels, 10, &l)
	}
}

func TestWKD(t *testing.T) {
	domainList := []string{
		"example.com.",  // exact match
		".example.net.", // suffix match
	}

	// Make it so dawg Add() does not panic if the containts of domainList
	// is not in alphabetical order
	slices.Sort(domainList)

	dBuilder := dawg.New()

	for _, domain := range domainList {
		dBuilder.Add(domain)
	}

	dFinder := dBuilder.Finish()

	wkdDawgIndexTests := []struct {
		name        string
		domain      string
		found       bool
		suffixMatch bool
	}{
		{
			name:        "found exact match",
			domain:      "example.com.",
			found:       true,
			suffixMatch: false,
		},
		{
			name:        "found exact match, case insensitive",
			domain:      "eXample.com.",
			found:       true,
			suffixMatch: false,
		},
		{
			name:        "missing exact match",
			domain:      "www.example.com.",
			found:       false,
			suffixMatch: false,
		},
		{
			name:        "found suffix match",
			domain:      "www.example.net.",
			found:       true,
			suffixMatch: true,
		},
		{
			name:        "found suffix match, case insensitive",
			domain:      "wWw.eXample.net.",
			found:       true,
			suffixMatch: true,
		},
		{
			name:        "found more nested suffix match",
			domain:      "example.www.example.net.",
			found:       true,
			suffixMatch: true,
		},
		{
			name:        "found more nested suffix match, case insensitive",
			domain:      "eXample.www.example.net.",
			found:       true,
			suffixMatch: true,
		},
		{
			name:        "no match for suffix entry",
			domain:      "example.net.",
			found:       false,
			suffixMatch: false,
		},
	}

	wkd, err := newWellKnownDomainsTracker(dFinder, time.Time{})
	if err != nil {
		t.Fatalf("unable to create well-known domains tracker: %s", err)
	}

	for _, test := range wkdDawgIndexTests {
		m := new(dns.Msg)
		m.SetQuestion(test.domain, dns.TypeA)
		i, suffixMatch := getDawgIndex(wkd.dawgFinder, m.Question[0].Name)

		if test.found && i == dawgNotFound {
			t.Fatalf("%s: expected match %s, but was not found", test.name, test.domain)
		}

		if !test.found && i != dawgNotFound {
			t.Fatalf("%s: expected not match for %s, but it was found", test.name, test.domain)
		}

		if suffixMatch != test.suffixMatch {
			t.Fatalf("%s: suffix match mismatch for %s, expected: %t, have: %t", test.name, test.domain, test.suffixMatch, suffixMatch)
		}
	}

	wkdLookupTests := []struct {
		name   string
		domain string
		known  bool
	}{
		{
			name:   "known IPv4",
			domain: "example.com.",
			known:  true,
		},
		{
			name:   "not known IPv4",
			domain: "www.example.com.",
			known:  false,
		},
		{
			name:   "known IPv6",
			domain: "example.com.",
			known:  true,
		},
		{
			name:   "not known IPv6",
			domain: "www.example.com.",
			known:  false,
		},
	}

	for _, test := range wkdLookupTests {
		m := new(dns.Msg)
		m.SetQuestion(test.domain, dns.TypeA)

		dawgIndex, _, _ := wkd.lookup(m)

		known := dawgIndex != dawgNotFound

		if test.known != known {
			t.Fatalf("%s: unexpected known status, have: %t, want: %t", test.name, known, test.known)
		}
	}
}

func TestIgnoredClientIPsValid(t *testing.T) {
	discardLogger := slog.NewTextHandler(io.Discard, nil)
	logger := slog.New(discardLogger)

	edm, err := newDnstapMinimiser(logger, defaultTC)
	if err != nil {
		t.Fatalf("unable to setup edm: %s", err)
	}

	testdataFile1 := "testdata/ignored-client-ips.valid1"
	testdataFile2 := "testdata/ignored-client-ips.valid2"

	edm.conf.IgnoredClientIPsFile = testdataFile1
	err = edm.setIgnoredClientIPs()
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
	discardLogger := slog.NewTextHandler(io.Discard, nil)
	logger := slog.New(discardLogger)

	edm, err := newDnstapMinimiser(logger, defaultTC)
	if err != nil {
		t.Fatalf("unable to setup edm: %s", err)
	}

	testdataFile := "testdata/ignored-client-ips.empty-lines-and-comments"

	edm.conf.IgnoredClientIPsFile = testdataFile
	err = edm.setIgnoredClientIPs()
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
	discardLogger := slog.NewTextHandler(io.Discard, nil)
	logger := slog.New(discardLogger)

	edm, err := newDnstapMinimiser(logger, defaultTC)
	if err != nil {
		t.Fatalf("unable to setup edm: %s", err)
	}

	testdataFile := "testdata/ignored-client-ips.valid1"
	// To make sure reading an empty file resets stuff as expected first read in a file with content
	edm.conf.IgnoredClientIPsFile = testdataFile
	err = edm.setIgnoredClientIPs()
	if err != nil {
		t.Fatalf("unable to parse testdata: %s", err)
	}

	// Magic value counted by hand
	expectedValidNumCIDRs := 2

	// Make sure we actually got anything loaded from the file with content
	if edm.ignoredClientsIPSet == nil {
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

	if edm.ignoredClientsIPSet != nil {
		t.Fatalf("edm.ignoredClientsIPSet should be nil, have: %#v", edm.ignoredClientsIPSet)
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
	discardLogger := slog.NewTextHandler(io.Discard, nil)
	logger := slog.New(discardLogger)

	edm, err := newDnstapMinimiser(logger, defaultTC)
	if err != nil {
		t.Fatalf("unable to setup edm: %s", err)
	}

	// To make sure unsetting the filename used for ignored client IPs
	// resets stuff as expected first read in a file with content
	edm.conf.IgnoredClientIPsFile = "testdata/ignored-client-ips.valid1"
	err = edm.setIgnoredClientIPs()
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
	discardLogger := slog.NewTextHandler(io.Discard, nil)
	logger := slog.New(discardLogger)

	edm, err := newDnstapMinimiser(logger, defaultTC)
	if err != nil {
		t.Fatalf("unable to setup edm: %s", err)
	}

	// Even if we are testing invalid data we still need to have loaded a
	// IP file with at least one valid entry in it to even inspect the
	// value.
	edm.conf.IgnoredClientIPsFile = "testdata/ignored-client-ips.valid1"
	err = edm.setIgnoredClientIPs()
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
	discardLogger := slog.NewTextHandler(io.Discard, nil)
	logger := slog.New(discardLogger)

	edm, err := newDnstapMinimiser(logger, defaultTC)
	if err != nil {
		t.Fatalf("unable to setup edm: %s", err)
	}

	testdataFile1 := "testdata/ignored-question-names.valid1.dawg"
	testdataFile2 := "testdata/ignored-question-names.valid2.dawg"

	// Magic value counted by hand
	expectedNumNames := 2

	edm.conf.IgnoredQuestionNamesFile = testdataFile1
	err = edm.setIgnoredQuestionNames()
	if err != nil {
		t.Fatalf("unable to parse testdata: %s", err)
	}

	if edm.ignoredQuestions.NumAdded() != expectedNumNames {
		t.Fatalf("unexpected number of names parsed from '%s': have: %d, want: %d", testdataFile1, edm.ignoredQuestions.NumAdded(), expectedNumNames)
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

	if edm.ignoredQuestions.NumAdded() != expectedNumNames {
		t.Fatalf("unexpected number of names parsed from '%s': have: %d, want: %d", testdataFile2, edm.ignoredQuestions.NumAdded(), expectedNumNames)
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
	discardLogger := slog.NewTextHandler(io.Discard, nil)
	logger := slog.New(discardLogger)

	edm, err := newDnstapMinimiser(logger, defaultTC)
	if err != nil {
		t.Fatalf("unable to setup edm: %s", err)
	}

	// To make sure reading an empty file resets stuff as expected first read in a file with content
	testdataFile := "testdata/ignored-question-names.valid1.dawg"
	edm.conf.IgnoredQuestionNamesFile = "testdata/ignored-question-names.valid1.dawg"
	err = edm.setIgnoredQuestionNames()
	if err != nil {
		t.Fatalf("unable to parse testdata: %s", err)
	}

	// Magic value counted by hand
	expectedNumNames := 2

	if edm.ignoredQuestions.NumAdded() != expectedNumNames {
		t.Fatalf("unexpected number of names parsed from '%s': have: %d, want: %d", testdataFile, edm.ignoredQuestions.NumAdded(), expectedNumNames)
	}

	testdataFile = "testdata/ignored-question-names.empty.dawg"
	edm.conf.IgnoredQuestionNamesFile = testdataFile
	err = edm.setIgnoredQuestionNames()
	if err != nil {
		t.Fatalf("unable to parse testdata: %s", err)
	}

	if edm.ignoredQuestions != nil {
		t.Fatalf("edm.ignoredQuestions should be nil: have: %#v", edm.ignoredQuestions)
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
	discardLogger := slog.NewTextHandler(io.Discard, nil)
	logger := slog.New(discardLogger)

	edm, err := newDnstapMinimiser(logger, defaultTC)
	if err != nil {
		t.Fatalf("unable to setup edm: %s", err)
	}

	// To make sure unsetting the filename used for ignored question names
	// resets stuff as expected first read in a file with content
	testdataFile := "testdata/ignored-question-names.valid1.dawg"
	edm.conf.IgnoredQuestionNamesFile = testdataFile
	err = edm.setIgnoredQuestionNames()
	if err != nil {
		t.Fatalf("unable to parse testdata: %s", err)
	}

	// Magic value counted by hand
	expectedNumNames := 2

	if edm.ignoredQuestions.NumAdded() != expectedNumNames {
		t.Fatalf("unexpected number of names parsed from '%s': have: %d, want: %d", testdataFile, edm.ignoredQuestions.NumAdded(), expectedNumNames)
	}

	// Now set an empty filename
	edm.conf.IgnoredQuestionNamesFile = ""
	err = edm.setIgnoredQuestionNames()
	if err != nil {
		t.Fatalf("unable to parse testdata: %s", err)
	}

	if edm.ignoredQuestions != nil {
		t.Fatalf("edm.ignoredQuestions should be nil: have: %#v", edm.ignoredQuestions)
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

func TestSetHistogramLabels(t *testing.T) {
	// The reason the labels are "backwards" is because we define "label0"
	// in the struct as the rightmost DNS label, e.g. "com", "net" etc.
	name := "label9.label8.label7.label6.label5.label4.label3.label2.label1.label0."
	labels := dns.SplitDomainName(name)

	// Reverse labels to get easier comparision matching (offset 0 -> label0)
	compLabels := slices.Clone(labels)
	slices.Reverse(compLabels)

	edm := &dnstapMinimiser{}
	hd := &histogramData{}

	edm.setLabels(labels, 10, &hd.dnsLabels)

	if *hd.Label0 != compLabels[0] {
		t.Fatalf("have: %s, want: %s", *hd.Label0, compLabels[0])
	}
	if *hd.Label1 != compLabels[1] {
		t.Fatalf("have: %s, want: %s", *hd.Label1, compLabels[1])
	}
	if *hd.Label2 != compLabels[2] {
		t.Fatalf("have: %s, want: %s", *hd.Label2, compLabels[2])
	}
	if *hd.Label3 != compLabels[3] {
		t.Fatalf("have: %s, want: %s", *hd.Label3, compLabels[3])
	}
	if *hd.Label4 != compLabels[4] {
		t.Fatalf("have: %s, want: %s", *hd.Label4, compLabels[4])
	}
	if *hd.Label5 != compLabels[5] {
		t.Fatalf("have: %s, want: %s", *hd.Label5, compLabels[5])
	}
	if *hd.Label6 != compLabels[6] {
		t.Fatalf("have: %s, want: %s", *hd.Label6, compLabels[6])
	}
	if *hd.Label7 != compLabels[7] {
		t.Fatalf("have: %s, want: %s", *hd.Label7, compLabels[7])
	}
	if *hd.Label8 != compLabels[8] {
		t.Fatalf("have: %s, want: %s", *hd.Label8, compLabels[8])
	}
	if *hd.Label9 != compLabels[9] {
		t.Fatalf("have: %s, want: %s", *hd.Label9, compLabels[9])
	}
}

func TestSetHistogramLabelsOverLimit(t *testing.T) {
	// The reason the labels are "backwards" is because we define "label0"
	// in the struct as the rightmost DNS label, e.g. "com", "net" etc.
	name := "label12.label11.label10.label9.label8.label7.label6.label5.label4.label3.label2.label1.label0."
	labels := dns.SplitDomainName(name)

	// Reverse labels to get easier comparision matching (offset 0 -> label0)
	compLabels := slices.Clone(labels)
	slices.Reverse(compLabels)

	edm := &dnstapMinimiser{}
	hd := &histogramData{}

	// The label9 field contains all overflowing labels
	overflowLabels := slices.Clone(labels[:4])
	slices.Reverse(overflowLabels)
	combinedLastLabel := strings.Join(overflowLabels, ".")

	edm.setLabels(labels, 10, &hd.dnsLabels)

	if *hd.Label0 != compLabels[0] {
		t.Fatalf("have: %s, want: %s", *hd.Label0, compLabels[0])
	}
	if *hd.Label1 != compLabels[1] {
		t.Fatalf("have: %s, want: %s", *hd.Label1, compLabels[1])
	}
	if *hd.Label2 != compLabels[2] {
		t.Fatalf("have: %s, want: %s", *hd.Label2, compLabels[2])
	}
	if *hd.Label3 != compLabels[3] {
		t.Fatalf("have: %s, want: %s", *hd.Label3, compLabels[3])
	}
	if *hd.Label4 != compLabels[4] {
		t.Fatalf("have: %s, want: %s", *hd.Label4, compLabels[4])
	}
	if *hd.Label5 != compLabels[5] {
		t.Fatalf("have: %s, want: %s", *hd.Label5, compLabels[5])
	}
	if *hd.Label6 != compLabels[6] {
		t.Fatalf("have: %s, want: %s", *hd.Label6, compLabels[6])
	}
	if *hd.Label7 != compLabels[7] {
		t.Fatalf("have: %s, want: %s", *hd.Label7, compLabels[7])
	}
	if *hd.Label8 != compLabels[8] {
		t.Fatalf("have: %s, want: %s", *hd.Label8, compLabels[8])
	}
	if *hd.Label9 != combinedLastLabel {
		t.Fatalf("have: %s, want: %s", *hd.Label9, combinedLastLabel)
	}
}

func TestSetSessionLabels(t *testing.T) {
	// The reason the labels are "backwards" is because we define "label0"
	// in the struct as the rightmost DNS label, e.g. "com", "net" etc.
	labels := []string{"label9", "label8", "label7", "label6", "label5", "label4", "label3", "label2", "label1", "label0"}
	edm := &dnstapMinimiser{}
	sd := &sessionData{}

	edm.setLabels(labels, 10, &sd.dnsLabels)

	if *sd.Label0 != labels[9] {
		t.Fatalf("have: %s, want: %s", *sd.Label0, labels[9])
	}
	if *sd.Label1 != labels[8] {
		t.Fatalf("have: %s, want: %s", *sd.Label1, labels[8])
	}
	if *sd.Label2 != labels[7] {
		t.Fatalf("have: %s, want: %s", *sd.Label2, labels[7])
	}
	if *sd.Label3 != labels[6] {
		t.Fatalf("have: %s, want: %s", *sd.Label3, labels[6])
	}
	if *sd.Label4 != labels[5] {
		t.Fatalf("have: %s, want: %s", *sd.Label4, labels[5])
	}
	if *sd.Label5 != labels[4] {
		t.Fatalf("have: %s, want: %s", *sd.Label5, labels[4])
	}
	if *sd.Label6 != labels[3] {
		t.Fatalf("have: %s, want: %s", *sd.Label6, labels[3])
	}
	if *sd.Label7 != labels[2] {
		t.Fatalf("have: %s, want: %s", *sd.Label7, labels[2])
	}
	if *sd.Label8 != labels[1] {
		t.Fatalf("have: %s, want: %s", *sd.Label8, labels[1])
	}
	if *sd.Label9 != labels[0] {
		t.Fatalf("have: %s, want: %s", *sd.Label9, labels[0])
	}
}

func TestEDMStatusBitsMulti(t *testing.T) {
	expectedString := "well-known-exact|well-known-wildcard"

	dsb := new(edmStatusBits)
	dsb.set(edmStatusWellKnownWildcard)
	dsb.set(edmStatusWellKnownExact)

	if dsb.String() != expectedString {
		t.Fatalf("have: %s, want: %s", dsb.String(), expectedString)
	}
}

func TestEDMStatusBitsSingle(t *testing.T) {
	expectedString := "well-known-exact"

	dsb := new(edmStatusBits)
	dsb.set(edmStatusWellKnownExact)

	if dsb.String() != expectedString {
		t.Fatalf("have: %s, want: %s", dsb.String(), expectedString)
	}
}

func TestEDMStatusBitsMax(t *testing.T) {
	expectedString := "unknown flags in status"

	dsb := new(edmStatusBits)
	dsb.set(edmStatusMax)

	if !strings.HasPrefix(dsb.String(), "unknown flags in status: ") {
		t.Fatalf("have: %s, want prefix: %s", dsb.String(), expectedString)
	}
}

func TestEDMStatusBitsUnknown(t *testing.T) {
	expectedString := "unknown flags in status"

	dsb := new(edmStatusBits)
	dsb.set(edmStatusMax << 1)

	if !strings.HasPrefix(dsb.String(), "unknown flags in status: ") {
		t.Fatalf("have: %s, want prefix: %s", dsb.String(), expectedString)
	}
}

func TestEDMIPBytesToInt(t *testing.T) {
	ipv4AddrString := "198.51.100.15"

	ip4Addr, err := netip.ParseAddr(ipv4AddrString)
	if err != nil {
		t.Fatalf("unable to parse IPv4 test address '%s': %s", ipv4AddrString, err)
	}

	ip4Int, err := ipBytesToInt(ip4Addr.AsSlice())
	if err != nil {
		t.Fatalf("unable to create uint32 variable from IPv4 test address '%s': %s", ipv4AddrString, err)
	}

	// Go back to IPv4 data
	constructedV4Data := []byte{}
	constructedV4Data = binary.BigEndian.AppendUint32(constructedV4Data, ip4Int)

	constructedIP4Addr, ok := netip.AddrFromSlice(constructedV4Data)
	if !ok {
		t.Fatalf("unable to create netip from from constructed IPv4 bytes: %b", constructedV4Data)
	}

	if ip4Addr != constructedIP4Addr {
		t.Fatalf("have: %s, want: %s", constructedIP4Addr, ip4Addr)
	}
}

func TestEDMIP6BytesToInt(t *testing.T) {
	ipv6AddrString := "2001:db8:1122:3344:5566:7788:99aa:bbcc"

	ip6Addr, err := netip.ParseAddr(ipv6AddrString)
	if err != nil {
		t.Fatalf("unable to parse IPv6 test address '%s': %s", ipv6AddrString, err)
	}

	ip6Network, ip6Host, err := ip6BytesToInt(ip6Addr.AsSlice())
	if err != nil {
		t.Fatalf("unable to create uint64 variables from IPv6 test address '%s': %s", ipv6AddrString, err)
	}

	// Go back to complete IPv6 data
	constructedV6Data := []byte{}
	constructedV6Data = binary.BigEndian.AppendUint64(constructedV6Data, ip6Network)
	constructedV6Data = binary.BigEndian.AppendUint64(constructedV6Data, ip6Host)

	constructedIP6Addr, ok := netip.AddrFromSlice(constructedV6Data)
	if !ok {
		t.Fatalf("unable to create netip from from constructed IPv6 bytes: %b", constructedV6Data)
	}

	if ip6Addr != constructedIP6Addr {
		t.Fatalf("have: %s, want: %s", constructedIP6Addr, ip6Addr)
	}
}

func TestPseudonymiseDnstap(t *testing.T) {
	// Dont output logging
	// https://github.com/golang/go/issues/62005
	discardLogger := slog.NewTextHandler(io.Discard, nil)
	logger := slog.New(discardLogger)

	// The original addresses we want to pseudonymise
	origQueryAddr4 := netip.MustParseAddr("198.51.100.20")
	origRespAddr4 := netip.MustParseAddr("198.51.100.30")
	origQueryAddr6 := netip.MustParseAddr("2001:db8:1122:3344:5566:7788:99aa:bbcc")
	origRespAddr6 := netip.MustParseAddr("2001:db8:1122:3344:5566:7788:99aa:ddee")

	// The expected result given our first and second keys
	expectedPseudoQueryAddr4 := netip.MustParseAddr("58.92.11.53")
	expectedPseudoRespAddr4 := netip.MustParseAddr("58.92.11.62")
	expectedPseudoQueryAddrUpdated4 := netip.MustParseAddr("185.204.164.235")
	expectedPseudoRespAddrUpdated4 := netip.MustParseAddr("185.204.164.225")

	expectedPseudoQueryAddr6 := netip.MustParseAddr("b780:8dc8:6ed9:cbc5:4d61:a6bb:6255:5a03")
	expectedPseudoRespAddr6 := netip.MustParseAddr("b780:8dc8:6ed9:cbc5:4d61:a6bb:6255:262d")
	expectedPseudoQueryAddrUpdated6 := netip.MustParseAddr("3f29:478:21d2:2c44:6915:7ca7:8654:aa28")
	expectedPseudoRespAddrUpdated6 := netip.MustParseAddr("3f29:478:21d2:2c44:6915:7ca7:8654:d21f")

	dt4 := &dnstap.Dnstap{
		Message: &dnstap.Message{
			QueryAddress:    origQueryAddr4.AsSlice(),
			ResponseAddress: origRespAddr4.AsSlice(),
		},
	}
	dt6 := &dnstap.Dnstap{
		Message: &dnstap.Message{
			QueryAddress:    origQueryAddr6.AsSlice(),
			ResponseAddress: origRespAddr6.AsSlice(),
		},
	}

	edm, err := newDnstapMinimiser(logger, defaultTC)
	if err != nil {
		t.Fatalf("unable to setup edm: %s", err)
	}

	if edm.cryptopanCache != nil {
		if edm.cryptopanCache.Len() != 0 {
			t.Fatalf("there should be no entries in newly initialised cryptopan cache but it contains items: %d", edm.cryptopanCache.Len())
		}
	}

	edm.pseudonymiseDnstap(dt4)
	edm.pseudonymiseDnstap(dt6)

	pseudoQueryAddr4, ok := netip.AddrFromSlice(dt4.Message.QueryAddress)
	if !ok {
		t.Fatal("unable to parse IPv4 QueryAddress")
	}
	pseudoRespAddr4, ok := netip.AddrFromSlice(dt4.Message.ResponseAddress)
	if !ok {
		t.Fatal("unable to parse IPv4 ResponseAddress")
	}

	pseudoQueryAddr6, ok := netip.AddrFromSlice(dt6.Message.QueryAddress)
	if !ok {
		t.Fatal("unable to parse IPv6 QueryAddress")
	}
	pseudoRespAddr6, ok := netip.AddrFromSlice(dt6.Message.ResponseAddress)
	if !ok {
		t.Fatal("unable to parse IPv6 ResponseAddress")
	}

	// Verify we are not accidentally getting IPv4-mapped IPv6 address
	if !pseudoQueryAddr4.Is4() {
		t.Fatalf("pseudonymised IPv4 query address appears to be IPv4-mapped IPv6 address: %s", pseudoQueryAddr4)
	}
	if !pseudoRespAddr4.Is4() {
		t.Fatalf("pseudonymised IPv4 response address appears to be IPv4-mapped IPv6 address: %s", pseudoRespAddr4)
	}

	// Verify they are different from the original addresses
	if origQueryAddr4 == pseudoQueryAddr4 {
		t.Fatalf("pseudonymised IPv4 query address %s is the same as the orignal address %s", pseudoQueryAddr4, origQueryAddr4)
	}
	if origRespAddr4 == pseudoRespAddr4 {
		t.Fatalf("pseudonymised IPv4 response address %s is the same as the orignal address %s", pseudoRespAddr4, origRespAddr4)
	}
	if origQueryAddr6 == pseudoQueryAddr6 {
		t.Fatalf("pseudonymised IPv6 query address %s is the same as the orignal address %s", pseudoQueryAddr6, origQueryAddr6)
	}
	if origRespAddr6 == pseudoRespAddr6 {
		t.Fatalf("pseudonymised IPv6 response address %s is the same as the orignal address %s", pseudoRespAddr6, origRespAddr6)
	}

	// Verify they are different as expected
	if pseudoQueryAddr4 != expectedPseudoQueryAddr4 {
		t.Fatalf("pseudonymised IPv4 query address %s is not the expected address %s", pseudoQueryAddr4, expectedPseudoQueryAddr4)
	}
	if pseudoRespAddr4 != expectedPseudoRespAddr4 {
		t.Fatalf("pseudonymised IPv4 resp address %s is not the expected address %s", pseudoRespAddr4, expectedPseudoRespAddr4)
	}
	if pseudoQueryAddr6 != expectedPseudoQueryAddr6 {
		t.Fatalf("pseudonymised IPv6 query address %s is not the expected address %s", pseudoQueryAddr6, expectedPseudoQueryAddr6)
	}
	if pseudoRespAddr6 != expectedPseudoRespAddr6 {
		t.Fatalf("pseudonymised IPv6 resp address %s is not the expected address %s", pseudoRespAddr6, expectedPseudoRespAddr6)
	}

	if edm.cryptopanCache != nil {
		if edm.cryptopanCache.Len() == 0 {
			t.Fatalf("there should be entries in the cryptopan cache but it is empty")
		}

		// Verify the entry in the cache is the same as the one we got back
		cachedPseudoQueryAddr4, ok := edm.cryptopanCache.Get(origQueryAddr4)
		if !ok {
			t.Fatalf("unable to lookup IPv4 query address %s in cache", origQueryAddr4)
		}
		if cachedPseudoQueryAddr4 != pseudoQueryAddr4 {
			t.Fatalf("cached pseudonymised IPv4 query address %s is not the same as the calculated address %s", cachedPseudoQueryAddr4, pseudoQueryAddr4)
		}

		cachedPseudoRespAddr4, ok := edm.cryptopanCache.Get(origRespAddr4)
		if !ok {
			t.Fatalf("unable to lookup IPv4 response address %s in cache", origRespAddr4)
		}
		if cachedPseudoRespAddr4 != pseudoRespAddr4 {
			t.Fatalf("cached pseudonymised IPv4 response address %s is not the same as the calculated address %s", cachedPseudoRespAddr4, pseudoRespAddr4)
		}

		cachedPseudoQueryAddr6, ok := edm.cryptopanCache.Get(origQueryAddr6)
		if !ok {
			t.Fatalf("unable to lookup IPv6 query address %s in cache", origQueryAddr6)
		}
		if cachedPseudoQueryAddr6 != pseudoQueryAddr6 {
			t.Fatalf("cached pseudonymised IPv6 query address %s is not the same as the calculated address %s", cachedPseudoQueryAddr6, pseudoQueryAddr6)
		}

		cachedPseudoRespAddr6, ok := edm.cryptopanCache.Get(origRespAddr6)
		if !ok {
			t.Fatalf("unable to lookup IPv6 response address %s in cache", origRespAddr6)
		}
		if cachedPseudoRespAddr6 != pseudoRespAddr6 {
			t.Fatalf("cached pseudonymised IPv6 response address %s is not the same as the calculated address %s", cachedPseudoRespAddr6, pseudoRespAddr6)
		}
	}

	if edm.cryptopanCache != nil {
		t.Logf("number of pseudonymisation cache entries before reset: %d", edm.cryptopanCache.Len())
	}

	if edm.cryptopanCache != nil {
		for _, key := range edm.cryptopanCache.Keys() {
			value, ok := edm.cryptopanCache.Get(key)
			if !ok {
				t.Fatalf("unable to extract value for key before reset: %s", key)
			}

			t.Logf("inital cache key: %s, value: %s", key, value)
		}
	}

	// Replace the cryptopan instance and verify we now get different pseudonymised results
	err = edm.setCryptopan("key2", defaultTC.CryptopanKeySalt, defaultTC.CryptopanAddressEntries)
	if err != nil {
		t.Fatalf("unable to call edm.SetCryptopan: %s", err)
	}

	if edm.cryptopanCache != nil {
		if edm.cryptopanCache.Len() != 0 {
			t.Fatalf("there should be no cache entries in replaced cryptopan cache but it contains items: %d", edm.cryptopanCache.Len())
		}
	}

	// Reset the addresses and pseudonymise again with the updated key
	dt4.Message.QueryAddress = origQueryAddr4.AsSlice()
	dt4.Message.ResponseAddress = origRespAddr4.AsSlice()
	dt6.Message.QueryAddress = origQueryAddr6.AsSlice()
	dt6.Message.ResponseAddress = origRespAddr6.AsSlice()

	edm.pseudonymiseDnstap(dt4)
	edm.pseudonymiseDnstap(dt6)

	pseudoQueryAddrUpdated4, ok := netip.AddrFromSlice(dt4.Message.QueryAddress)
	if !ok {
		t.Fatal("unable to parse second IPv4 QueryAddress")
	}
	pseudoRespAddrUpdated4, ok := netip.AddrFromSlice(dt4.Message.ResponseAddress)
	if !ok {
		t.Fatal("unable to parse second IPv4 ResponseAddress")
	}
	pseudoQueryAddrUpdated6, ok := netip.AddrFromSlice(dt6.Message.QueryAddress)
	if !ok {
		t.Fatal("unable to parse second IPv6 QueryAddress")
	}
	pseudoRespAddrUpdated6, ok := netip.AddrFromSlice(dt6.Message.ResponseAddress)
	if !ok {
		t.Fatal("unable to parse second IPv6 ResponseAddress")
	}

	// Verify they are different from the original addresses
	if origQueryAddr4 == pseudoQueryAddrUpdated4 {
		t.Fatalf("updated pseudonymised IPv4 query address %s is the same as the orignal address %s", pseudoQueryAddrUpdated4, origQueryAddr4)
	}
	if origRespAddr4 == pseudoRespAddrUpdated4 {
		t.Fatalf("updated pseudonymised IPv4 response address %s is the same as the orignal address %s", pseudoRespAddrUpdated4, origRespAddr4)
	}
	if origQueryAddr6 == pseudoQueryAddrUpdated6 {
		t.Fatalf("updated pseudonymised IPv6 query address %s is the same as the orignal address %s", pseudoQueryAddrUpdated6, origQueryAddr6)
	}
	if origRespAddr6 == pseudoRespAddrUpdated6 {
		t.Fatalf("updated pseudonymised IPv6 response address %s is the same as the orignal address %s", pseudoRespAddrUpdated6, origRespAddr6)
	}

	// Verify the new pseudo addresses are different from the previous pseudo addresses
	if pseudoQueryAddr4 == pseudoQueryAddrUpdated4 {
		t.Fatalf("updated pseudonymised IPv4 query address %s is the same as the orignal pseudonymised address %s", pseudoQueryAddrUpdated4, pseudoQueryAddr4)
	}
	if pseudoRespAddr4 == pseudoRespAddrUpdated4 {
		t.Fatalf("updated pseudonymised IPv4 response address %s is the same as the orignal pseudonymised address %s", pseudoRespAddrUpdated4, pseudoRespAddr4)
	}
	if pseudoQueryAddr6 == pseudoQueryAddrUpdated6 {
		t.Fatalf("updated pseudonymised IPv6 query address %s is the same as the orignal pseudonymised address %s", pseudoQueryAddrUpdated6, pseudoQueryAddr6)
	}
	if pseudoRespAddr6 == pseudoRespAddrUpdated6 {
		t.Fatalf("updated pseudonymised IPv6 response address %s is the same as the orignal pseudonymised address %s", pseudoRespAddrUpdated6, pseudoRespAddr6)
	}

	// Verify they are different as expected
	if pseudoQueryAddrUpdated4 != expectedPseudoQueryAddrUpdated4 {
		t.Fatalf("updated pseudonymised IPv4 query address %s is not the expected address %s", pseudoQueryAddrUpdated4, expectedPseudoQueryAddrUpdated4)
	}
	if pseudoRespAddrUpdated4 != expectedPseudoRespAddrUpdated4 {
		t.Fatalf("updated pseudonymised IPv4 resp address %s is not the expected address %s", pseudoRespAddrUpdated4, expectedPseudoRespAddrUpdated4)
	}
	if pseudoQueryAddrUpdated6 != expectedPseudoQueryAddrUpdated6 {
		t.Fatalf("updated pseudonymised IPv6 query address %s is not the expected address %s", pseudoQueryAddrUpdated6, expectedPseudoQueryAddrUpdated6)
	}
	if pseudoRespAddrUpdated6 != expectedPseudoRespAddrUpdated6 {
		t.Fatalf("updated pseudonymised IPv6 resp address %s is not the expected address %s", pseudoRespAddrUpdated6, expectedPseudoRespAddrUpdated6)
	}

	if edm.cryptopanCache != nil {
		t.Logf("number of pseudonymisation cache entries before end: %d", edm.cryptopanCache.Len())
		for _, key := range edm.cryptopanCache.Keys() {
			value, ok := edm.cryptopanCache.Get(key)
			if !ok {
				t.Fatalf("unable to extract value for key before end: %s", key)
			}

			t.Logf("reset cache key: %s, value: %s", key, value)
		}
	}

	// Replace the cryptopan instance with uncached version and the first key and verify we get the same pseudonymised results
	err = edm.setCryptopan(defaultTC.CryptopanKey, defaultTC.CryptopanKeySalt, 0)
	if err != nil {
		t.Fatalf("unable to call edm.SetCryptopan with 0 cache size: %s", err)
	}

	// Reset the addresses and pseudonymise again with the updated key
	dt4.Message.QueryAddress = origQueryAddr4.AsSlice()
	dt4.Message.ResponseAddress = origRespAddr4.AsSlice()
	dt6.Message.QueryAddress = origQueryAddr6.AsSlice()
	dt6.Message.ResponseAddress = origRespAddr6.AsSlice()

	edm.pseudonymiseDnstap(dt4)
	edm.pseudonymiseDnstap(dt6)

	uncachedPseudoQueryAddr4, ok := netip.AddrFromSlice(dt4.Message.QueryAddress)
	if !ok {
		t.Fatal("unable to parse uncached IPv4 QueryAddress")
	}
	uncachedPseudoRespAddr4, ok := netip.AddrFromSlice(dt4.Message.ResponseAddress)
	if !ok {
		t.Fatal("unable to parse uncached IPv4 ResponseAddress")
	}
	uncachedPseudoQueryAddr6, ok := netip.AddrFromSlice(dt6.Message.QueryAddress)
	if !ok {
		t.Fatal("unable to parse uncached IPv6 QueryAddress")
	}
	uncachedPseudoRespAddr6, ok := netip.AddrFromSlice(dt6.Message.ResponseAddress)
	if !ok {
		t.Fatal("unable to parse uncached IPv6 ResponseAddress")
	}

	// Verify we are not accidentally getting IPv4-mapped IPv6 address
	if !uncachedPseudoQueryAddr4.Is4() {
		t.Fatalf("uncached pseudonymised IPv4 query address appears to be IPv4-mapped IPv6 address: %s", uncachedPseudoQueryAddr4)
	}
	if !uncachedPseudoRespAddr4.Is4() {
		t.Fatalf("uncached pseudonymised IPv4 response address appears to be IPv4-mapped IPv6 address: %s", uncachedPseudoRespAddr4)
	}

	// Verify they are different from the original addresses
	if origQueryAddr4 == uncachedPseudoQueryAddr4 {
		t.Fatalf("uncached pseudonymised IPv4 query address %s is the same as the orignal address %s", uncachedPseudoQueryAddr4, origQueryAddr4)
	}
	if origRespAddr4 == uncachedPseudoRespAddr4 {
		t.Fatalf("uncached pseudonymised IPv4 response address %s is the same as the orignal address %s", uncachedPseudoRespAddr4, origRespAddr4)
	}
	if origQueryAddr6 == uncachedPseudoQueryAddr6 {
		t.Fatalf("uncached pseudonymised IPv6 query address %s is the same as the orignal address %s", uncachedPseudoQueryAddr6, origQueryAddr6)
	}
	if origRespAddr6 == uncachedPseudoRespAddr6 {
		t.Fatalf("uncached pseudonymised IPv6 response address %s is the same as the orignal address %s", uncachedPseudoRespAddr6, origRespAddr6)
	}

	// Verify they are different as expected
	if uncachedPseudoQueryAddr4 != expectedPseudoQueryAddr4 {
		t.Fatalf("uncached pseudonymised IPv4 query address %s is not the expected address %s", uncachedPseudoQueryAddr4, expectedPseudoQueryAddr4)
	}
	if uncachedPseudoRespAddr4 != expectedPseudoRespAddr4 {
		t.Fatalf("uncached pseudonymised IPv4 resp address %s is not the expected address %s", uncachedPseudoRespAddr4, expectedPseudoRespAddr4)
	}
	if uncachedPseudoQueryAddr6 != expectedPseudoQueryAddr6 {
		t.Fatalf("uncached pseudonymised IPv6 query address %s is not the expected address %s", uncachedPseudoQueryAddr6, expectedPseudoQueryAddr6)
	}
	if uncachedPseudoRespAddr6 != expectedPseudoRespAddr6 {
		t.Fatalf("uncached pseudonymised IPv6 resp address %s is not the expected address %s", uncachedPseudoRespAddr6, expectedPseudoRespAddr6)
	}
}

func BenchmarkPseudonymiseDnstapWithCache4(b *testing.B) {
	b.ReportAllocs()

	// Dont output logging
	// https://github.com/golang/go/issues/62005
	discardLogger := slog.NewTextHandler(io.Discard, nil)
	logger := slog.New(discardLogger)

	// The original addresses we want to pseudonymise
	origQueryAddr4 := netip.MustParseAddr("198.51.100.20")
	origRespAddr4 := netip.MustParseAddr("198.51.100.30")

	edm, err := newDnstapMinimiser(logger, defaultTC)
	if err != nil {
		b.Fatalf("unable to setup edm: %s", err)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		dt4 := &dnstap.Dnstap{
			Message: &dnstap.Message{
				QueryAddress:    origQueryAddr4.AsSlice(),
				ResponseAddress: origRespAddr4.AsSlice(),
			},
		}
		edm.pseudonymiseDnstap(dt4)
	}
}

func BenchmarkPseudonymiseDnstapWithoutCache4(b *testing.B) {
	b.ReportAllocs()

	// Dont output logging
	// https://github.com/golang/go/issues/62005
	discardLogger := slog.NewTextHandler(io.Discard, nil)
	logger := slog.New(discardLogger)

	// The original addresses we want to pseudonymise
	origQueryAddr4 := netip.MustParseAddr("198.51.100.20")
	origRespAddr4 := netip.MustParseAddr("198.51.100.30")

	uncachedTC := defaultTC
	uncachedTC.CryptopanAddressEntries = 0

	edm, err := newDnstapMinimiser(logger, uncachedTC)
	if err != nil {
		b.Fatalf("unable to setup edm: %s", err)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		dt4 := &dnstap.Dnstap{
			Message: &dnstap.Message{
				QueryAddress:    origQueryAddr4.AsSlice(),
				ResponseAddress: origRespAddr4.AsSlice(),
			},
		}
		edm.pseudonymiseDnstap(dt4)
	}
}

func BenchmarkPseudonymiseDnstapWithCache6(b *testing.B) {
	b.ReportAllocs()

	// Dont output logging
	// https://github.com/golang/go/issues/62005
	discardLogger := slog.NewTextHandler(io.Discard, nil)
	logger := slog.New(discardLogger)

	// The original addresses we want to pseudonymise
	origQueryAddr6 := netip.MustParseAddr("2001:db8:1122:3344:5566:7788:99aa:bbcc")
	origRespAddr6 := netip.MustParseAddr("2001:db8:1122:3344:5566:7788:99aa:ddee")

	edm, err := newDnstapMinimiser(logger, defaultTC)
	if err != nil {
		b.Fatalf("unable to setup edm: %s", err)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		dt6 := &dnstap.Dnstap{
			Message: &dnstap.Message{
				QueryAddress:    origQueryAddr6.AsSlice(),
				ResponseAddress: origRespAddr6.AsSlice(),
			},
		}
		edm.pseudonymiseDnstap(dt6)
	}
}

func BenchmarkPseudonymiseDnstapWithoutCache6(b *testing.B) {
	b.ReportAllocs()

	// Dont output logging
	// https://github.com/golang/go/issues/62005
	discardLogger := slog.NewTextHandler(io.Discard, nil)
	logger := slog.New(discardLogger)

	// The original addresses we want to pseudonymise
	origQueryAddr6 := netip.MustParseAddr("2001:db8:1122:3344:5566:7788:99aa:bbcc")
	origRespAddr6 := netip.MustParseAddr("2001:db8:1122:3344:5566:7788:99aa:ddee")

	uncachedTC := defaultTC
	uncachedTC.CryptopanAddressEntries = 0

	edm, err := newDnstapMinimiser(logger, uncachedTC)
	if err != nil {
		b.Fatalf("unable to setup edm: %s", err)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		dt6 := &dnstap.Dnstap{
			Message: &dnstap.Message{
				QueryAddress:    origQueryAddr6.AsSlice(),
				ResponseAddress: origRespAddr6.AsSlice(),
			},
		}
		edm.pseudonymiseDnstap(dt6)
	}
}

func BenchmarkMurmurHasher(b *testing.B) {
	b.ReportAllocs()

	ipBytes := netip.MustParseAddr("198.51.100.20").AsSlice()

	murmur3Hasher := murmur3.New64()

	for n := 0; n < b.N; n++ {
		murmur3Hasher.Write(ipBytes) // #nosec G104 -- Write() on hash.Hash never returns an error (https://pkg.go.dev/hash#Hash)
		murmur3Hasher.Sum64()
		murmur3Hasher.Reset()
	}
}

func BenchmarkMurmurSum64(b *testing.B) {
	b.ReportAllocs()

	ipBytes := netip.MustParseAddr("198.51.100.20").AsSlice()

	for n := 0; n < b.N; n++ {
		murmur3.Sum64(ipBytes)
	}
}

func TestCompareMurmurHashing(t *testing.T) {
	murmur3Hasher := murmur3.New64()

	ipAddrs := []string{"198.51.100.20", "198.51.100.21", "198.51.100.22"}

	for _, ipAddr := range ipAddrs {
		ipBytes := netip.MustParseAddr(ipAddr).AsSlice()
		murmur3Hasher.Write(ipBytes) // #nosec G104 -- Write() on hash.Hash never returns an error (https://pkg.go.dev/hash#Hash)
		hasherRes := murmur3Hasher.Sum64()
		murmur3Hasher.Reset()

		sumRes := murmur3.Sum64(ipBytes)

		if hasherRes != sumRes {
			t.Fatalf("have: %d, want: %d", hasherRes, sumRes)
		}
	}
}

func ptr[T any](v T) *T {
	return &v
}

func BenchmarkSessionWriter(b *testing.B) {
	b.ReportAllocs()

	var buf bytes.Buffer
	snappyCodec := parquet.LookupCompressionCodec(format.Snappy)
	parquetWriter := parquet.NewGenericWriter[sessionData](&buf, parquet.Compression(snappyCodec))

	ipInt, err := ipBytesToInt(netip.MustParseAddr("198.51.100.20").AsSlice())
	if err != nil {
		b.Fatalf("unable to create uint32 from address: %s", err)
	}
	i32IPInt := int32(ipInt) // #nosec G115 -- Used in parquet struct with logical type uint32

	ip6NetworkUint, ip6HostUint, err := ip6BytesToInt(netip.MustParseAddr("2001:db8:1122:3344:5566:7788:99aa:bbcc").AsSlice())
	if err != nil {
		b.Fatalf("unable to create uint64 from ipv6 address: %s", err)
	}
	ip6NetworkInt := int64(ip6NetworkUint) // #nosec G115 -- Used in parquet struct with logical type uint64
	ip6HostInt := int64(ip6HostUint)       // #nosec G115 -- Used in parquet struct with logical type uint64

	sd := sessionData{
		dnsLabels: dnsLabels{
			Label0: ptr("com"),
			Label1: ptr("example"),
			Label2: ptr("www"),
		},
		ServerID:          ptr("serverID"),
		QueryTime:         ptr(int64(10)),
		ResponseTime:      ptr(int64(10)),
		SourceIPv4:        &i32IPInt,
		DestIPv4:          &i32IPInt,
		SourceIPv6Network: &ip6NetworkInt,
		SourceIPv6Host:    &ip6HostInt,
		DestIPv6Network:   &ip6NetworkInt,
		DestIPv6Host:      &ip6HostInt,
		SourcePort:        ptr(int32(1337)),
		DestPort:          ptr(int32(1337)),
		DNSProtocol:       ptr(int32(1)),
		QueryMessage:      ptr("query message"),
		ResponseMessage:   ptr("response message"),
	}

	for b.Loop() {
		_, err = parquetWriter.Write([]sessionData{sd})
		if err != nil {
			b.Fatalf("unable to call Write() on parquet writer: %s", err)
		}
	}
	err = parquetWriter.Close()
	if err != nil {
		b.Fatalf("unable to call WriteStop() on parquet writer: %s", err)
	}
}

func TestSessionWriter(t *testing.T) {
	var buf bytes.Buffer

	snappyCodec := parquet.LookupCompressionCodec(format.Snappy)
	parquetWriter := parquet.NewGenericWriter[sessionData](&buf, sessionDataSchema, parquet.Compression(snappyCodec))

	ipInt, err := ipBytesToInt(netip.MustParseAddr("198.51.100.20").AsSlice())
	if err != nil {
		t.Fatalf("unable to create uint32 from address: %s", err)
	}
	i32IPInt := int32(ipInt) // #nosec G115 -- Used in parquet struct with logical type uint64

	ip6NetworkUint, ip6HostUint, err := ip6BytesToInt(netip.MustParseAddr("2001:db8:1122:3344:5566:7788:99aa:bbcc").AsSlice())
	if err != nil {
		t.Fatalf("unable to create uint64 from ipv6 address: %s", err)
	}

	ip6NetworkInt := int64(ip6NetworkUint) // #nosec G115 -- Used in parquet struct with logical type uint64
	ip6HostInt := int64(ip6HostUint)       // #nosec G115 -- Used in parquet struct with logical type uint64

	sd := sessionData{
		dnsLabels: dnsLabels{
			Label0: ptr("com"),
			Label1: ptr("example"),
			Label2: ptr("www"),
		},
		ServerID:          ptr("serverID"),
		QueryTime:         ptr(int64(10)),
		ResponseTime:      ptr(int64(10)),
		SourceIPv4:        &i32IPInt,
		SourceIPv6Network: &ip6NetworkInt,
		SourceIPv6Host:    &ip6HostInt,
		DestIPv6Network:   &ip6NetworkInt,
		DestIPv6Host:      &ip6HostInt,
		DestIPv4:          &i32IPInt,
		SourcePort:        ptr(int32(1337)),
		DestPort:          ptr(int32(1337)),
		DNSProtocol:       ptr(int32(1)),
		QueryMessage:      ptr("query message"),
		ResponseMessage:   ptr("response message"),
	}

	_, err = parquetWriter.Write([]sessionData{sd})
	if err != nil {
		t.Fatalf("unable to call Write() on parquet writer: %s", err)
	}

	err = parquetWriter.Close()
	if err != nil {
		t.Fatalf("unable to call Close() on parquet writer: %s", err)
	}

	if *writeParquet {
		f, err := os.Create("testdata/generated-session.parquet")
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			err := f.Close()
			if err != nil {
				t.Fatal(err)
			}
		}()

		_, err = buf.WriteTo(f)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestHistogramWriter(t *testing.T) {
	var buf bytes.Buffer

	ip4 := netip.MustParseAddr("198.51.100.20")
	ip6 := netip.MustParseAddr("2001:db8:1122:3344:5566:7788:99aa:bbcc")

	hllSettings := getHllDefaults(0)

	v4hll, err := hll.NewHll(hllSettings)
	if err != nil {
		t.Fatalf("unable to init IPv4 HLL: %s", err)
	}

	v6hll, err := hll.NewHll(hllSettings)
	if err != nil {
		t.Fatalf("unable to init IPv6 HLL: %s", err)
	}

	v4hll.AddRaw(murmur3.Sum64(ip4.AsSlice()))
	v6hll.AddRaw(murmur3.Sum64(ip6.AsSlice()))

	snappyCodec := parquet.LookupCompressionCodec(format.Snappy)
	parquetWriter := parquet.NewGenericWriter[histogramData](&buf, parquet.Compression(snappyCodec))

	hd := histogramData{
		dnsLabels: dnsLabels{
			Label0: ptr("com"),
			Label1: ptr("example"),
			Label2: ptr("www"),
		},
		StartTime:             10,
		ACount:                11,
		AAAACount:             12,
		MXCount:               13,
		NSCount:               14,
		OtherTypeCount:        15,
		NonINCount:            16,
		OKCount:               17,
		NXCount:               18,
		FailCount:             19,
		OtherRcodeCount:       20,
		EDMStatusBits:         21,
		V4ClientCountHLLBytes: string(v4hll.ToBytes()),
		V6ClientCountHLLBytes: string(v6hll.ToBytes()),
	}

	_, err = parquetWriter.Write([]histogramData{hd})
	if err != nil {
		t.Fatalf("unable to call Write() on parquet writer: %s", err)
	}

	err = parquetWriter.Close()
	if err != nil {
		t.Fatalf("unable to call WriteStop() on parquet writer: %s", err)
	}

	if *writeParquet {
		f, err := os.Create("testdata/generated-histogram.parquet")
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			err := f.Close()
			if err != nil {
				t.Fatal(err)
			}
		}()

		_, err = buf.WriteTo(f)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkHistogramWriter(b *testing.B) {
	b.ReportAllocs()

	var err error

	ip4 := netip.MustParseAddr("198.51.100.20")
	ip6 := netip.MustParseAddr("2001:db8:1122:3344:5566:7788:99aa:bbcc")

	hllSettings := getHllDefaults(0)

	v4hll, err := hll.NewHll(hllSettings)
	if err != nil {
		b.Fatalf("unable to init IPv4 HLL: %s", err)
	}

	v6hll, err := hll.NewHll(hllSettings)
	if err != nil {
		b.Fatalf("unable to init IPv6 HLL: %s", err)
	}

	v4hll.AddRaw(murmur3.Sum64(ip4.AsSlice()))
	v6hll.AddRaw(murmur3.Sum64(ip6.AsSlice()))

	var buf bytes.Buffer
	snappyCodec := parquet.LookupCompressionCodec(format.Snappy)
	parquetWriter := parquet.NewGenericWriter[histogramData](&buf, parquet.Compression(snappyCodec))

	hd := histogramData{
		dnsLabels: dnsLabels{
			Label0: ptr("com"),
			Label1: ptr("example"),
			Label2: ptr("www"),
		},
		StartTime:             10,
		ACount:                11,
		AAAACount:             12,
		MXCount:               13,
		NSCount:               14,
		OtherTypeCount:        15,
		NonINCount:            16,
		OKCount:               17,
		NXCount:               18,
		FailCount:             19,
		OtherRcodeCount:       20,
		EDMStatusBits:         21,
		V4ClientCountHLLBytes: string(v4hll.ToBytes()),
		V6ClientCountHLLBytes: string(v6hll.ToBytes()),
	}

	for b.Loop() {
		_, err = parquetWriter.Write([]histogramData{hd})
		if err != nil {
			b.Fatalf("unable to call Write() on parquet writer: %s", err)
		}
	}
	err = parquetWriter.Close()
	if err != nil {
		b.Fatalf("unable to call WriteStop() on parquet writer: %s", err)
	}
}

func BenchmarkHgramWithHLLDefaults(b *testing.B) {
	b.ReportAllocs()

	hllSettings := getHllDefaults(0)
	err := hll.Defaults(hllSettings)
	if err != nil {
		b.Fatal(err)
	}

	ip4 := netip.MustParseAddr("198.51.100.20")

	v4Hash := murmur3.Sum64(ip4.AsSlice())

	for b.Loop() {
		hd := &histogramData{}
		hd.v4ClientHLL.AddRaw(v4Hash)
	}
}

func BenchmarkHgramWithHLLSettings(b *testing.B) {
	b.ReportAllocs()

	ip4 := netip.MustParseAddr("198.51.100.20")

	v4Hash := murmur3.Sum64(ip4.AsSlice())

	hllSettings := getHllDefaults(0)

	for b.Loop() {
		hd := &histogramData{}
		h, err := hll.NewHll(hllSettings)
		if err != nil {
			b.Fatal(err)
		}
		hd.v4ClientHLL = h
		hd.v4ClientHLL.AddRaw(v4Hash)
	}
}

func generateTestIPs(numIPv4, numIPv6 int, increment bool) []netip.Addr {
	ips := []netip.Addr{}

	ipv4Addr := netip.MustParseAddr("127.0.0.1")
	for range numIPv4 {
		ips = append(ips, ipv4Addr)
		if increment {
			ipv4Addr = ipv4Addr.Next()
		}
	}

	ipv6Addr := netip.MustParseAddr("::1")
	for range numIPv6 {
		ips = append(ips, ipv6Addr)
		if increment {
			ipv6Addr = ipv6Addr.Next()
		}
	}
	return ips
}

func TestWriteHistogramParquetExplicitThreshold(t *testing.T) {
	// Make sure we only include HLL data once the number of unique IPv4 or
	// IPv6 client IPs exceed the configured explicit threshold where we
	// start using probabilistic HLL data.
	discardLogger := slog.NewTextHandler(io.Discard, nil)
	logger := slog.New(discardLogger)

	edm, err := newDnstapMinimiser(logger, defaultTC)
	if err != nil {
		t.Fatalf("unable to setup edm: %s", err)
	}

	tests := []struct {
		description       string
		explicitThreshold int
		domains           []string
		ips               []netip.Addr
		ipv4HllIsNull     bool
		ipv6HllIsNull     bool
	}{
		{
			description:       "same number of IPv4/IPv6 as explicit threshold, should be NULL",
			ipv4HllIsNull:     true,
			ipv6HllIsNull:     true,
			explicitThreshold: 10,
			domains:           []string{"example.com.", "example.se."},
			ips:               generateTestIPs(10, 10, true),
		},
		{
			description:       "one more IPv4/IPv6 than explicit threshold, should not be NULL",
			ipv4HllIsNull:     false,
			ipv6HllIsNull:     false,
			explicitThreshold: 10,
			domains:           []string{"example.com.", "example.se."},
			ips:               generateTestIPs(11, 11, true),
		},
		{
			description:       "one more than explicit threshold but the same IPv4/IPv6, should be NULL",
			ipv4HllIsNull:     true,
			ipv6HllIsNull:     true,
			explicitThreshold: 10,
			domains:           []string{"example.com.", "example.se."},
			ips:               generateTestIPs(11, 11, false),
		},
	}

	for _, test := range tests {
		wkd := wellKnownDomainsData{
			m: map[int]*histogramData{},
		}

		hllSettings := getHllDefaults(test.explicitThreshold)

		d := dawg.New()
		for i, domain := range test.domains {
			wkd.m[i] = edm.newHistogramData(hllSettings, false)
			d.Add(domain)

			wkd.m[i].OKCount++
			wkd.m[i].NXCount += 2
			wkd.m[i].FailCount += 3
			wkd.m[i].ACount += 4
			wkd.m[i].AAAACount += 5
			wkd.m[i].MXCount += 6
			wkd.m[i].NSCount += 7
			wkd.m[i].OtherTypeCount += 8
			wkd.m[i].OtherRcodeCount += 9
			wkd.m[i].NonINCount += 10

			for _, ip := range test.ips {
				hllHash := murmur3.Sum64(ip.AsSlice())
				if ip.IsValid() {
					if ip.Unmap().Is4() {
						wkd.m[i].v4ClientHLL.AddRaw(hllHash)
					} else {
						wkd.m[i].v6ClientHLL.AddRaw(hllHash)
					}
				}
			}

		}
		wkd.dawgFinder = d.Finish()

		startTime := time.Time{}
		var b bytes.Buffer
		err := edm.writeHistogramParquet(&b, startTime, &wkd, defaultLabelLimit)
		if err != nil {
			t.Fatal(err)
		}

		r := bytes.NewReader(b.Bytes())
		rows, err := parquet.Read[histogramData](r, int64(r.Len()))
		if err != nil {
			t.Fatal(err)
		}

		for _, row := range rows {
			if test.ipv4HllIsNull && len(row.V4ClientCountHLLBytes) != 0 {
				t.Fatalf("IPv4 HLL data should be length 0 but is %d", len(row.V4ClientCountHLLBytes))
			}
			if !test.ipv4HllIsNull && len(row.V4ClientCountHLLBytes) == 0 {
				t.Fatal("IPv4 HLL data is 0 when it should have content")
			}
			if test.ipv6HllIsNull && len(row.V6ClientCountHLLBytes) != 0 {
				t.Fatalf("IPv6 HLL data should be length 0 but is %d", len(row.V6ClientCountHLLBytes))
			}
			if !test.ipv6HllIsNull && len(row.V6ClientCountHLLBytes) == 0 {
				t.Fatal("IPv6 HLL data is 0 when it should have content")
			}
		}
	}
}
