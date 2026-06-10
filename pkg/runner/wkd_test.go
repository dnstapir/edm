package runner

import (
	"errors"
	"io"
	"log/slog"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/smhanov/dawg"
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
		i, suffixMatch := getDawgIndex(wkd.snap.Load().dawgFinder, m.Question[0].Name)

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

func TestRotateTrackerUsesSafeDawgLoader(t *testing.T) {
	dBuilder := dawg.New()
	dBuilder.Add("example.com.")
	dFinder := dBuilder.Finish()

	dawgFile := filepath.Join(t.TempDir(), "well-known-domains.dawg")
	if _, err := dFinder.Save(dawgFile); err != nil {
		t.Fatalf("Save: %s", err)
	}
	fileInfo, err := os.Stat(dawgFile)
	if err != nil {
		t.Fatalf("Stat: %s", err)
	}

	wkd, err := newWellKnownDomainsTracker(dFinder, fileInfo.ModTime())
	if err != nil {
		t.Fatalf("newWellKnownDomainsTracker: %s", err)
	}
	edm := &DnstapMinimiser{
		log:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		deps: defaultDependencies(),
	}

	if err := os.WriteFile(dawgFile, nil, 0o600); err != nil {
		t.Fatalf("WriteFile: %s", err)
	}
	nextModTime := fileInfo.ModTime().Add(time.Second)
	if err := os.Chtimes(dawgFile, nextModTime, nextModTime); err != nil {
		t.Fatalf("Chtimes: %s", err)
	}

	if _, err := wkd.rotateTracker(edm, dawgFile, time.Time{}, time.Now()); !errors.Is(err, errEmptyDawgFile) {
		t.Fatalf("rotateTracker error have: %v, want: %v", err, errEmptyDawgFile)
	}
}

func TestWellKnownDomainUpdatesAndRotation(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	path := testDawgFile(t, "example.com.")
	finder, modTime, err := (realDawgLoader{fs: osFileSystem{}}).LoadDawgFile(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = finder.Close() })

	wkd, err := newWellKnownDomainsTracker(finder, modTime)
	if err != nil {
		t.Fatal(err)
	}
	msg := new(dns.Msg)
	msg.SetQuestion("example.com.", dns.TypeMX)
	msg.Rcode = dns.RcodeNameError
	wkd.sendUpdate(netip.MustParseAddr("198.51.100.20").AsSlice(), msg, 0, false, modTime)

	select {
	case wu := <-wkd.updateCh:
		if wu.NXCount != 1 || wu.MXCount != 1 || !wu.ip.IsValid() || wu.hllHash == 0 {
			t.Fatalf("unexpected update: %#v", wu)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for update")
	}

	prev, err := wkd.rotateTracker(edm, path, time.Unix(0, 0), time.Unix(60, 0))
	if err != nil {
		t.Fatal(err)
	}
	if !prev.startTime.Equal(time.Unix(0, 0)) || !prev.rotationTime.Equal(time.Unix(60, 0)) || len(wkd.m) != 0 {
		t.Fatalf("unexpected rotation state: %#v", prev)
	}

	if _, err := wkd.rotateTracker(edm, filepath.Join(t.TempDir(), "missing.dawg"), time.Unix(0, 0), time.Now()); err == nil {
		t.Fatal("rotateTracker with missing file succeeded")
	}
}

func TestUpdateRetryer(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	finder := testDawgFinder(t, "example.com.")
	wkd, err := newWellKnownDomainsTracker(finder, time.Unix(2, 0))
	if err != nil {
		t.Fatal(err)
	}
	msg := new(dns.Msg)
	msg.SetQuestion("example.com.", dns.TypeA)

	var wg sync.WaitGroup
	wg.Add(1)
	go wkd.updateRetryer(edm, &wg)
	wkd.retryCh <- wkdUpdate{msg: msg, dawgModTime: time.Unix(1, 0), retryLimit: 2}
	close(wkd.retryCh)

	select {
	case wu := <-wkd.updateCh:
		if wu.retry != 1 || wu.dawgIndex != 0 || wu.dawgModTime != time.Unix(2, 0) {
			t.Fatalf("unexpected retried update: %#v", wu)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for retried update")
	}
	wg.Wait()
	<-wkd.retryerDone
}

// TestSendUpdateBranches exercises the rcode/qtype switch arms and the
// invalid-IP-slice fallback in sendUpdate. TestWellKnownDomainUpdatesAndRotation
// already covers the RcodeNameError+TypeMX path; this drives the rest.
func TestSendUpdateBranches(t *testing.T) {
	finder := testDawgFinder(t, "example.com.")
	wkd, err := newWellKnownDomainsTracker(finder, time.Unix(2, 0))
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name    string
		ipBytes []byte
		rcode   int
		qtype   uint16
		qclass  uint16
		check   func(t *testing.T, wu wkdUpdate)
	}{
		{
			name:    "ServerFailure rcode AAAA in",
			ipBytes: netip.MustParseAddr("198.51.100.20").AsSlice(),
			rcode:   dns.RcodeServerFailure,
			qtype:   dns.TypeAAAA,
			qclass:  dns.ClassINET,
			check: func(t *testing.T, wu wkdUpdate) {
				if wu.FailCount != 1 || wu.AAAACount != 1 {
					t.Fatalf("FailCount/AAAACount: %#v", wu)
				}
			},
		},
		{
			name:    "Other rcode NS in",
			ipBytes: netip.MustParseAddr("198.51.100.20").AsSlice(),
			rcode:   dns.RcodeRefused,
			qtype:   dns.TypeNS,
			qclass:  dns.ClassINET,
			check: func(t *testing.T, wu wkdUpdate) {
				if wu.OtherRcodeCount != 1 || wu.NSCount != 1 {
					t.Fatalf("OtherRcode/NSCount: %#v", wu)
				}
			},
		},
		{
			name:    "Success Other-type in",
			ipBytes: netip.MustParseAddr("198.51.100.20").AsSlice(),
			rcode:   dns.RcodeSuccess,
			qtype:   dns.TypeSRV,
			qclass:  dns.ClassINET,
			check: func(t *testing.T, wu wkdUpdate) {
				if wu.OKCount != 1 || wu.OtherTypeCount != 1 {
					t.Fatalf("OK/OtherType: %#v", wu)
				}
			},
		},
		{
			name:    "Non-INET class",
			ipBytes: netip.MustParseAddr("198.51.100.20").AsSlice(),
			rcode:   dns.RcodeSuccess,
			qtype:   dns.TypeA,
			qclass:  dns.ClassCHAOS,
			check: func(t *testing.T, wu wkdUpdate) {
				if wu.NonINCount != 1 {
					t.Fatalf("NonINCount: %#v", wu)
				}
			},
		},
		{
			name:    "Bad IP slice leaves ip invalid",
			ipBytes: []byte{1, 2, 3},
			rcode:   dns.RcodeSuccess,
			qtype:   dns.TypeA,
			qclass:  dns.ClassINET,
			check: func(t *testing.T, wu wkdUpdate) {
				if wu.ip.IsValid() {
					t.Fatalf("expected invalid ip from short slice; got %v", wu.ip)
				}
				if wu.hllHash != 0 {
					t.Fatalf("expected zero hllHash from short slice; got %d", wu.hllHash)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg := new(dns.Msg)
			msg.SetQuestion("example.com.", tc.qtype)
			msg.Question[0].Qclass = tc.qclass
			msg.Rcode = tc.rcode
			wkd.sendUpdate(tc.ipBytes, msg, 0, false, time.Unix(2, 0))
			select {
			case wu := <-wkd.updateCh:
				tc.check(t, wu)
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for update")
			}
		})
	}
}

// TestUpdateRetryerBranches drives the two skip arms of updateRetryer
// that TestUpdateRetryer (which covers the happy resend path) does not
// reach: hitting the retry limit and the dawgNotFound case where the
// reloaded tracker no longer recognises the qname.
func TestUpdateRetryerBranches(t *testing.T) {
	t.Run("retry limit reached drops update", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		finder := testDawgFinder(t, "example.com.")
		wkd, err := newWellKnownDomainsTracker(finder, time.Unix(2, 0))
		if err != nil {
			t.Fatal(err)
		}
		msg := new(dns.Msg)
		msg.SetQuestion("example.com.", dns.TypeA)

		var wg sync.WaitGroup
		wg.Add(1)
		go wkd.updateRetryer(edm, &wg)
		// retry is 1 BEFORE the increment, becomes 2 after — equal to
		// retryLimit, so the skip arm fires and no resend reaches updateCh.
		wkd.retryCh <- wkdUpdate{msg: msg, dawgModTime: time.Unix(1, 0), retry: 1, retryLimit: 2}
		close(wkd.retryCh)
		wg.Wait()
		<-wkd.retryerDone

		select {
		case wu := <-wkd.updateCh:
			t.Fatalf("expected no resend, got %#v", wu)
		default:
		}
	})

	t.Run("dawgNotFound drops update", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		// Tracker only knows example.com; the retry will look up a
		// different qname so wkd.lookup returns dawgNotFound and the
		// retryer drops the update.
		finder := testDawgFinder(t, "example.com.")
		wkd, err := newWellKnownDomainsTracker(finder, time.Unix(2, 0))
		if err != nil {
			t.Fatal(err)
		}
		msg := new(dns.Msg)
		msg.SetQuestion("unknown.example.", dns.TypeA)

		var wg sync.WaitGroup
		wg.Add(1)
		go wkd.updateRetryer(edm, &wg)
		wkd.retryCh <- wkdUpdate{msg: msg, dawgModTime: time.Unix(1, 0), retryLimit: 5}
		close(wkd.retryCh)
		wg.Wait()
		<-wkd.retryerDone

		select {
		case wu := <-wkd.updateCh:
			t.Fatalf("expected no resend on dawgNotFound, got %#v", wu)
		default:
		}
	})
}
