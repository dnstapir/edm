package runner

import (
	"context"
	"net"
	"sync"
	"testing"

	dnstap "github.com/dnstap/golang-dnstap"
	dnsv1 "github.com/miekg/dns"
)

// BenchmarkMinimiserResponse measures a well-known-domain response through runMinimiser.
func BenchmarkMinimiserResponse(b *testing.B) {
	edm, seenQnameLRU, pdb, wkdTracker := newRunMinimiserTestFixture(b, "www.example.com.")
	cryptopanCache := edm.testCryptopanCache()
	b.Cleanup(edm.testResetCryptopanCache)

	response := new(dnsv1.Msg)
	response.SetQuestion("www.example.com.", dnsv1.TypeA)
	response.Response = true
	response.RecursionDesired = true
	response.RecursionAvailable = true
	response.Compress = true
	response.Answer = []dnsv1.RR{&dnsv1.A{
		Hdr: dnsv1.RR_Header{
			Name:   "www.example.com.",
			Rrtype: dnsv1.TypeA,
			Class:  dnsv1.ClassINET,
			Ttl:    300,
		},
		A: net.IPv4(192, 0, 2, 1),
	}}
	response.SetEdns0(1232, false)
	wire, err := response.Pack()
	if err != nil {
		b.Fatal(err)
	}
	frame := marshaledDnstap(b, testDnstapMessage(b, dnstap.Message_CLIENT_RESPONSE, dnstap.SocketFamily_INET, wire))

	ctx, cancel := context.WithCancel(b.Context())
	var wg sync.WaitGroup
	wg.Go(func() {
		edm.runMinimiser(ctx, 0, edm.reloadMinimiserConfigCh[0], cryptopanCache, seenQnameLRU, &pebbleSeenQnameStore{db: pdb}, nil, defaultLabelLimit, wkdTracker)
	})

	// Warm the cryptopan cache and wait until the worker is ready.
	edm.inputChannel <- frame
	<-wkdTracker.updateCh

	b.ReportAllocs()
	for b.Loop() {
		edm.inputChannel <- frame
		<-wkdTracker.updateCh
	}

	cancel()
	wg.Wait()
}
