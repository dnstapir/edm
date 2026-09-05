package runner

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/miekg/dns"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	"github.com/segmentio/go-hll"
	"github.com/smhanov/dawg"
	"github.com/twmb/murmur3"
)

func TestSetHistogramLabels(t *testing.T) {
	// The reason the labels are "backwards" is because we define "label0"
	// in the struct as the rightmost DNS label, e.g. "com", "net" etc.
	name := "label9.label8.label7.label6.label5.label4.label3.label2.label1.label0."
	labels := dns.SplitDomainName(name)

	// Reverse labels to get easier comparision matching (offset 0 -> label0)
	compLabels := slices.Clone(labels)
	slices.Reverse(compLabels)

	edm := &DnstapMinimiser{}
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

	edm := &DnstapMinimiser{}
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
			Label0: new("com"),
			Label1: new("example"),
			Label2: new("www"),
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
		V4ClientCountHLLBytes: v4hll.ToBytes(),
		V6ClientCountHLLBytes: v6hll.ToBytes(),
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
		f, err := os.Create(filepath.Join(t.TempDir(), "generated-histogram.parquet"))
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
			Label0: new("com"),
			Label1: new("example"),
			Label2: new("www"),
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
		V4ClientCountHLLBytes: v4hll.ToBytes(),
		V6ClientCountHLLBytes: v6hll.ToBytes(),
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
	edm := newTestDnstapMinimiser(t, defaultTC)

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
			if test.ipv4HllIsNull && row.V4ClientCountHLLBytes != nil {
				t.Fatalf("IPv4 HLL data should be nil but is %#v", row.V4ClientCountHLLBytes)
			}
			if !test.ipv4HllIsNull && len(row.V4ClientCountHLLBytes) == 0 {
				t.Fatal("IPv4 HLL data is 0 when it should have content")
			}
			if test.ipv6HllIsNull && row.V6ClientCountHLLBytes != nil {
				t.Fatalf("IPv6 HLL data should be nil but is %#v", row.V6ClientCountHLLBytes)
			}
			if !test.ipv6HllIsNull && len(row.V6ClientCountHLLBytes) == 0 {
				t.Fatal("IPv6 HLL data is 0 when it should have content")
			}
		}
	}
}

func TestParseHLLStorageTypeErrors(t *testing.T) {
	if _, err := parseHllStorageType(nil); err == nil {
		t.Fatal("empty HLL bytes succeeded")
	}
	if _, err := parseHllStorageType([]byte{0x20}); err == nil {
		t.Fatal("unsupported HLL version succeeded")
	}

	h, err := hll.NewHll(getHllDefaults(10))
	if err != nil {
		t.Fatal(err)
	}
	storageType, err := parseHllStorageType(h.ToBytes())
	if err != nil {
		t.Fatal(err)
	}
	if storageType != hllEmpty {
		t.Fatalf("storage type = %v, want hllEmpty", storageType)
	}
}

func TestNewHistogramDataAndWriteParquet(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)

	exact := edm.newHistogramData(getHllDefaults(0), false)
	if exact.EDMStatusBits != uint64(edmStatusWellKnownExact) {
		t.Fatalf("exact status = %d", exact.EDMStatusBits)
	}
	wildcard := edm.newHistogramData(getHllDefaults(0), true)
	if wildcard.EDMStatusBits != uint64(edmStatusWellKnownWildcard) {
		t.Fatalf("wildcard status = %d", wildcard.EDMStatusBits)
	}

	finder := testDawgFinder(t, "example.com.")
	wkd := &wellKnownDomainsData{
		m:          map[int]*histogramData{0: exact},
		dawgFinder: finder,
	}
	exact.ACount = 1
	exact.v4ClientHLL.AddRaw(murmur3.Sum64(netip.MustParseAddr("198.51.100.20").AsSlice()))

	var buf bytes.Buffer
	if err := edm.writeHistogramParquet(&buf, time.Unix(10, 0), wkd, defaultLabelLimit); err != nil {
		t.Fatal(err)
	}
	rows, err := parquet.Read[histogramData](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ACount != 1 || rows[0].V4ClientCount == 0 {
		t.Fatalf("unexpected rows: %#v", rows)
	}

	badWKD := &wellKnownDomainsData{m: map[int]*histogramData{99: exact}, dawgFinder: finder}
	if err := edm.writeHistogramParquet(io.Discard, time.Time{}, badWKD, defaultLabelLimit); err == nil {
		t.Fatal("writeHistogramParquet with bad DAWG index succeeded")
	}
}

func TestHistogramSender(t *testing.T) {
	ctx := t.Context()

	wkd := wellKnownDomainsData{
		m: map[int]*histogramData{},
	}

	t.Run("initially disabled sender", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			// setup
			edm := newTestDnstapMinimiser(t, defaultTC)
			resetTester(edm, false)
			edm.conf.DisableHistogramSender = true
			// start
			go edm.histogramSender(ctx, defaultLabelLimit)
			// send
			edm.histogramWriterCh <- &wkd
			// wait until histogramSender is idling
			synctest.Wait()
			// check
			checkHistogramQueueLength(t, edm, 0, 0, 0)
			// cleanup
			close(edm.histogramWriterCh)
		})
	})

	t.Run("initially disabled sender (retry)", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			// setup
			edm := newTestDnstapMinimiser(t, defaultTC)
			resetTester(edm, false)
			edm.conf.DisableHistogramSender = true
			// start
			go edm.histogramSender(ctx, defaultLabelLimit)
			// wait for startup(unbuffered channel)
			edm.reloadHistogramSenderConfigCh <- struct{}{}
			// send on retry channel
			edm.histogramRetryWriterCh <- &wkd
			edm.histogramRetryWriterCh <- &wkd
			edm.histogramRetryWriterCh <- &wkd
			edm.histogramRetryWriterCh <- &wkd
			// wait for retry to trigger
			time.Sleep(edm.deps.HistogramSenderRetryInterval)
			// wait until histogramSender is idling
			synctest.Wait()
			// check
			checkHistogramQueueLength(t, edm, 0, 0, 0)
			// cleanup
			close(edm.histogramWriterCh)
		})
	})

	t.Run("initially enabled sender (no aggreg)", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			// setup
			edm := newTestDnstapMinimiser(t, defaultTC)
			resetTester(edm, false)
			edm.aggregSender = nil
			edm.conf.DisableHistogramSender = false
			// start
			go edm.histogramSender(ctx, defaultLabelLimit)
			// send
			edm.histogramWriterCh <- &wkd
			// finish up
			close(edm.histogramWriterCh)
			synctest.Wait()
			// to satisfy check function below, set aggregSender
			edm.aggregSender = &testAggregSender{}
			// check
			checkHistogramQueueLength(t, edm, 0, 0, 1)
		})
	})

	t.Run("initially enabled sender (failing aggreg)", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			// setup
			edm := newTestDnstapMinimiser(t, defaultTC)
			resetTester(edm, true)
			edm.conf.DisableHistogramSender = false
			// start
			go edm.histogramSender(ctx, defaultLabelLimit)
			// send
			edm.histogramWriterCh <- &wkd
			// wait until histogramSender is idling
			synctest.Wait()
			// check
			checkHistogramQueueLength(t, edm, 1, 0, 1)
			// wait for retry to trigger
			time.Sleep(edm.deps.HistogramSenderRetryInterval)
			// wait until histogramSender is idling
			synctest.Wait()
			// check
			checkHistogramQueueLength(t, edm, 2, 0, 0)
			// cleanup
			close(edm.histogramWriterCh)
		})
	})

	t.Run("initially enabled sender (working aggreg)", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			// setup
			edm := newTestDnstapMinimiser(t, defaultTC)
			resetTester(edm, false)
			edm.conf.DisableHistogramSender = false
			// start
			go edm.histogramSender(ctx, defaultLabelLimit)
			// send
			edm.histogramWriterCh <- &wkd
			// wait until histogramSender is idling
			synctest.Wait()
			// check
			checkHistogramQueueLength(t, edm, 1, 0, 0)
			// cleanup
			close(edm.histogramWriterCh)
		})
	})

	t.Run("if nil pointer", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			// setup
			edm := newTestDnstapMinimiser(t, defaultTC)
			resetTester(edm, false)
			edm.conf.DisableHistogramSender = false
			// start
			go edm.histogramSender(ctx, defaultLabelLimit)
			// send nil
			edm.histogramWriterCh <- nil
			// wait until histogramSender is idling
			synctest.Wait()
			// check
			checkHistogramQueueLength(t, edm, 0, 0, 0)
			// cleanup
			close(edm.histogramWriterCh)
		})
	})

	t.Run("configuration reload", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			// setup (sender disabled)
			edm := newTestDnstapMinimiser(t, defaultTC)
			resetTester(edm, false)
			edm.conf.DisableHistogramSender = true
			// start
			go edm.histogramSender(ctx, defaultLabelLimit)
			// wait for startup(unbuffered channel)
			edm.reloadHistogramSenderConfigCh <- struct{}{}
			// send
			edm.histogramWriterCh <- &wkd
			// wait until histogramSender is idling
			synctest.Wait()
			// check
			checkHistogramQueueLength(t, edm, 0, 0, 0)
			// now enable sender
			edm.confMutex.Lock()
			edm.conf.DisableHistogramSender = false
			edm.confMutex.Unlock()
			edm.reloadHistogramSenderConfigCh <- struct{}{}
			// send
			edm.histogramWriterCh <- &wkd
			// wait until histogramSender is idling
			synctest.Wait()
			// check
			checkHistogramQueueLength(t, edm, 1, 0, 0)
			// now disable sender
			edm.confMutex.Lock()
			edm.conf.DisableHistogramSender = true
			edm.confMutex.Unlock()
			edm.reloadHistogramSenderConfigCh <- struct{}{}
			// send
			edm.histogramWriterCh <- &wkd
			// wait until histogramSender is idling
			synctest.Wait()
			// check
			checkHistogramQueueLength(t, edm, 1, 0, 0)
			// cleanup
			close(edm.histogramWriterCh)
		})
	})
}

func resetTester(edm *DnstapMinimiser, aggregFail bool) {
	// make reload channel unbuffered to use as a synchronization channel
	edm.histogramWriterCh = make(chan *wellKnownDomainsData)
	// leave retry channel as buffered
	edm.histogramRetryWriterCh = make(chan *wellKnownDomainsData, 4)
	// make reload channel unbuffered to use as a synchronization channel
	edm.reloadHistogramSenderConfigCh = make(chan struct{})
	// setup aggreg sender
	edm.aggregSender = &testAggregSender{
		Fail: aggregFail,
	}
}

func checkHistogramQueueLength(t *testing.T, edm *DnstapMinimiser, aggregSentCount int, primary int, retry int) {
	t.Helper()
	edm.aggregSenderMutex.RLock()
	tAS, ok := edm.aggregSender.(*testAggregSender)
	edm.aggregSenderMutex.RUnlock()
	if !ok {
		t.Fatal("not testAggregSender")
	}
	count := tAS.Count()
	if count != aggregSentCount {
		t.Fatalf("number of aggreg sents are incorrect, expected: %v got: %v", aggregSentCount, count)
	}
	if len(edm.histogramWriterCh) != primary {
		t.Fatalf("histogramWriterCh length incorrect, expected: %v got: %v", primary, len(edm.histogramWriterCh))
	}
	if len(edm.histogramRetryWriterCh) != retry {
		t.Fatalf("histogramRetryWriterCh length incorrect, expected: %v got: %v", retry, len(edm.histogramRetryWriterCh))
	}
}

type testAggregSender struct {
	Fail  bool
	count int
	lock  sync.Mutex
}

func (a *testAggregSender) Send(context.Context, *bytes.Buffer, time.Time, time.Duration) error {
	// lock
	a.lock.Lock()
	defer a.lock.Unlock()
	// update aggred send count
	a.count++
	// return
	if a.Fail {
		return errors.New("fail")
	}
	return nil
}
func (a *testAggregSender) CloseIdleConnections() {}
func (a *testAggregSender) Count() int {
	// lock
	a.lock.Lock()
	defer a.lock.Unlock()
	// return count
	return a.count
}
