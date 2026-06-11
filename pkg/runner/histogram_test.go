package runner

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
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
	"github.com/spaolacci/murmur3"
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
	edm := newTestDnstapMinimiser(t, defaultTC)
	edm.deps.HistogramSenderInterval = time.Millisecond
	edm.deps.HistogramSenderBackoff = time.Millisecond
	edm.reloadHistogramSenderConfigCh = make(chan struct{}, 1)
	outboxDir := filepath.Join(t.TempDir(), "outbox")
	sentDir := filepath.Join(t.TempDir(), "sent")
	if err := os.MkdirAll(outboxDir, 0o750); err != nil {
		t.Fatal(err)
	}
	name := "dns_histogram-2026-05-28T12-00-00Z_2026-05-28T12-01-00Z.parquet"
	if err := os.WriteFile(filepath.Join(outboxDir, name), []byte("payload"), 0o600); err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "/ok")
		w.WriteHeader(http.StatusCreated)
	}))
	t.Cleanup(server.Close)
	u, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	as, err := newAggregateSender(edm.log, u, testJWK(t), nil, edm.httpClientCertStore.getClientCertificate, edm.deps.FileSystem, edm.deps.Clock)
	if err != nil {
		t.Fatal(err)
	}
	edm.aggregSender = as

	ctx, cancel := testRunContext(t)
	var wg sync.WaitGroup
	wg.Add(1)
	go edm.histogramSender(ctx, outboxDir, sentDir, &wg)
	for range 200 {
		if _, err := os.Stat(filepath.Join(sentDir, name)); err == nil {
			cancel()
			wg.Wait()
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	t.Fatal("histogramSender did not move sent file")
}

// TestHistogramSenderBranches covers the histogramSender arms that
// TestHistogramSender (the happy send-and-rename path) does not reach:
// disabled-at-startup, parse-error filename, send-error backoff, and
// the reload arm that flips DisableHistogramSender at runtime.
func TestHistogramSenderBranches(t *testing.T) {
	t.Run("disabled at startup skips ticks", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			tc := defaultTC
			tc.DisableHistogramSender = true
			edm := newSynctestDnstapMinimiser(t, tc)
			edm.deps.HistogramSenderInterval = time.Millisecond
			edm.reloadHistogramSenderConfigCh = make(chan struct{}, 1)

			buf := &syncBuf{}
			edm.log = slog.New(slog.NewJSONHandler(buf, nil))

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var wg sync.WaitGroup
			wg.Add(1)
			go edm.histogramSender(ctx, t.TempDir(), t.TempDir(), &wg)
			// Let several ticks elapse; nothing happens because the
			// DisableHistogramSender guard short-circuits.
			time.Sleep(20 * time.Millisecond)
			cancel()
			wg.Wait()

			if !strings.Contains(buf.String(), `"state":"disabled"`) {
				t.Fatalf("expected disabled-state log, got: %q", buf.String())
			}
		})
	})

	t.Run("parse-error filename is logged and skipped", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		edm.deps.HistogramSenderInterval = time.Millisecond
		edm.reloadHistogramSenderConfigCh = make(chan struct{}, 1)
		outboxDir := t.TempDir()
		sentDir := t.TempDir()
		// Filename has the expected prefix/suffix but a malformed
		// timestamp section, so timestampsFromFilename errors out.
		badName := "dns_histogram-not-a-timestamp.parquet"
		if err := os.WriteFile(filepath.Join(outboxDir, badName), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}

		buf := &syncBuf{}
		edm.log = slog.New(slog.NewJSONHandler(buf, nil))

		ctx, cancel := testRunContext(t)
		var wg sync.WaitGroup
		wg.Add(1)
		go edm.histogramSender(ctx, outboxDir, sentDir, &wg)
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if strings.Contains(buf.String(), "unable to parse timestamps from histogram filename") {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		cancel()
		wg.Wait()

		if !strings.Contains(buf.String(), "unable to parse timestamps from histogram filename") {
			t.Fatalf("expected parse-error log, got: %q", buf.String())
		}
	})

	t.Run("send error triggers backoff log", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		edm.deps.HistogramSenderInterval = time.Millisecond
		// Keep the backoff short so the test does not wait the real backoff.
		edm.deps.HistogramSenderBackoff = time.Millisecond
		edm.reloadHistogramSenderConfigCh = make(chan struct{}, 1)
		outboxDir := t.TempDir()
		sentDir := t.TempDir()
		name := "dns_histogram-2026-05-28T12-00-00Z_2026-05-28T12-01-00Z.parquet"
		if err := os.WriteFile(filepath.Join(outboxDir, name), []byte("payload"), 0o600); err != nil {
			t.Fatal(err)
		}

		// Aggregate sender points at an unreachable URL so send fails.
		u, err := url.Parse("http://127.0.0.1:1")
		if err != nil {
			t.Fatal(err)
		}
		as, err := newAggregateSender(edm.log, u, testJWK(t), nil, edm.httpClientCertStore.getClientCertificate, edm.deps.FileSystem, edm.deps.Clock)
		if err != nil {
			t.Fatal(err)
		}
		edm.aggregSender = as

		buf := &syncBuf{}
		edm.log = slog.New(slog.NewJSONHandler(buf, nil))

		ctx, cancel := testRunContext(t)
		var wg sync.WaitGroup
		wg.Add(1)
		go edm.histogramSender(ctx, outboxDir, sentDir, &wg)
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if strings.Contains(buf.String(), "unable to send histogram file") {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		cancel()
		wg.Wait()

		if !strings.Contains(buf.String(), "unable to send histogram file") {
			t.Fatalf("expected send-error log, got: %q", buf.String())
		}
	})

	t.Run("backoff interrupted by stop", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		edm.deps.HistogramSenderInterval = time.Millisecond
		// A long backoff: a non-interruptible wait would block shutdown for the
		// full minute, so exiting promptly proves stop() interrupts the backoff.
		edm.deps.HistogramSenderBackoff = time.Minute
		edm.reloadHistogramSenderConfigCh = make(chan struct{}, 1)
		outboxDir := t.TempDir()
		sentDir := t.TempDir()
		name := "dns_histogram-2026-05-28T12-00-00Z_2026-05-28T12-01-00Z.parquet"
		if err := os.WriteFile(filepath.Join(outboxDir, name), []byte("payload"), 0o600); err != nil {
			t.Fatal(err)
		}

		// Aggregate sender points at an unreachable URL so the send fails and
		// the sender enters its backoff.
		u, err := url.Parse("http://127.0.0.1:1")
		if err != nil {
			t.Fatal(err)
		}
		as, err := newAggregateSender(edm.log, u, testJWK(t), nil, edm.httpClientCertStore.getClientCertificate, edm.deps.FileSystem, edm.deps.Clock)
		if err != nil {
			t.Fatal(err)
		}
		edm.aggregSender = as

		buf := &syncBuf{}
		edm.log = slog.New(slog.NewJSONHandler(buf, nil))

		ctx, cancel := testRunContext(t)
		var wg sync.WaitGroup
		wg.Add(1)
		go edm.histogramSender(ctx, outboxDir, sentDir, &wg)

		// Wait until the send has failed and the sender is in its backoff.
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if strings.Contains(buf.String(), "unable to send histogram file") {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if !strings.Contains(buf.String(), "unable to send histogram file") {
			t.Fatalf("sender did not reach backoff: %q", buf.String())
		}

		// Cancel during the in-flight one-minute backoff; histogramSender must
		// exit promptly instead of waiting it out.
		cancel()
		waitOrFail(t, &wg, 2*time.Second, "histogramSender did not exit when cancelled during backoff")
	})

	t.Run("reload toggles enabled state", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			tc := defaultTC
			tc.DisableHistogramSender = true
			edm := newSynctestDnstapMinimiser(t, tc)
			edm.deps.HistogramSenderInterval = time.Millisecond
			edm.reloadHistogramSenderConfigCh = make(chan struct{}, 1)

			buf := &syncBuf{}
			edm.log = slog.New(slog.NewJSONHandler(buf, nil))

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var wg sync.WaitGroup
			wg.Add(1)
			go edm.histogramSender(ctx, t.TempDir(), t.TempDir(), &wg)

			// Wait until the worker has read its startup conf before flipping
			// edm.conf — otherwise we race the worker's edm.getConfig() at
			// histogramSender's entry and it may pick up the post-flip value.
			time.Sleep(edm.deps.HistogramSenderInterval)
			synctest.Wait()
			if !strings.Contains(buf.String(), `"state":"disabled"`) {
				t.Fatalf("worker did not log the initial disabled state: %q", buf.String())
			}

			// Flip DisableHistogramSender on edm.conf and signal a reload.
			edm.confMutex.Lock()
			edm.conf.DisableHistogramSender = false
			edm.confMutex.Unlock()
			edm.reloadHistogramSenderConfigCh <- struct{}{}
			synctest.Wait()
			cancel()
			wg.Wait()

			if strings.Contains(buf.String(), "enabling histogram sender") {
				return
			}
			t.Fatalf("expected enable log, got: %q", buf.String())
		})
	})
}

// TestHistogramWriterLogsCreateError mirrors the session writer test for the
// histogram writer worker.
func TestHistogramWriterLogsCreateError(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	var buf bytes.Buffer
	edm.log = slog.New(slog.NewJSONHandler(&buf, nil))

	edm.deps.FileSystem = faultingFileSystem{fileSystem: edm.deps.FileSystem, create: func(string) (fsFile, error) { return nil, errInjected }}

	edm.histogramWriterCh <- &wellKnownDomainsData{
		rotationTime: time.Now(),
		m:            map[int]*histogramData{},
	}
	close(edm.histogramWriterCh)

	var wg sync.WaitGroup
	wg.Add(1)
	go edm.histogramWriter(defaultLabelLimit, t.TempDir(), &wg)
	waitForWaitGroup(t, &wg, 5*time.Second, "histogramWriter did not exit")

	if !strings.Contains(buf.String(), `"level":"ERROR"`) || !strings.Contains(buf.String(), "histogramWriter") {
		t.Fatalf("expected error log from histogramWriter, got: %q", buf.String())
	}
}
