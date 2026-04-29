package runner

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestTimestampToFileString locks in the on-disk filename format used for
// session/histogram parquet files: RFC 3339 with the colons rewritten to
// hyphens so the value is shell-safe without quoting. The format is part
// of the operator-visible interface (operators tail directories of these
// files and time-range tools parse them back into timestamps), so any
// accidental change — switching to ISO 8601 basic, dropping the tz
// suffix, etc. — needs to be a deliberate decision rather than slip in
// silently.
func TestTimestampToFileString(t *testing.T) {
	tests := []struct {
		name string
		in   time.Time
		want string
	}{
		{
			name: "UTC zero seconds",
			in:   time.Date(2026, 1, 2, 3, 4, 0, 0, time.UTC),
			want: "2026-01-02T03-04-00Z",
		},
		{
			name: "UTC with seconds",
			in:   time.Date(2026, 11, 29, 13, 50, 30, 0, time.UTC),
			want: "2026-11-29T13-50-30Z",
		},
		{
			name: "non-UTC offset is preserved verbatim",
			in:   time.Date(2026, 11, 29, 13, 50, 0, 0, time.FixedZone("CET", 60*60)),
			// Expect "+01-00" because all colons get replaced — that
			// is the existing behaviour and is what
			// timestampsFromFilename round-trips against.
			want: "2026-11-29T13-50-00+01-00",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := timestampToFileString(tc.in); got != tc.want {
				t.Fatalf("have: %s, want: %s", got, tc.want)
			}
		})
	}
}

// TestBuildParquetFilenames verifies that the helper composes the .tmp
// and final .parquet paths from the configured base directory, base
// name, and the start/stop timestamps. The ordering matters — the .tmp
// path is what gets written to during the parquet-write loop and is
// later atomically renamed to the final path; if buildParquetFilenames
// returned them swapped the entire writer pipeline would silently
// produce inverted filenames. So we pin the (tmp, final) ordering here.
//
// We also confirm that the timestamps are first converted to UTC: the
// helper calls .UTC() on its inputs, which means callers can pass
// in any timezone and still get stable, comparable filenames in the
// same day/hour bucket.
func TestBuildParquetFilenames(t *testing.T) {
	baseDir := filepath.Join("/var/lib/edm", "histograms")
	baseName := "dns_histogram"

	startUTC := time.Date(2026, 11, 29, 13, 50, 0, 0, time.UTC)
	stopUTC := time.Date(2026, 11, 29, 13, 51, 0, 0, time.UTC)

	wantTmp := filepath.Join(baseDir, "dns_histogram-2026-11-29T13-50-00Z_2026-11-29T13-51-00Z.parquet.tmp")
	wantFinal := filepath.Join(baseDir, "dns_histogram-2026-11-29T13-50-00Z_2026-11-29T13-51-00Z.parquet")

	gotTmp, gotFinal := buildParquetFilenames(baseDir, baseName, startUTC, stopUTC)
	if gotTmp != wantTmp {
		t.Fatalf("tmp filename\n  have: %s\n  want: %s", gotTmp, wantTmp)
	}
	if gotFinal != wantFinal {
		t.Fatalf("final filename\n  have: %s\n  want: %s", gotFinal, wantFinal)
	}
	if !strings.HasSuffix(gotTmp, ".parquet.tmp") {
		t.Fatalf("tmp filename does not end with .parquet.tmp: %s", gotTmp)
	}
	if !strings.HasSuffix(gotFinal, ".parquet") || strings.HasSuffix(gotFinal, ".tmp") {
		t.Fatalf("final filename has wrong suffix: %s", gotFinal)
	}

	// Same instants in a non-UTC location must produce the same
	// filenames as the UTC inputs above — this is the .UTC() contract.
	cet := time.FixedZone("CET", 60*60)
	startCET := startUTC.In(cet)
	stopCET := stopUTC.In(cet)
	gotTmpCET, gotFinalCET := buildParquetFilenames(baseDir, baseName, startCET, stopCET)
	if gotTmpCET != wantTmp || gotFinalCET != wantFinal {
		t.Fatalf("filenames differ when callers pass non-UTC times\n  utc tmp: %s\n  cet tmp: %s\n  utc fin: %s\n  cet fin: %s",
			wantTmp, gotTmpCET, wantFinal, gotFinalCET)
	}
}

// TestTimestampsFromFilenameRoundTrip verifies the inverse direction of
// the on-disk format: given a filename produced by buildParquetFilenames,
// timestampsFromFilename must return the exact (start, stop) instants
// that were embedded — no truncation, no timezone drift. This is what
// the histogram-sender uses to populate the Aggregate-Interval header
// (see aggregate_sender.send), so a parsing regression would corrupt
// the metadata reported to aggrec.
func TestTimestampsFromFilenameRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		start time.Time
		stop  time.Time
	}{
		{
			name:  "minute-aligned UTC",
			start: time.Date(2026, 11, 29, 13, 50, 0, 0, time.UTC),
			stop:  time.Date(2026, 11, 29, 13, 51, 0, 0, time.UTC),
		},
		{
			name:  "second-precision UTC",
			start: time.Date(2026, 11, 29, 13, 50, 17, 0, time.UTC),
			stop:  time.Date(2026, 11, 29, 13, 51, 42, 0, time.UTC),
		},
		{
			name: "non-UTC location is normalised to UTC by buildParquetFilenames",
			start: time.Date(2026, 11, 29, 14, 50, 0, 0,
				time.FixedZone("CET", 60*60)),
			stop: time.Date(2026, 11, 29, 14, 51, 0, 0,
				time.FixedZone("CET", 60*60)),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, finalPath := buildParquetFilenames("/tmp/edm", "dns_histogram", tc.start, tc.stop)

			// timestampsFromFilename takes a filename, not a path —
			// the production caller is histogramSender which builds
			// the filename by reading from outboxDir, so it passes
			// the bare basename.
			gotStart, gotStop, err := timestampsFromFilename(filepath.Base(finalPath))
			if err != nil {
				t.Fatalf("timestampsFromFilename(%s): %s", finalPath, err)
			}

			// The on-disk representation is always in UTC because
			// buildParquetFilenames calls .UTC() before formatting.
			// Compare against the UTC-normalised instants.
			if !gotStart.Equal(tc.start.UTC()) {
				t.Fatalf("start\n  have: %s\n  want: %s", gotStart, tc.start.UTC())
			}
			if !gotStop.Equal(tc.stop.UTC()) {
				t.Fatalf("stop\n  have: %s\n  want: %s", gotStop, tc.stop.UTC())
			}
		})
	}
}

// TestTimestampsFromFilenameMalformed exercises the error paths so a
// future refactor that, say, swapped strings.Split for a more permissive
// parser can't silently start accepting garbage. We expect a non-nil
// error for each malformed input — we don't pin the exact wording
// because that's an internal detail.
//
// The "missing _ separator" case is special: before the length-check
// fix, this input crashed timestampsFromFilename with an index-out-of-
// range panic, which would in turn crash the histogramSender goroutine
// (runner.go ~line 2489) when a stray malformed file landed in the
// outbox directory. Now it returns a clean error and the histogramSender
// can log + skip the file, as it does for the other malformed cases.
func TestTimestampsFromFilenameMalformed(t *testing.T) {
	cases := []struct {
		name string
		in   string
	}{
		{
			name: "missing _ between start and stop",
			in:   "dns_histogram-2026-11-29T13-50-00Z.parquet",
		},
		{
			name: "missing - between prefix and timestamps",
			in:   "dns_histogram.parquet",
		},
		{
			name: "non-RFC3339 start",
			in:   "dns_histogram-not-a-time_2026-11-29T13-51-00Z.parquet",
		},
		{
			name: "non-RFC3339 stop",
			in:   "dns_histogram-2026-11-29T13-50-00Z_also-not-a-time.parquet",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := timestampsFromFilename(tc.in); err == nil {
				t.Fatalf("expected error for input %q, got nil", tc.in)
			}
		})
	}
}
