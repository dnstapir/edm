package runner

import (
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
)

func TestISO8601Duration(t *testing.T) {
	tests := []struct {
		name string
		in   time.Duration
		want string
	}{
		{
			name: "zero",
			in:   0,
			want: "PT0S",
		},
		{
			name: "seconds",
			in:   45 * time.Second,
			want: "PT45S",
		},
		{
			name: "minutes",
			in:   time.Minute,
			want: "PT1M",
		},
		{
			name: "mixed",
			in:   time.Hour + 2*time.Minute + 3*time.Second,
			want: "PT1H2M3S",
		},
		{
			name: "fractional seconds",
			in:   1500 * time.Millisecond,
			want: "PT1.5S",
		},
		{
			name: "negative clamps to zero",
			in:   -time.Second,
			want: "PT0S",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := iso8601Duration(tc.in); got != tc.want {
				t.Fatalf("have: %s, want: %s", got, tc.want)
			}
		})
	}
}

func TestAggregateSenderUsesExactIntervalHeader(t *testing.T) {
	var gotInterval string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotInterval = r.Header.Get("Aggregate-Interval")
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			t.Fatalf("reading request body: %s", err)
		}
		w.Header().Set("Location", "/stored")
		w.WriteHeader(http.StatusCreated)
	}))
	t.Cleanup(server.Close)

	file, err := os.CreateTemp(t.TempDir(), "aggregate-*.parquet")
	if err != nil {
		t.Fatalf("CreateTemp: %s", err)
	}
	if _, err := file.WriteString("payload"); err != nil {
		t.Fatalf("write temp aggregate: %s", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close temp aggregate: %s", err)
	}

	_, signingKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %s", err)
	}
	signingJWK, err := jwk.FromRaw(signingKey)
	if err != nil {
		t.Fatalf("FromRaw: %s", err)
	}
	if err := signingJWK.Set(jwk.AlgorithmKey, jwa.EdDSA); err != nil {
		t.Fatalf("set Algorithm: %s", err)
	}
	if err := signingJWK.Set(jwk.KeyIDKey, "aggregate-test-key"); err != nil {
		t.Fatalf("set KeyID: %s", err)
	}

	aggrecURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse server URL: %s", err)
	}

	edm := &dnstapMinimiser{
		log:                 slog.New(slog.NewTextHandler(io.Discard, nil)),
		httpClientCertStore: newCertStore(),
	}
	as, err := edm.newAggregateSender(aggrecURL, signingJWK, nil)
	if err != nil {
		t.Fatalf("newAggregateSender: %s", err)
	}

	start := time.Date(2026, 4, 29, 12, 34, 45, 0, time.UTC)
	if err := as.send(file.Name(), start, 45*time.Second); err != nil {
		t.Fatalf("send: %s", err)
	}

	want := "2026-04-29T12:34:45Z/PT45S"
	if gotInterval != want {
		t.Fatalf("Aggregate-Interval header\n  have: %s\n  want: %s", gotInterval, want)
	}
}

func TestAggregateSenderClosesBodyOnReadError(t *testing.T) {
	closed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			t.Logf("draining request body: %s", err)
		}

		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Error("ResponseWriter does not implement Hijacker")
			return
		}
		conn, buf, err := hj.Hijack()
		if err != nil {
			t.Errorf("Hijack: %s", err)
			return
		}
		_, _ = buf.WriteString("HTTP/1.1 201 Created\r\nContent-Length: 1000\r\nLocation: /stored\r\n\r\ntruncated")
		_ = buf.Flush()
		_ = conn.Close()
		close(closed)
	}))
	t.Cleanup(server.Close)

	file, err := os.CreateTemp(t.TempDir(), "aggregate-*.parquet")
	if err != nil {
		t.Fatalf("CreateTemp: %s", err)
	}
	if _, err := file.WriteString("payload"); err != nil {
		t.Fatalf("write temp aggregate: %s", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close temp aggregate: %s", err)
	}

	_, signingKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %s", err)
	}
	signingJWK, err := jwk.FromRaw(signingKey)
	if err != nil {
		t.Fatalf("FromRaw: %s", err)
	}
	if err := signingJWK.Set(jwk.AlgorithmKey, jwa.EdDSA); err != nil {
		t.Fatalf("set Algorithm: %s", err)
	}
	if err := signingJWK.Set(jwk.KeyIDKey, "test-key"); err != nil {
		t.Fatalf("set KeyID: %s", err)
	}

	aggrecURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse server URL: %s", err)
	}

	edm := &dnstapMinimiser{
		log:                 slog.New(slog.NewTextHandler(io.Discard, nil)),
		httpClientCertStore: newCertStore(),
	}
	as, err := edm.newAggregateSender(aggrecURL, signingJWK, nil)
	if err != nil {
		t.Fatalf("newAggregateSender: %s", err)
	}

	start := time.Date(2026, 4, 29, 12, 34, 45, 0, time.UTC)
	err = as.send(file.Name(), start, 45*time.Second)
	if err == nil {
		t.Fatal("expected error from send when response body is truncated")
	}

	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("server handler did not run")
	}
}
