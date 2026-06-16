package runner

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
)

func TestAggregateSenderClosesBodyOnReadError(t *testing.T) {
	closed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			t.Errorf("draining request body: %s", err)
			return
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
		if _, err := buf.WriteString("HTTP/1.1 201 Created\r\nContent-Length: 1000\r\nLocation: /stored\r\n\r\ntruncated"); err != nil {
			t.Errorf("buf.WriteString: %s", err)
			return
		}
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
	signingJWK, err := jwk.Import(signingKey)
	if err != nil {
		t.Fatalf("Import: %s", err)
	}
	if err := signingJWK.Set(jwk.AlgorithmKey, jwa.EdDSA()); err != nil {
		t.Fatalf("set Algorithm: %s", err)
	}
	if err := signingJWK.Set(jwk.KeyIDKey, "test-key"); err != nil {
		t.Fatalf("set KeyID: %s", err)
	}

	aggrecURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse server URL: %s", err)
	}

	edm := &DnstapMinimiser{
		log:                 slog.New(slog.NewTextHandler(io.Discard, nil)),
		deps:                defaultDependencies(),
		httpClientCertStore: newCertStore(),
	}
	as, err := newAggregateSender(edm.log, aggrecURL, signingJWK, nil, edm.httpClientCertStore.getClientCertificate, edm.deps.FileSystem, edm.deps.Clock)
	if err != nil {
		t.Fatalf("newAggregateSender: %s", err)
	}

	start := time.Date(2026, 4, 29, 12, 34, 45, 0, time.UTC)
	err = as.Send(t.Context(), file.Name(), start, 45*time.Second)
	if err == nil {
		t.Fatal("expected error from send when response body is truncated")
	}

	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("server handler did not run")
	}
}

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
	edm := newTestDnstapMinimiser(t, defaultTC)
	fileName := writeTempFile(t, "aggregate.parquet", []byte("payload"))

	var gotInterval string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotInterval = r.Header.Get("Aggregate-Interval")
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			t.Errorf("reading request body: %s", err)
		}
		w.Header().Set("Location", "/stored")
		w.WriteHeader(http.StatusCreated)
	}))
	t.Cleanup(server.Close)

	aggrecURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse server URL: %s", err)
	}
	as, err := newAggregateSender(edm.log, aggrecURL, testJWK(t), nil, edm.httpClientCertStore.getClientCertificate, edm.deps.FileSystem, edm.deps.Clock)
	if err != nil {
		t.Fatalf("newAggregateSender: %s", err)
	}

	start := time.Date(2026, 4, 29, 12, 34, 45, 0, time.UTC)
	if err := as.Send(t.Context(), fileName, start, 45*time.Second); err != nil {
		t.Fatalf("send: %s", err)
	}

	want := "2026-04-29T12:34:45Z/PT45S"
	if gotInterval != want {
		t.Fatalf("Aggregate-Interval header\n  have: %s\n  want: %s", gotInterval, want)
	}
}

func TestAggregateSender(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	payload := []byte("parquet-ish")
	fileName := writeTempFile(t, "hist.parquet", payload)

	var sawRequest bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawRequest = true
		if r.URL.Path != "/api/v1/aggregate/histogram" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/vnd.apache.parquet" {
			t.Fatalf("content type = %q", r.Header.Get("Content-Type"))
		}
		if r.Header.Get("Aggregate-Interval") != "2026-05-28T12:34:56Z/PT2M" {
			t.Fatalf("aggregate interval = %q", r.Header.Get("Aggregate-Interval"))
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(body, payload) {
			t.Fatalf("body = %q", body)
		}
		w.Header().Set("Location", "/uploaded/hist.parquet")
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
	if err := as.Send(t.Context(), fileName, time.Date(2026, 5, 28, 12, 34, 56, 0, time.UTC), 2*time.Minute); err != nil {
		t.Fatal(err)
	}
	if !sawRequest {
		t.Fatal("server did not receive request")
	}

	if err := as.Send(t.Context(), filepath.Join(t.TempDir(), "missing.parquet"), time.Now(), time.Minute); err == nil {
		t.Fatal("sending missing file succeeded")
	}

	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	badKey, err := jwk.Import(rsaKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newAggregateSender(edm.log, u, badKey, nil, edm.httpClientCertStore.getClientCertificate, edm.deps.FileSystem, edm.deps.Clock); err == nil {
		t.Fatal("newAggregateSender accepted non-Ed25519 key")
	}
}

func TestAggregateSenderStatusAndLocationErrors(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	fileName := writeTempFile(t, "hist.parquet", []byte("data"))

	statusServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusBadRequest)
	}))
	t.Cleanup(statusServer.Close)
	statusURL, err := url.Parse(statusServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	as, err := newAggregateSender(edm.log, statusURL, testJWK(t), nil, edm.httpClientCertStore.getClientCertificate, edm.deps.FileSystem, edm.deps.Clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := as.Send(t.Context(), fileName, time.Now(), time.Minute); err == nil {
		t.Fatal("unexpected status succeeded")
	}

	locationServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", ":// bad")
		w.WriteHeader(http.StatusCreated)
	}))
	t.Cleanup(locationServer.Close)
	locationURL, err := url.Parse(locationServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	as.aggrecURL = locationURL
	if err := as.Send(t.Context(), fileName, time.Now(), time.Minute); err == nil {
		t.Fatal("bad Location succeeded")
	}
}

func TestSetupHistogramSenderAndCertLoaders(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	certPath, keyPath, caPath := testCertFiles(t)
	httpKeyPath := testJWKFile(t)
	mqttKeyPath := testJWKFile(t)

	edm.conf.HTTPURL = "https://example.test"
	edm.conf.HTTPSigningKeyFile = httpKeyPath
	edm.conf.HTTPCAFile = caPath
	edm.conf.HTTPClientCertFile = certPath
	edm.conf.HTTPClientKeyFile = keyPath
	edm.conf.MQTTClientCertFile = certPath
	edm.conf.MQTTClientKeyFile = keyPath
	if err := edm.loadHTTPClientCert(); err != nil {
		t.Fatal(err)
	}
	if err := edm.loadMQTTClientCert(); err != nil {
		t.Fatal(err)
	}
	if err := edm.setupHistogramSender(); err != nil {
		t.Fatal(err)
	}
	sender, ok := edm.aggregSender.(realAggregateSender)
	if !ok {
		t.Fatalf("aggregate sender type = %T, want aggregateSender", edm.aggregSender)
	}
	if sender.aggrecURL.String() != "https://example.test" {
		t.Fatalf("aggregate URL = %s", sender.aggrecURL)
	}

	edm.conf.HTTPURL = "://bad"
	if err := edm.setupHistogramSender(); err == nil {
		t.Fatal("bad HTTP URL succeeded")
	}
	edm.conf.HTTPURL = "https://example.test"
	edm.conf.HTTPSigningKeyFile = filepath.Join(t.TempDir(), "missing.jwk")
	if err := edm.setupHistogramSender(); err == nil {
		t.Fatal("missing HTTP signing key succeeded")
	}

	edm.conf.MQTTSigningKeyFile = mqttKeyPath
}

// TestSetupHistogramSenderClosesOldTransport verifies that reloading the
// histogram sender retains the previous aggregate sender's transport and
// closes its idle connections, so reloads do not leak keep-alive connections.
func TestSetupHistogramSenderClosesOldTransport(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)

	connStateCh := make(chan http.ConnState, 16)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "/uploaded/hist.parquet")
		w.WriteHeader(http.StatusCreated)
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		select {
		case connStateCh <- state:
		default:
		}
	}
	server.Start()
	t.Cleanup(server.Close)

	edm.conf.HTTPURL = server.URL
	edm.conf.HTTPSigningKeyFile = testJWKFile(t)

	if err := edm.setupHistogramSender(); err != nil {
		t.Fatal(err)
	}
	oldSender := edm.aggregSender
	oldConcrete, ok := oldSender.(realAggregateSender)
	if !ok {
		t.Fatalf("aggregate sender type = %T, want aggregateSender", oldSender)
	}
	if oldConcrete.httpTransport == nil {
		t.Fatal("httpTransport was not retained on the aggregate sender")
	}

	// Make a request through the old transport so it holds an idle keep-alive
	// connection that the reload is expected to close.
	fileName := writeTempFile(t, "hist.parquet", []byte("payload"))
	if err := oldSender.Send(t.Context(), fileName, time.Now(), time.Minute); err != nil {
		t.Fatal(err)
	}
	waitForConnState(t, connStateCh, http.StateIdle)

	// Reloading creates a new sender and must close the old transport's idle
	// connections without touching the live one.
	if err := edm.setupHistogramSender(); err != nil {
		t.Fatal(err)
	}
	newConcrete, ok := edm.aggregSender.(realAggregateSender)
	if !ok {
		t.Fatalf("aggregate sender type = %T, want aggregateSender", edm.aggregSender)
	}
	if newConcrete.httpTransport == oldConcrete.httpTransport {
		t.Fatal("reload did not create a new transport")
	}
	waitForConnState(t, connStateCh, http.StateClosed)
}

// waitForConnState blocks until want is observed on ch or the test deadline is
// reached, failing the test if the state never arrives.
func waitForConnState(t testing.TB, ch <-chan http.ConnState, want http.ConnState) {
	t.Helper()

	timeout := time.After(10 * time.Second)
	for {
		select {
		case state := <-ch:
			if state == want {
				return
			}
		case <-timeout:
			t.Fatalf("timed out waiting for connection state %s", want)
		}
	}
}
