package runner

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jws"
)

// newTestMQTTJWK builds an EdDSA jwk.Key suitable for the JWS pipeline.
// The key's algorithm/key-id are populated the same way the production
// loader (edDsaJWKFromFile) does. Returned alongside the corresponding
// public key so tests can verify signed messages.
func newTestMQTTJWK(t *testing.T) (priv jwk.Key, pub ed25519.PublicKey) {
	t.Helper()

	pub, sk, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %s", err)
	}

	priv, err = jwk.FromRaw(sk)
	if err != nil {
		t.Fatalf("jwk.FromRaw: %s", err)
	}
	if err := priv.Set(jwk.AlgorithmKey, jwa.EdDSA); err != nil {
		t.Fatalf("set Algorithm: %s", err)
	}
	if err := priv.Set(jwk.KeyIDKey, "test-mqtt-key"); err != nil {
		t.Fatalf("set KeyID: %s", err)
	}
	return priv, pub
}

func TestParseMQTTServerURL(t *testing.T) {
	tests := []struct {
		name       string
		in         string
		wantScheme string
		wantHost   string
		wantErr    bool
	}{
		{
			name:       "bare host port defaults to tls",
			in:         "127.0.0.1:8883",
			wantScheme: "tls",
			wantHost:   "127.0.0.1:8883",
		},
		{
			name:       "bare IPv6 host port defaults to tls",
			in:         "[2001:db8::1]:8883",
			wantScheme: "tls",
			wantHost:   "[2001:db8::1]:8883",
		},
		{
			name:       "explicit tls is preserved",
			in:         "tls://mqtt.example:8883",
			wantScheme: "tls",
			wantHost:   "mqtt.example:8883",
		},
		{
			name:       "explicit mqtt is preserved",
			in:         "mqtt://mqtt.example:1883",
			wantScheme: "mqtt",
			wantHost:   "mqtt.example:1883",
		},
		{
			name:       "explicit tcp is preserved",
			in:         "tcp://mqtt.example:1883",
			wantScheme: "tcp",
			wantHost:   "mqtt.example:1883",
		},
		{
			name:    "unsupported scheme",
			in:      "ftp://mqtt.example:21",
			wantErr: true,
		},
		{
			name:    "missing host",
			in:      "tls://",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseMQTTServerURL(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseMQTTServerURL(%q) returned nil error", tc.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseMQTTServerURL(%q): %s", tc.in, err)
			}
			if got.Scheme != tc.wantScheme {
				t.Fatalf("scheme have: %s, want: %s", got.Scheme, tc.wantScheme)
			}
			if got.Host != tc.wantHost {
				t.Fatalf("host have: %s, want: %s", got.Host, tc.wantHost)
			}
		})
	}
}

// TestMqttSignWorkerSignsAndForwards covers the happy path: the worker
// reads an unsigned payload from mqttPubCh, JWS-signs it with the supplied
// JWK, and forwards the signed envelope on mqttSignedCh. We then verify
// the JWS using the matching public key to make sure the worker is
// actually signing the payload it received (not e.g. a constant or empty
// buffer).
//
// This pins the contract introduced in TIER2OPT.md "Change 6": signing is
// parallelisable because each worker is independent of the others and of
// paho — a future refactor that, say, mutated the payload buffer in place
// across workers would corrupt the signature, and this test would catch
// it.
func TestMqttSignWorkerSignsAndForwards(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	edm, err := newDnstapMinimiser(logger, defaultTC)
	if err != nil {
		t.Fatalf("newDnstapMinimiser: %s", err)
	}
	t.Cleanup(func() { _ = edm.Close() })

	// The worker ranges over mqttPubCh and selects on autopahoCtx.Done()
	// for cancellation. Bind a fresh context so this test does not affect
	// other tests and so we can guarantee cancellation on cleanup.
	ctx, cancel := context.WithCancel(t.Context())
	edm.autopahoCtx = ctx
	t.Cleanup(cancel)

	priv, pub := newTestMQTTJWK(t)

	var wg sync.WaitGroup
	wg.Add(1)
	go edm.mqttSignWorker(&wg, priv)

	payload := []byte(`{"qname":"example.com.","time":"2026-01-02T03:04:05Z"}`)
	edm.mqttPubCh <- payload

	select {
	case signed := <-edm.mqttSignedCh:
		// Verify the signature using the matching public key. Verify
		// returns the original payload bytes on success.
		got, err := jws.Verify(signed, jws.WithKey(jwa.EdDSA, pub))
		if err != nil {
			t.Fatalf("jws.Verify: %s", err)
		}
		if string(got) != string(payload) {
			t.Fatalf("signed payload mismatch\n  have: %s\n  want: %s", got, payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for signed message on mqttSignedCh")
	}

	// Closing the input channel must let the worker exit cleanly. If a
	// future refactor accidentally introduced an unbounded inner loop the
	// wg.Wait() below would hang and t.Cleanup-driven cancel() would not
	// rescue us — so we wait with a timeout and fail loudly instead.
	close(edm.mqttPubCh)
	waitOrFail(t, &wg, 2*time.Second, "mqttSignWorker did not exit after mqttPubCh close")
}

// TestMqttSignWorkerExitsOnContextCancelWhenSignedFull demonstrates the
// back-pressure escape hatch: if the publisher stalls and mqttSignedCh
// fills, the sign worker must not deadlock — it must return when
// autopahoCtx is cancelled. Without this, cancelling the run context
// would leave goroutines blocked on the channel send.
//
// Setup: replace mqttSignedCh with a *full* unbuffered-equivalent channel
// (capacity 1, pre-loaded) so the worker's send blocks. Then cancel the
// context and observe that the worker exits.
func TestMqttSignWorkerExitsOnContextCancelWhenSignedFull(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	edm, err := newDnstapMinimiser(logger, defaultTC)
	if err != nil {
		t.Fatalf("newDnstapMinimiser: %s", err)
	}
	t.Cleanup(func() { _ = edm.Close() })

	ctx, cancel := context.WithCancel(t.Context())
	edm.autopahoCtx = ctx

	// Replace the default 1024-deep channel with a tiny pre-filled one so
	// we can deterministically force the worker's send to block.
	edm.mqttSignedCh = make(chan []byte, 1)
	edm.mqttSignedCh <- []byte("placeholder")

	priv, _ := newTestMQTTJWK(t)

	var wg sync.WaitGroup
	wg.Add(1)
	go edm.mqttSignWorker(&wg, priv)

	// Hand the worker exactly one message; it will sign it and then block
	// trying to enqueue on the (already full) signed channel.
	edm.mqttPubCh <- []byte("payload")

	// Give the worker a moment to actually reach the blocked select.
	// 50ms is generous on any real machine; we don't poll with a
	// shorter, busier loop because we want to keep the test simple and
	// the operation we're racing against is a cheap goroutine reaching
	// a select.
	time.Sleep(50 * time.Millisecond)

	cancel()
	waitOrFail(t, &wg, 2*time.Second, "mqttSignWorker did not exit after context cancel")
}

// TestMqttSignWorkerSkipsBadKey verifies the worker tolerates jws.Sign
// failures: a misconfigured/unsigned-eligible jwk causes the sign call to
// error, which the worker logs and then continues to the next message.
// We cannot easily construct a "broken" jwk.Key from an unsigned input,
// so we instead set a clearly mismatched algorithm on a valid Ed25519 key
// — jws.Sign with WithKey(<wrong-alg>, <ed25519 key>) rejects the
// combination — and confirm: (a) the worker does not exit, (b) the next
// well-formed message after fixing the key still gets signed.
//
// Why this matters: the worker is a long-lived goroutine. If a transient
// signing error caused it to exit, every subsequent message would pile
// up unsigned in mqttPubCh (or be dropped on close) until the process
// restarted. The "continue past sign errors" behaviour was a deliberate
// design choice; this test pins it.
func TestMqttSignWorkerSkipsBadKey(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	edm, err := newDnstapMinimiser(logger, defaultTC)
	if err != nil {
		t.Fatalf("newDnstapMinimiser: %s", err)
	}
	t.Cleanup(func() { _ = edm.Close() })

	ctx, cancel := context.WithCancel(t.Context())
	edm.autopahoCtx = ctx
	t.Cleanup(cancel)

	priv, pub := newTestMQTTJWK(t)

	// Force a signing error by claiming the Ed25519 key uses RS256 — the
	// jws library will refuse to sign.
	if err := priv.Set(jwk.AlgorithmKey, jwa.RS256); err != nil {
		t.Fatalf("set Algorithm: %s", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go edm.mqttSignWorker(&wg, priv)

	// Push one "bad" message; the worker will fail to sign and continue.
	edm.mqttPubCh <- []byte("bad-payload")

	// Drain attempt: there must be no signed message. We rely on a short
	// wait because nothing else is feeding mqttSignedCh.
	select {
	case got := <-edm.mqttSignedCh:
		t.Fatalf("mqttSignedCh unexpectedly received: %q", got)
	case <-time.After(100 * time.Millisecond):
		// expected: no signed output
	}

	// Now flip the algorithm back to a valid one and push a real message.
	// A correctly configured worker continues past the prior error and
	// signs this one.
	if err := priv.Set(jwk.AlgorithmKey, jwa.EdDSA); err != nil {
		t.Fatalf("restore Algorithm: %s", err)
	}
	good := []byte(`{"ok":true}`)
	edm.mqttPubCh <- good

	select {
	case signed := <-edm.mqttSignedCh:
		got, err := jws.Verify(signed, jws.WithKey(jwa.EdDSA, pub))
		if err != nil {
			t.Fatalf("jws.Verify: %s", err)
		}
		if string(got) != string(good) {
			t.Fatalf("payload mismatch have: %s want: %s", got, good)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for signed message after recovery")
	}

	close(edm.mqttPubCh)
	waitOrFail(t, &wg, 2*time.Second, "mqttSignWorker did not exit cleanly")
}

type blockingMQTTConnectionManager struct {
	publishStarted chan []byte
	release        chan struct{}
	active         atomic.Int32
	concurrent     atomic.Bool
}

func (cm *blockingMQTTConnectionManager) AwaitConnection(context.Context) error {
	return nil
}

func (cm *blockingMQTTConnectionManager) PublishViaQueue(context.Context, *autopaho.QueuePublish) error {
	return nil
}

func (cm *blockingMQTTConnectionManager) Publish(ctx context.Context, publish *paho.Publish) (*paho.PublishResponse, error) {
	if cm.active.Add(1) > 1 {
		cm.concurrent.Store(true)
	}
	defer cm.active.Add(-1)

	select {
	case cm.publishStarted <- publish.Payload:
	default:
	}

	select {
	case <-cm.release:
	case <-ctx.Done():
	}
	return nil, nil
}

func TestMqttPublishWorkerPublishesSerially(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	edm, err := newDnstapMinimiser(logger, defaultTC)
	if err != nil {
		t.Fatalf("newDnstapMinimiser: %s", err)
	}
	t.Cleanup(func() { _ = edm.Close() })

	ctx, cancel := context.WithCancel(t.Context())
	edm.autopahoCtx = ctx
	t.Cleanup(cancel)

	edm.mqttSignedCh = make(chan []byte, 2)
	cm := &blockingMQTTConnectionManager{
		publishStarted: make(chan []byte, 2),
		release:        make(chan struct{}),
	}

	edm.autopahoWg.Add(1)
	go edm.mqttPublishWorker(cm, "events/up/test/new_qname", false)

	edm.mqttSignedCh <- []byte("first")
	select {
	case got := <-cm.publishStarted:
		if string(got) != "first" {
			t.Fatalf("first publish payload have: %s, want: first", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first publish did not start")
	}

	edm.mqttSignedCh <- []byte("second")
	select {
	case got := <-cm.publishStarted:
		t.Fatalf("second publish started before first publish completed: %s", got)
	case <-time.After(100 * time.Millisecond):
	}
	if cm.concurrent.Load() {
		t.Fatal("mqttPublishWorker called Publish concurrently")
	}

	close(cm.release)
	close(edm.mqttSignedCh)
	waitOrFail(t, &edm.autopahoWg, 2*time.Second, "mqttPublishWorker did not drain and exit")
}

// waitOrFail waits for wg with a deadline, calling t.Fatalf with the
// supplied message on timeout. Centralised so the timeout discipline is
// uniform across the MQTT worker tests.
func waitOrFail(t *testing.T, wg *sync.WaitGroup, d time.Duration, msg string) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatal(msg)
	}
}
