package runner

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/dnstapir/edm/pkg/protocols"
	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jws"
)

// TestMqttSignWorkerSignsAndForwards covers the happy path: the worker
// reads an unsigned payload from mqttPubCh, JWS-signs it with the supplied
// JWK, and forwards the signed envelope on mqttSignedCh. We then verify
// the JWS using the matching public key to make sure the worker is
// actually signing the payload it received (not e.g. a constant or empty
// buffer).
//
// This pins the contract introduced in signing is
// parallelizable because each worker is independent of the others and of
// paho - a future refactor that, say, mutated the payload buffer in place
// across workers would corrupt the signature, and this test would catch
// it.
func TestMqttSignWorkerSignsAndForwards(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		edm := newSynctestDnstapMinimiser(t, defaultTC)

		// The worker ranges over mqttPubCh and selects on ctx.Done() for
		// cancellation. Bind a fresh context so this test does not affect other
		// tests and so we can guarantee cancellation before the bubble exits.
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		priv, pub := testJWKPair(t)

		var wg sync.WaitGroup
		wg.Add(1)
		go edm.mqttSignWorker(ctx, &wg, priv)

		payload := []byte(`{"qname":"example.com.","time":"2026-01-02T03:04:05Z"}`)
		edm.mqttPubCh <- payload

		select {
		case signed := <-edm.mqttSignedCh:
			// Verify the signature using the matching public key. Verify
			// returns the original payload bytes on success.
			got, err := jws.Verify(signed, jws.WithKey(jwa.EdDSA(), pub))
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
		// wg.Wait() below would hang and deferred cancel() would not rescue us,
		// so we wait with a timeout and fail loudly instead.
		close(edm.mqttPubCh)
		waitOrFail(t, &wg, 2*time.Second, "mqttSignWorker did not exit after mqttPubCh close")
	})
}

// TestMqttSignWorkerExitsOnContextCancelWhenSignedFull demonstrates the
// back-pressure escape hatch: if the publisher stalls and mqttSignedCh
// fills, the sign worker must not deadlock - it must return when
// ctx is cancelled. Without this, cancelling the run context
// would leave goroutines blocked on the channel send.
//
// Setup: replace mqttSignedCh with a *full* unbuffered-equivalent channel
// (capacity 1, pre-loaded) so the worker's send blocks. Then cancel the
// context and observe that the worker exits.
func TestMqttSignWorkerExitsOnContextCancelWhenSignedFull(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		edm := newSynctestDnstapMinimiser(t, defaultTC)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		// Replace the default 1024-deep channel with a tiny pre-filled one so
		// we can deterministically force the worker's send to block.
		edm.mqttSignedCh = make(chan []byte, 1)
		edm.mqttSignedCh <- []byte("placeholder")

		priv := testJWK(t)

		var wg sync.WaitGroup
		wg.Add(1)
		go edm.mqttSignWorker(ctx, &wg, priv)

		// Hand the worker exactly one message; it will sign it and then block
		// trying to enqueue on the (already full) signed channel.
		edm.mqttPubCh <- []byte("payload")

		synctest.Wait()

		cancel()
		waitOrFail(t, &wg, 2*time.Second, "mqttSignWorker did not exit after context cancel")
	})
}

// TestMqttSignWorkerSkipsBadKey verifies the worker tolerates jws.Sign
// failures: a misconfigured/unsigned-eligible jwk causes the sign call to
// error, which the worker logs and then continues to the next message.
// We cannot easily construct a "broken" jwk.Key from an unsigned input,
// so we instead set a clearly mismatched algorithm on a valid Ed25519 key
// - jws.Sign with WithKey(<wrong-alg>, <ed25519 key>) rejects the
// combination - and confirm: (a) the worker does not exit, (b) the next
// well-formed message after fixing the key still gets signed.
//
// Why this matters: the worker is a long-lived goroutine. If a transient
// signing error caused it to exit, every subsequent message would pile
// up unsigned in mqttPubCh (or be dropped on close) until the process
// restarted. The "continue past sign errors" behaviour was a deliberate
// design choice; this test pins it.
func TestMqttSignWorkerSkipsBadKey(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		edm := newSynctestDnstapMinimiser(t, defaultTC)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		priv, pub := testJWKPair(t)

		// Force a signing error by claiming the Ed25519 key uses RS256 - the
		// jws library will refuse to sign.
		if err := priv.Set(jwk.AlgorithmKey, jwa.RS256()); err != nil {
			t.Fatalf("set Algorithm: %s", err)
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go edm.mqttSignWorker(ctx, &wg, priv)

		// Push one "bad" message; the worker will fail to sign and continue.
		edm.mqttPubCh <- []byte("bad-payload")

		synctest.Wait()
		select {
		case got := <-edm.mqttSignedCh:
			t.Fatalf("mqttSignedCh unexpectedly received: %q", got)
		default:
		}

		// Now flip the algorithm back to a valid one and push a real message.
		// A correctly configured worker continues past the prior error and
		// signs this one.
		if err := priv.Set(jwk.AlgorithmKey, jwa.EdDSA()); err != nil {
			t.Fatalf("restore Algorithm: %s", err)
		}
		good := []byte(`{"ok":true}`)
		edm.mqttPubCh <- good

		select {
		case signed := <-edm.mqttSignedCh:
			got, err := jws.Verify(signed, jws.WithKey(jwa.EdDSA(), pub))
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
	})
}

// TestMqttSignWorkerSkipsKeyWithoutAlgorithm pins the no-algorithm branch:
// when the signing key reports no algorithm (Algorithm returns ok=false),
// the worker logs and skips the message instead of signing or exiting.
//
// Like the wrong-algorithm case, a skipped message must not terminate the
// long-lived worker, so the test also confirms a later well-formed message
// is signed once the algorithm is restored.
func TestMqttSignWorkerSkipsKeyWithoutAlgorithm(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		edm := newSynctestDnstapMinimiser(t, defaultTC)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		priv, pub := testJWKPair(t)

		// Drop the algorithm so Algorithm() reports ok=false and the
		// worker takes the no-algorithm skip path.
		if err := priv.Remove(jwk.AlgorithmKey); err != nil {
			t.Fatalf("remove Algorithm: %s", err)
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go edm.mqttSignWorker(ctx, &wg, priv)

		// The worker cannot sign this message and must skip it.
		edm.mqttPubCh <- []byte("no-alg-payload")

		synctest.Wait()
		select {
		case got := <-edm.mqttSignedCh:
			t.Fatalf("mqttSignedCh unexpectedly received: %q", got)
		default:
		}

		// Restoring the algorithm lets the worker sign a later message,
		// proving the skip did not terminate the goroutine.
		if err := priv.Set(jwk.AlgorithmKey, jwa.EdDSA()); err != nil {
			t.Fatalf("restore Algorithm: %s", err)
		}
		good := []byte(`{"ok":true}`)
		edm.mqttPubCh <- good

		select {
		case signed := <-edm.mqttSignedCh:
			got, err := jws.Verify(signed, jws.WithKey(jwa.EdDSA(), pub))
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
	})
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
	synctest.Test(t, func(t *testing.T) {
		edm := newSynctestDnstapMinimiser(t, defaultTC)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		edm.mqttSignedCh = make(chan []byte, 2)
		cm := &blockingMQTTConnectionManager{
			publishStarted: make(chan []byte, 2),
			release:        make(chan struct{}),
		}

		edm.autopahoWg.Add(1)
		go edm.mqttPublishWorker(ctx, cm, "events/up/test/new_qname", false)

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
		synctest.Wait()
		select {
		case got := <-cm.publishStarted:
			t.Fatalf("second publish started before first publish completed: %s", got)
		default:
		}
		if cm.concurrent.Load() {
			t.Fatal("mqttPublishWorker called Publish concurrently")
		}

		close(cm.release)
		close(edm.mqttSignedCh)
		waitOrFail(t, &edm.autopahoWg, 2*time.Second, "mqttPublishWorker did not drain and exit")
	})
}

// TestMqttPublishWorkerExitsOnContextCancel verifies that mqttPublishWorker
// exits when ctx is cancelled even if mqttSignedCh is empty. Without
// the fix, the goroutine blocks on the channel receive and can only exit when
// the channel is closed (the !ok path) or a message arrives, not on context
// cancellation.
func TestMqttPublishWorkerExitsOnContextCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		edm := newSynctestDnstapMinimiser(t, defaultTC)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		edm.mqttSignedCh = make(chan []byte)

		cm := &blockingMQTTConnectionManager{
			publishStarted: make(chan []byte, 1),
			release:        make(chan struct{}),
		}

		edm.autopahoWg.Add(1)
		go edm.mqttPublishWorker(ctx, cm, "events/up/test/new_qname", false)

		synctest.Wait()

		cancel()
		waitOrFail(t, &edm.autopahoWg, 2*time.Second, "mqttPublishWorker did not exit after context cancel")
	})
}

// TestMqttPublishWorkerLogsPublishError feeds a signed message at a fake
// connection manager whose Publish returns errInjected; the worker logs
// "error publishing" and continues to the next iteration rather than
// exiting. Closing mqttSignedCh ends the loop.
func TestMqttPublishWorkerLogsPublishError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&buf, nil))
		edm := newSynctestDnstapMinimiserWithLogger(t, defaultTC, logger)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		edm.mqttSignedCh = make(chan []byte, 1)
		conn := &fakeAutoPahoConnection{
			publishedCh: make(chan struct{}, 1),
			publishErr:  errInjected,
		}

		edm.autopahoWg.Add(1)
		go edm.mqttPublishWorker(ctx, conn, "events/up/test/new_qname", false)

		edm.mqttSignedCh <- []byte(`{"hi":"there"}`)
		select {
		case <-conn.publishedCh:
		case <-time.After(2 * time.Second):
			t.Fatal("Publish was never called")
		}
		close(edm.mqttSignedCh)
		waitOrFail(t, &edm.autopahoWg, 2*time.Second, "mqttPublishWorker did not exit")

		if !strings.Contains(buf.String(), "error publishing") {
			t.Fatalf("expected error log, got: %q", buf.String())
		}
	})
}

// TestMqttPublishWorkerLogsNonZeroReasonCode covers the QoS-1+ "reason
// code received" log: a non-nil PublishResponse with a ReasonCode that
// is neither 0 (success) nor 16 (no-subscribers, which is silenced) is
// logged at info.
func TestMqttPublishWorkerLogsNonZeroReasonCode(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&buf, nil))
		edm := newSynctestDnstapMinimiserWithLogger(t, defaultTC, logger)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		edm.mqttSignedCh = make(chan []byte, 1)
		conn := &fakeAutoPahoConnection{
			publishedCh: make(chan struct{}, 1),
			publishResp: &paho.PublishResponse{ReasonCode: 0x80},
		}

		edm.autopahoWg.Add(1)
		go edm.mqttPublishWorker(ctx, conn, "events/up/test/new_qname", false)

		edm.mqttSignedCh <- []byte(`{"hi":"there"}`)
		select {
		case <-conn.publishedCh:
		case <-time.After(2 * time.Second):
			t.Fatal("Publish was never called")
		}
		close(edm.mqttSignedCh)
		waitOrFail(t, &edm.autopahoWg, 2*time.Second, "mqttPublishWorker did not exit")

		if !strings.Contains(buf.String(), "reason code received") {
			t.Fatalf("expected reason code log, got: %q", buf.String())
		}
	})
}

// waitOrFail waits for wg with a deadline, calling t.Fatalf with the
// supplied message on timeout. Centralized so the timeout discipline is
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
		{
			name:    "missing hostname with port",
			in:      "tls://:8883",
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

func TestMQTTConfigAndPublisher(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		edm := newSynctestDnstapMinimiser(t, defaultTC)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		cfg, err := edm.newAutoPahoClientConfig(nil, "mqtts://example.test:8883", "client-id", 30, nil)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.ClientID != "client-id" || cfg.KeepAlive != 30 || cfg.TlsCfg.MinVersion != tls.VersionTLS13 {
			t.Fatalf("unexpected MQTT config: %#v", cfg)
		}
		cfg.OnConnectionUp(nil, nil)
		cfg.OnConnectError(errors.New("connect"))
		cfg.OnClientError(errors.New("client"))
		cfg.OnServerDisconnect(&paho.Disconnect{ReasonCode: 1})
		cfg.OnServerDisconnect(&paho.Disconnect{Properties: &paho.DisconnectProperties{ReasonString: "bye"}})
		if _, err := edm.newAutoPahoClientConfig(nil, "://bad", "client-id", 30, nil); err == nil {
			t.Fatal("bad MQTT URL succeeded")
		}

		jwk := testJWK(t)
		conn := &fakeAutoPahoConnection{}
		edm.startMQTTPipeline(ctx, conn, jwk, true, 1)
		edm.mqttPubCh <- []byte(`{"hello":"world"}`)
		close(edm.mqttPubCh)
		edm.autopahoWg.Wait()
		conn.mu.Lock()
		queued := len(conn.queued)
		conn.mu.Unlock()
		if queued != 1 {
			t.Fatalf("queued messages = %d, want 1", queued)
		}

		var buf bytes.Buffer
		pahoDebugLogger{logger: slog.New(slog.NewTextHandler(&buf, nil))}.Printf("hello %s", "debug")
		pahoDebugLogger{logger: slog.New(slog.NewTextHandler(&buf, nil))}.Println("hello", "debug")
		pahoErrorLogger{logger: slog.New(slog.NewTextHandler(&buf, nil))}.Printf("hello %s", "error")
		pahoErrorLogger{logger: slog.New(slog.NewTextHandler(&buf, nil))}.Println("hello", "error")
		if !strings.Contains(buf.String(), "hello") {
			t.Fatalf("logger output = %q", buf.String())
		}
	})
}

func TestMQTTPipelinePublishPath(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		edm := newSynctestDnstapMinimiser(t, defaultTC)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		jwk := testJWK(t)
		conn := &fakeAutoPahoConnection{publishedCh: make(chan struct{}, 1)}

		edm.startMQTTPipeline(ctx, conn, jwk, false, 1)
		edm.mqttPubCh <- []byte(`{"publish":"now"}`)
		select {
		case <-conn.publishedCh:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for publish")
		}
		close(edm.mqttPubCh)
		edm.autopahoWg.Wait()

		conn.mu.Lock()
		published := len(conn.published)
		conn.mu.Unlock()
		if published != 1 {
			t.Fatalf("published messages = %d, want 1", published)
		}
	})
}

func TestMQTTPublishWorkerAwaitError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		edm := newSynctestDnstapMinimiser(t, defaultTC)
		conn := &fakeAutoPahoConnection{awaitErr: context.Canceled}

		edm.autopahoWg.Add(1)
		go edm.mqttPublishWorker(t.Context(), conn, "events/up/test/new_qname", false)
		waitOrFail(t, &edm.autopahoWg, time.Second, "mqttPublishWorker did not exit after AwaitConnection error")
	})
}

type fakeAutoPahoConnection struct {
	mu          sync.Mutex
	queued      []*autopaho.QueuePublish
	published   []*paho.Publish
	awaitErr    error
	publishedCh chan struct{}
	// publishErr, if non-nil, is returned from Publish so the
	// mqttPublishWorker's error log branch can be exercised.
	publishErr error
	// publishResp, if non-nil, is returned from Publish; otherwise
	// Publish returns &paho.PublishResponse{} (ReasonCode 0).
	publishResp *paho.PublishResponse
}

func (f *fakeAutoPahoConnection) AwaitConnection(context.Context) error {
	return f.awaitErr
}

func (f *fakeAutoPahoConnection) Publish(_ context.Context, p *paho.Publish) (*paho.PublishResponse, error) {
	f.mu.Lock()
	f.published = append(f.published, p)
	f.mu.Unlock()
	if f.publishedCh != nil {
		select {
		case f.publishedCh <- struct{}{}:
		default:
		}
	}
	if f.publishErr != nil {
		return nil, f.publishErr
	}
	if f.publishResp != nil {
		return f.publishResp, nil
	}
	return &paho.PublishResponse{}, nil
}

func (f *fakeAutoPahoConnection) PublishViaQueue(_ context.Context, p *autopaho.QueuePublish) error {
	f.mu.Lock()
	f.queued = append(f.queued, p)
	f.mu.Unlock()
	if f.publishedCh != nil {
		select {
		case f.publishedCh <- struct{}{}:
		default:
		}
	}
	return nil
}

func TestNewQnamePublisher(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		edm := newSynctestDnstapMinimiser(t, defaultTC)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		edm.newQnamePublisherCh = make(chan *protocols.NewQnameJSON, 1)
		edm.mqttPubCh = make(chan []byte, 1)

		var wg sync.WaitGroup
		wg.Add(1)
		go edm.newQnamePublisher(ctx, &wg)
		event := protocols.NewQnameJSON{Type: protocols.NewQnameJSONType, Qname: "example.com.", Version: protocols.NewQnameJSONVersion}
		edm.newQnamePublisherCh <- &event
		close(edm.newQnamePublisherCh)
		wg.Wait()

		msg := <-edm.mqttPubCh
		if !strings.Contains(string(msg), "example.com.") {
			t.Fatalf("MQTT payload = %s", msg)
		}
		if _, ok := <-edm.mqttPubCh; ok {
			t.Fatal("mqttPubCh was not closed")
		}
	})
}

// TestRunMQTTPipelineOutlivesRunCtx verifies the MQTT pipeline context is
// detached from Run's context: cancelling Run must not cancel the MQTT
// pipeline before the shutdown path has drained the minimisers and closed
// newQnamePublisherCh, so buffered new_qname events can still be signed and
// queued during shutdown (the load-bearing shutdown ordering in Run).
func TestRunMQTTPipelineOutlivesRunCtx(t *testing.T) {
	tc := runCoreTC(t)
	tc.DisableMQTT = false
	certPath, keyPath, _ := testCertFiles(t)
	tc.MQTTClientCertFile = certPath
	tc.MQTTClientKeyFile = keyPath
	tc.MQTTSigningKeyFile = testJWKFile(t)
	tc.MQTTServer = "mqtts://example.test:8883"

	input := newBlockingTestDnstapInput()
	input.cancelSeen = make(chan struct{})
	input.release = make(chan struct{})

	deps := newTestDependencies()
	deps.HTTPServerRunner = httpServerRunnerFunc(func(*http.Server) error {
		return http.ErrServerClosed
	})
	listener := newTestNetListener("unix", tc.InputUnix)
	deps.ListenerFactory = testListenerFactory{
		listenerFactory: deps.ListenerFactory,
		listen: func(_, _ string) (net.Listener, error) {
			return listener, nil
		},
	}
	deps.DnstapInputFactory = testDnstapInputFactory{
		dnstapInputFactory: deps.DnstapInputFactory,
		newFromListener: func(net.Listener) dnstapInput {
			return input
		},
	}
	mqttCtxCh := make(chan context.Context, 1)
	conn := &fakeAutoPahoConnection{}
	deps.MQTTFactory = testMQTTFactory{
		mqttFactory: deps.MQTTFactory,
		newConnection: func(ctx context.Context, _ autopaho.ClientConfig) (mqttConnectionManager, error) {
			mqttCtxCh <- ctx
			return conn, nil
		},
	}
	edm := newTestDnstapMinimiserWithDependencies(t, tc, deps)

	ctx, cancel := context.WithCancel(t.Context())
	runErr := make(chan error, 1)
	go func() {
		runErr <- edm.Run(ctx)
	}()

	<-input.ready
	mqttCtx := <-mqttCtxCh
	cancel()
	<-input.cancelSeen
	// ReadInto is now blocked on input.release, holding Run at
	// dnstapInputWg.Wait() — before the shutdown path closes
	// newQnamePublisherCh and calls mqttCancel.
	if err := mqttCtx.Err(); err != nil {
		t.Fatalf("MQTT pipeline context cancelled by Run ctx cancel: %v", err)
	}
	close(input.release)
	if err := <-runErr; err != nil {
		t.Fatalf("Run err = %v, want nil", err)
	}
	if mqttCtx.Err() == nil {
		t.Fatal("MQTT pipeline context not cancelled after Run returned")
	}
}

type testMQTTFactory struct {
	mqttFactory
	newConnection func(context.Context, autopaho.ClientConfig) (mqttConnectionManager, error)
}

func (tmf testMQTTFactory) NewConnection(ctx context.Context, cfg autopaho.ClientConfig) (mqttConnectionManager, error) {
	if tmf.newConnection != nil {
		return tmf.newConnection(ctx, cfg)
	}
	return tmf.mqttFactory.NewConnection(ctx, cfg)
}

func TestSetupMQTT(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			conn := &fakeAutoPahoConnection{publishedCh: make(chan struct{}, 1)}

			edm := newSynctestDnstapMinimiser(t, defaultTC)
			edm.deps.MQTTFactory = testMQTTFactory{
				mqttFactory: edm.deps.MQTTFactory,
				newConnection: func(context.Context, autopaho.ClientConfig) (mqttConnectionManager, error) {
					return conn, nil
				},
			}
			edm.conf.DataDir = t.TempDir()
			edm.conf.MQTTSigningKeyFile = testJWKFile(t)
			edm.conf.MQTTServer = "mqtts://example.test:8883"
			edm.conf.MQTTKeepalive = 30
			edm.conf.DisableMQTTFilequeue = false
			edm.conf.MQTTSignWorkers = 0 // exercise the GOMAXPROCS default branch

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if err := edm.setupMQTT(ctx); err != nil {
				t.Fatalf("setupMQTT: %v", err)
			}
			// Drive the publish path so the fake connection manager is actually
			// exercised; otherwise the worker would exit before touching cm.
			edm.mqttPubCh <- []byte(`{"hello":"world"}`)
			select {
			case <-conn.publishedCh:
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for publish")
			}
			close(edm.mqttPubCh)
			edm.autopahoWg.Wait()
			conn.mu.Lock()
			queued := len(conn.queued)
			conn.mu.Unlock()
			if queued != 1 {
				t.Fatalf("queued messages = %d, want 1", queued)
			}
		})
	})

	t.Run("missing signing key", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		edm.conf.DataDir = t.TempDir()
		edm.conf.MQTTSigningKeyFile = filepath.Join(t.TempDir(), "missing.jwk")
		err := edm.setupMQTT(t.Context())
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("setupMQTT error = %v, want os.ErrNotExist", err)
		}
	})

	t.Run("bad CA file", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		edm.conf.DataDir = t.TempDir()
		edm.conf.MQTTSigningKeyFile = testJWKFile(t)
		edm.conf.MQTTCAFile = writeTempFile(t, "bad-ca.pem", []byte("not a pem"))
		err := edm.setupMQTT(t.Context())
		if err == nil || !strings.Contains(err.Error(), "CA cert pool") {
			t.Fatalf("setupMQTT error = %v, want CA cert pool failure", err)
		}
	})

	t.Run("queue dir creation failure", func(t *testing.T) {
		edm := newTestDnstapMinimiser(t, defaultTC)
		// Point DataDir below a regular file so MkdirAll fails with ENOTDIR
		// regardless of the uid the tests run as.
		blocker := writeTempFile(t, "blocker", []byte("x"))
		edm.conf.DataDir = filepath.Join(blocker, "datadir")
		edm.conf.MQTTSigningKeyFile = testJWKFile(t)
		edm.conf.DisableMQTTFilequeue = false
		err := edm.setupMQTT(t.Context())
		if err == nil || !strings.Contains(err.Error(), "queue dir") {
			t.Fatalf("setupMQTT error = %v, want queue dir failure", err)
		}
	})

	t.Run("connection manager failure", func(t *testing.T) {
		errConnect := errors.New("connect boom")
		edm := newTestDnstapMinimiser(t, defaultTC)
		edm.deps.MQTTFactory = testMQTTFactory{
			mqttFactory: edm.deps.MQTTFactory,
			newConnection: func(context.Context, autopaho.ClientConfig) (mqttConnectionManager, error) {
				return nil, errConnect
			},
		}
		edm.conf.DataDir = t.TempDir()
		edm.conf.MQTTSigningKeyFile = testJWKFile(t)
		edm.conf.MQTTServer = "mqtts://example.test:8883"
		edm.conf.DisableMQTTFilequeue = true
		err := edm.setupMQTT(t.Context())
		if !errors.Is(err, errConnect) {
			t.Fatalf("setupMQTT error = %v, want %v", err, errConnect)
		}
	})
}
