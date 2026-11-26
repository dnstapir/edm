package runner

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"runtime"
	"strings"
	"sync"

	queue "github.com/dnstapir/edm/pkg/mqtt-queue"
	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jws"
)

const (
	pahoLogTypeDebug      = "debug"
	pahoLogTypeErrors     = "errors"
	pahoLogTypePahoDebug  = "paho_debug"
	pahoLogTypePahoErrors = "paho_errors"
)

// pahoDebugLogger implements paho/log.Logger interface for debug-level logging
type pahoDebugLogger struct {
	logger *slog.Logger
}

func (pdl pahoDebugLogger) Println(v ...interface{}) {
	pdl.logger.Debug(fmt.Sprint(v...))
}

func (pdl pahoDebugLogger) Printf(format string, v ...interface{}) {
	pdl.logger.Debug(fmt.Sprintf(format, v...))
}

// pahoErrorLogger implements paho/log.Logger interface for error-level logging
type pahoErrorLogger struct {
	logger *slog.Logger
}

func (pel pahoErrorLogger) Println(v ...interface{}) {
	pel.logger.Error(fmt.Sprint(v...))
}

func (pel pahoErrorLogger) Printf(format string, v ...interface{}) {
	pel.logger.Error(fmt.Sprintf(format, v...))
}

func (edm *DnstapMinimiser) newAutoPahoClientConfig(caCertPool *x509.CertPool, server string, clientID string, mqttKeepAlive uint16) (autopaho.ClientConfig, error) {
	u, err := parseMQTTServerURL(server)
	if err != nil {
		return autopaho.ClientConfig{}, fmt.Errorf("newAutoPahoClientConfig: unable to parse MQTT server URL: %w", err)
	}

	cliCfg := autopaho.ClientConfig{
		ServerUrls: []*url.URL{u},
		TlsCfg: &tls.Config{
			RootCAs:              caCertPool,
			GetClientCertificate: edm.mqttClientCertStore.getClientCertificate,
			MinVersion:           tls.VersionTLS13,
		},
		Queue:          queue.NewRingQueue(8 * 1024),
		KeepAlive:      mqttKeepAlive,
		OnConnectionUp: func(*autopaho.ConnectionManager, *paho.Connack) { edm.log.Info("mqtt connection up") },
		OnConnectError: func(err error) { edm.log.Error("error whilst attempting connection", "error", err) },
		Debug:          pahoDebugLogger{logger: edm.log.With("paho_log_type", pahoLogTypeDebug)},
		Errors:         pahoErrorLogger{logger: edm.log.With("paho_log_type", pahoLogTypeErrors)},
		PahoDebug:      pahoDebugLogger{logger: edm.log.With("paho_log_type", pahoLogTypePahoDebug)},
		PahoErrors:     pahoErrorLogger{logger: edm.log.With("paho_log_type", pahoLogTypePahoErrors)},
		ClientConfig: paho.ClientConfig{
			ClientID:      clientID,
			OnClientError: func(err error) { edm.log.Error("server requested disconnect", "error", err) },
			OnServerDisconnect: func(d *paho.Disconnect) {
				if d.Properties != nil {
					edm.log.Error("server requested disconnect", "reason_string", d.Properties.ReasonString)
				} else {
					edm.log.Error("server requested disconnect", "reason_code", d.ReasonCode)
				}
			},
		},
	}

	return cliCfg, nil
}

func parseMQTTServerURL(server string) (*url.URL, error) {
	if !strings.Contains(server, "://") {
		server = "tls://" + server
	}

	u, err := url.Parse(server)
	if err != nil {
		return nil, err
	}
	if u.Hostname() == "" {
		return nil, fmt.Errorf("missing host in %q", server)
	}

	switch strings.ToLower(u.Scheme) {
	case "mqtt", "tcp", "ssl", "tls", "mqtts", "mqtt+ssl", "tcps", "ws", "wss":
	default:
		return nil, fmt.Errorf("unsupported scheme %q", u.Scheme)
	}

	return u, nil
}

// startMQTTPipeline launches N JWS sign workers and 1 paho publisher. The
// sign workers parallelize CPU-bound JWS signing across cores while the lone
// publisher preserves paho ConnectionManager's single-connection behavior.
func (edm *DnstapMinimiser) startMQTTPipeline(ctx context.Context, cm mqttConnectionManager, mqttJWK jwk.Key, signWorkers int) {
	if signWorkers <= 0 {
		signWorkers = 1
	}
	keyID, _ := mqttJWK.KeyID()
	alg, _ := mqttJWK.Algorithm()
	topic := "events/up/" + keyID + "/new_qname"

	edm.log.Info(
		"starting signing MQTT publisher",
		"jwk_id", keyID,
		"jwk_alg", alg,
		"topic", topic,
		"sign_workers", signWorkers,
	)

	// Sign workers: each independently reads unsigned bytes, JWS-signs,
	// pushes the signed bytes onto the publisher's queue. When mqttPubCh
	// is closed, each worker exits; when all are done, the last one
	// closes mqttSignedCh so the publisher knows to drain and exit.
	var signWg sync.WaitGroup
	for range signWorkers {
		signWg.Go(func() { edm.mqttSignWorker(ctx, mqttJWK) })
	}

	edm.autopahoWg.Go(func() {
		signWg.Wait()
		close(edm.mqttSignedCh)
	})

	edm.autopahoWg.Go(func() {
		edm.mqttPublishWorker(ctx, cm, topic)
	})
}

// mqttSignWorker drains mqttPubCh, JWS-signs each message, and forwards to
// mqttSignedCh. Exits when mqttPubCh is closed.
func (edm *DnstapMinimiser) mqttSignWorker(ctx context.Context, mqttJWK jwk.Key) {
	for unsignedMsg := range edm.mqttPubCh {
		// The signing algorithm is read from the key for each message.
		// A key without an algorithm cannot be used to sign, so the
		// message is logged and skipped rather than aborting the worker.
		alg, ok := mqttJWK.Algorithm()
		if !ok {
			edm.log.Error("mqttSignWorker: skipping message, JWK has no algorithm set")
			continue
		}
		signedMsg, err := jws.Sign(unsignedMsg, jws.WithJSON(), jws.WithKey(alg, mqttJWK))
		if err != nil {
			edm.log.Error("mqttSignWorker: failed to create JWS message", "error", err)
			continue
		}
		select {
		case edm.mqttSignedCh <- signedMsg:
		case <-ctx.Done():
			return
		}
	}
}

// mqttPublishWorker is the single goroutine that talks to paho. Single-writer
// matches paho's ConnectionManager expectations; signing remains parallel
// upstream while broker back-pressure is contained to this publisher.
func (edm *DnstapMinimiser) mqttPublishWorker(ctx context.Context, cm mqttConnectionManager, topic string) {
	var (
		signedMsg []byte
		ok        bool
	)
	for {
		select {
		case signedMsg, ok = <-edm.mqttSignedCh:
			if !ok {
				edm.log.Info("mqttPublishWorker: signed queue closed, exiting")
				return
			}
		case <-ctx.Done():
			edm.log.Info("mqttPublishWorker: context cancelled, exiting")
			return
		}

		err := cm.PublishViaQueue(ctx, &autopaho.QueuePublish{
			Publish: &paho.Publish{
				QoS:     0,
				Topic:   topic,
				Payload: signedMsg,
			},
		})
		if err != nil {
			edm.log.Error("error writing message to queue", "error", err)
		}

		select {
		case <-ctx.Done():
			edm.log.Info("publisher done")
			return
		default:
		}
	}
}

// setupMQTT prepares the MQTT signing/publish pipeline from the current
// config and starts it. It returns an error for any setup failure; callers
// decide how to react (Run terminates the process). This mirrors the
// error-returning style of setupHistogramSender.
func (edm *DnstapMinimiser) setupMQTT(ctx context.Context) error {
	conf := edm.getConfig()

	mqttJWK, err := edm.deps.KeyMaterialLoader.LoadEdDSAJWK(conf.MQTTSigningKeyFile)
	if err != nil {
		return fmt.Errorf("setupMQTT: unable to parse jwk from 'mqtt-signing-key-file': %w", err)
	}

	// Leaving these nil will use the OS default CA certs
	var mqttCACertPool *x509.CertPool

	if conf.MQTTCAFile != "" {
		// Setup CA cert for validating the MQTT connection
		mqttCACertPool, err = edm.deps.KeyMaterialLoader.LoadCertPool(conf.MQTTCAFile)
		if err != nil {
			return fmt.Errorf("setupMQTT: failed to create CA cert pool for '--mqtt-ca-file': %w", err)
		}
	}

	mqttKeyID, _ := mqttJWK.KeyID()
	mqttClientID := mqttKeyID + "-edm"

	edm.log.Info("creating MQTT client", "mqtt_client_id", mqttClientID)

	autopahoConfig, err := edm.newAutoPahoClientConfig(mqttCACertPool, conf.MQTTServer, mqttClientID, conf.MQTTKeepalive)
	if err != nil {
		return fmt.Errorf("setupMQTT: unable to create autopaho config: %w", err)
	}

	autopahoCm, err := edm.deps.MQTTFactory.NewConnection(ctx, autopahoConfig)
	if err != nil {
		return fmt.Errorf("setupMQTT: unable to create autopaho connection manager: %w", err)
	}

	// Connect to the broker - this will return immediately after initiating the connection process.
	signWorkers := conf.MQTTSignWorkers
	if signWorkers <= 0 {
		signWorkers = runtime.GOMAXPROCS(0)
	}
	edm.startMQTTPipeline(ctx, autopahoCm, mqttJWK, signWorkers)

	return nil
}

func (edm *DnstapMinimiser) newQnamePublisher(ctx context.Context) {
	edm.log.Info("newQnamePublisher: starting")

	for newQname := range edm.newQnamePublisherCh {
		newQnameJSON, err := json.Marshal(newQname)
		if err != nil {
			edm.log.Error("unable to create json for new_qname event", "error", err)
			continue
		}

		select {
		case edm.mqttPubCh <- newQnameJSON:
		case <-ctx.Done():
			edm.log.Info("newQnamePublisher: the MQTT connection is shutting down, stop writing")
			// No need to break out of for loop here because
			// edm.newQnamePublisherCh is already closed in Run()
		}
	}
	close(edm.mqttPubCh)
	edm.log.Info("newQnamePublisher: exiting loop")
}
