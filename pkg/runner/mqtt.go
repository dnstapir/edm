package runner

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net/url"
	"sync"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/autopaho/queue/file"
	"github.com/eclipse/paho.golang/paho"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jws"
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

func (edm *dnstapMinimiser) newAutoPahoClientConfig(caCertPool *x509.CertPool, server string, clientID string, mqttKeepAlive uint16, localFileQueue *file.Queue) (autopaho.ClientConfig, error) {
	u, err := url.Parse(server)
	if err != nil {
		return autopaho.ClientConfig{}, fmt.Errorf("newAutoPahoClientConfig: unable to parse URL: %w", err)
	}

	cliCfg := autopaho.ClientConfig{
		ServerUrls: []*url.URL{u},
		TlsCfg: &tls.Config{
			RootCAs:              caCertPool,
			GetClientCertificate: edm.mqttClientCertStore.getClientCertificate,
			MinVersion:           tls.VersionTLS13,
		},
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

	if localFileQueue != nil {
		edm.log.Info("using file based queue for MQTT messages")
		cliCfg.Queue = localFileQueue
	}

	return cliCfg, nil
}

// startMQTTPipeline launches N JWS sign workers and 1 paho publisher. The
// previous design ran sign + publish in a single goroutine, which made
// jws.Sign a serialisation bottleneck (~14 % of total CPU on a 20-core
// box). Splitting them lets sign work parallelise across cores while
// the paho ConnectionManager's single-connection requirement is preserved
// by the lone publisher. See TIER2OPT.md.
func (edm *dnstapMinimiser) startMQTTPipeline(cm *autopaho.ConnectionManager, mqttJWK jwk.Key, usingFileQueue bool, signWorkers int) {
	if signWorkers <= 0 {
		signWorkers = 1
	}
	topic := "events/up/" + mqttJWK.KeyID() + "/new_qname"

	edm.log.Info("starting signing MQTT publisher",
		"jwk_id", mqttJWK.KeyID(),
		"jwk_alg", mqttJWK.Algorithm(),
		"topic", topic,
		"sign_workers", signWorkers,
	)

	// Sign workers: each independently reads unsigned bytes, JWS-signs,
	// pushes the signed bytes onto the publisher's queue. When mqttPubCh
	// is closed, each worker exits; when all are done, the last one
	// closes mqttSignedCh so the publisher knows to drain and exit.
	var signWg sync.WaitGroup
	signWg.Add(signWorkers)
	for i := 0; i < signWorkers; i++ {
		go edm.mqttSignWorker(&signWg, mqttJWK)
	}

	edm.autopahoWg.Add(1)
	go func() {
		signWg.Wait()
		close(edm.mqttSignedCh)
	}()

	edm.autopahoWg.Add(1)
	go edm.mqttPublishWorker(cm, topic, usingFileQueue)
}

// mqttSignWorker drains mqttPubCh, JWS-signs each message, and forwards to
// mqttSignedCh. Exits when mqttPubCh is closed.
func (edm *dnstapMinimiser) mqttSignWorker(wg *sync.WaitGroup, mqttJWK jwk.Key) {
	defer wg.Done()
	for unsignedMsg := range edm.mqttPubCh {
		signedMsg, err := jws.Sign(unsignedMsg, jws.WithJSON(), jws.WithKey(mqttJWK.Algorithm(), mqttJWK))
		if err != nil {
			edm.log.Error("mqttSignWorker: failed to create JWS message", "error", err)
			continue
		}
		select {
		case edm.mqttSignedCh <- signedMsg:
		case <-edm.autopahoCtx.Done():
			return
		}
	}
}

// mqttPublishWorker is the single goroutine that talks to paho. Single-writer
// matches paho's ConnectionManager expectations and avoids head-of-line
// blocking on the signed-message queue while we sign.
func (edm *dnstapMinimiser) mqttPublishWorker(cm *autopaho.ConnectionManager, topic string, usingFileQueue bool) {
	defer edm.autopahoWg.Done()

	for {
		// We only need to wait for a server connection if we have no
		// local queue. Otherwise we can just start appending messages
		// to disk.
		if !usingFileQueue {
			err := cm.AwaitConnection(edm.autopahoCtx)
			if err != nil { // Should only happen when context is cancelled
				edm.log.Error("publisher done", "AwaitConnection", err)
				return
			}
		}

		signedMsg, ok := <-edm.mqttSignedCh
		if !ok {
			edm.log.Info("mqttPublishWorker: signed queue closed, exiting")
			return
		}

		if usingFileQueue {
			err := cm.PublishViaQueue(edm.autopahoCtx, &autopaho.QueuePublish{
				Publish: &paho.Publish{
					QoS:     0,
					Topic:   topic,
					Payload: signedMsg,
				},
			})
			if err != nil {
				edm.log.Error("error writing message to queue", "error", err)
			}
		} else {
			// Publish will block so we run it in a goroutine
			go func(msg []byte) {
				pr, err := cm.Publish(edm.autopahoCtx, &paho.Publish{
					QoS:     0,
					Topic:   topic,
					Payload: msg,
				})
				if err != nil {
					edm.log.Error("error publishing", "error", err)
				} else if pr != nil && pr.ReasonCode != 0 && pr.ReasonCode != 16 {
					// pr is only non-nil for QoS 1 and up;
					// 16 = "no subscribers" which is fine.
					edm.log.Info("reason code received", "reason_code", pr.ReasonCode)
				}
				if edm.debug {
					edm.log.Info("sent message", "content", string(msg))
				}
			}(signedMsg)
		}

		select {
		case <-edm.autopahoCtx.Done():
			edm.log.Info("publisher done")
			return
		default:
		}
	}
}
