package runner

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net/url"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/autopaho/queue/file"
	"github.com/eclipse/paho.golang/paho"
	paholog "github.com/eclipse/paho.golang/paho/log"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jws"
)

func (edm *dnstapMinimiser) newAutoPahoClientConfig(caCertPool *x509.CertPool, server string, clientID string, clientCertStore *certStore, mqttKeepAlive uint16, localFileQueue *file.Queue) (autopaho.ClientConfig, error) {
	u, err := url.Parse(server)
	if err != nil {
		return autopaho.ClientConfig{}, fmt.Errorf("newAutoPahoClientConfig: unable to parse URL: %w", err)
	}

	cliCfg := autopaho.ClientConfig{
		ServerUrls: []*url.URL{u},
		TlsCfg: &tls.Config{
			RootCAs:              caCertPool,
			GetClientCertificate: clientCertStore.getClientCertficate,
			MinVersion:           tls.VersionTLS13,
		},
		KeepAlive:      mqttKeepAlive,
		OnConnectionUp: func(*autopaho.ConnectionManager, *paho.Connack) { edm.log.Info("mqtt connection up") },
		OnConnectError: func(err error) { edm.log.Error("error whilst attempting connection", "error", err) },
		Debug:          paholog.NOOPLogger{},
		Errors:         log.Default(),
		PahoDebug:      paholog.NOOPLogger{},
		PahoErrors:     log.Default(),
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

	if edm.debug {
		cliCfg.Debug = log.Default()
		cliCfg.PahoDebug = log.Default()
	}

	return cliCfg, nil
}

func (edm *dnstapMinimiser) runAutoPaho(cm *autopaho.ConnectionManager, topic string, mqttJWK jwk.Key, usingFileQueue bool) {
	defer edm.autopahoWg.Done()
	for {
		// We only need to wait for a server connection if we have no
		// local queue. Otherwise we can just start appending messages
		// to disk.
		if !usingFileQueue {
			// AwaitConnection will return immediately if connection is up; adding this call stops publication whilst
			// connection is unavailable.
			err := cm.AwaitConnection(edm.autopahoCtx)
			if err != nil { // Should only happen when context is cancelled
				edm.log.Error("publisher done", "AwaitConnection", err)
				return
			}
		}

		// Wait for a message to publish
		unsignedMsg := <-edm.mqttPubCh
		if unsignedMsg == nil {
			// The channel has been closed
			edm.log.Info("runAutoPaho: message queue closed, exiting")
			return
		}

		signedMsg, err := jws.Sign(unsignedMsg, jws.WithJSON(), jws.WithKey(jwa.ES256, mqttJWK))
		if err != nil {
			edm.log.Error("runAutoPaho: failed to created JWS message", "error", err)
			continue
		}

		if usingFileQueue {
			err = cm.PublishViaQueue(context.Background(), &autopaho.QueuePublish{
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
				} else if pr != nil && pr.ReasonCode != 0 && pr.ReasonCode != 16 { // 16 = Server received message but there are no subscribers
					// pr is only non-nil for QoS 1 and up
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
