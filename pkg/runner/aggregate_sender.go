package runner

import (
	"bufio"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/yaronf/httpsign"
)

type aggregateSender struct {
	edm               *dnstapMinimiser
	aggrecURL         *url.URL
	signingKey        ed25519.PrivateKey
	caCertPool        *x509.CertPool
	signingHTTPClient *httpsign.Client
}

func (edm *dnstapMinimiser) newAggregateSender(aggrecURL *url.URL, signingJwk jwk.Key, caCertPool *x509.CertPool, clientCertStore *certStore) (aggregateSender, error) {
	var signingKey ed25519.PrivateKey

	err := signingJwk.Raw(&signingKey)
	if err != nil {
		return aggregateSender{}, fmt.Errorf("newAggregateSender: unable to create ed25519 private key from jwk: %w", err)
	}

	// Create HTTP handler for sending aggregate files to aggrec
	httpClient := http.Client{
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).Dial,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 10 * time.Second,
			TLSClientConfig: &tls.Config{
				RootCAs:              caCertPool,
				GetClientCertificate: clientCertStore.getClientCertficate,
				MinVersion:           tls.VersionTLS13,
			},
		},
	}

	edm.log.Info("creating HTTP signer", "key_id", signingJwk.KeyID(), "key_alg", signingJwk.Algorithm())

	// Create signer and wrapped HTTP client
	signer, err := httpsign.NewEd25519Signer(signingKey,
		httpsign.NewSignConfig().SetKeyID(signingJwk.KeyID()),
		httpsign.Headers("content-type", "content-length", "content-digest")) // The Content-Digest header will be auto-generated, headers selected by https://github.com/dnstapir/aggregate-receiver/blob/main/aggrec/openapi.yaml
	if err != nil {
		return aggregateSender{}, fmt.Errorf("newAggregateSender: unable to create signer: %w", err)
	}

	client := httpsign.NewClient(httpClient, httpsign.NewClientConfig().SetSignatureName("sig1").SetSigner(signer)) // sign requests, don't verify responses

	return aggregateSender{
		edm:               edm,
		aggrecURL:         aggrecURL,
		signingKey:        signingKey,
		caCertPool:        caCertPool,
		signingHTTPClient: client,
	}, nil
}

// Send histogram data via signed HTTP message to aggregate-receiver (https://github.com/dnstapir/aggregate-receiver)
func (as aggregateSender) send(fileName string, ts time.Time, duration time.Duration) error {
	fileName = filepath.Clean(fileName)
	file, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("sendAggregateFile: unable to open file: %w", err)
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("sendAggregateFile: unable to stat file: %w", err)
	}
	fileSize := fileInfo.Size()

	// Path based on https://github.com/dnstapir/aggregate-receiver/blob/main/aggrec/openapi.yaml
	histogramURL, err := url.JoinPath(as.aggrecURL.String(), "api", "v1", "aggregate", "histogram")
	if err != nil {
		return fmt.Errorf("sendAggregateFile: unable to join URL paths")
	}

	// Send signed HTTP POST message
	req, err := http.NewRequest("POST", histogramURL, bufio.NewReader(file))
	if err != nil {
		return fmt.Errorf("sendAggregateFile: unable to create request: %w", err)
	}

	// From https://datatracker.ietf.org/doc/html/draft-ietf-httpbis-digest-headers-13#section-6.3:
	// ===
	// Digests explicitly depend on the "representation metadata" (e.g.,
	// the values of Content-Type, Content-Encoding etc.). A signature that
	// protects Integrity fields but not other "representation metadata"
	// can expose the communication to tampering.
	// ===
	req.Header.Add("Content-Type", "application/vnd.apache.parquet")

	// This is set automatically by the transport, but we need to add it
	// here as well to make the signer see it, otherwise it errors out:
	// ===
	// failed to sign request: header content-length not found
	// ===
	req.Header.Add("Content-Length", strconv.FormatInt(fileSize, 10))

	// Beacuse we are using a bufio.Reader we need to set the length
	// here as well, otherwise net/http will set the header
	// "Transfer-Encoding: chunked" and remove the Content-Length header.
	req.ContentLength = fileSize

	// Expected by aggrec, e.g:
	// Aggregate-Interval: 2023-11-16T09:24:13.487591+01:00/PT1M
	minutesFloat := duration.Minutes()
	minutes := int(math.Round(minutesFloat))
	req.Header.Add("Aggregate-Interval", fmt.Sprintf("%s/PT%dM", ts.Truncate(time.Minute).Format(time.RFC3339), minutes))

	as.edm.log.Info("aggregateSender.send", "filename", fileName, "url", histogramURL)
	startTime := time.Now()
	res, err := as.signingHTTPClient.Do(req)
	elapsedTime := time.Since(startTime)
	if err != nil {
		return fmt.Errorf("sendAggregateFile: unable to send request, elapsed time %s: %w", elapsedTime, err)
	}

	bodyData, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("sendAggregateFile: unable to read response body: %w", err)
	}

	err = res.Body.Close()
	if err != nil {
		return fmt.Errorf("sendAggregateFile: unable to close HTTP body: %w", err)
	}

	if res.StatusCode != http.StatusCreated {
		as.edm.log.Error(string(bodyData))
		return fmt.Errorf("sendAggregateFile: unexpected status code: %d", res.StatusCode)
	}

	locationURL, err := url.Parse(res.Header.Get("Location"))
	if err != nil {
		return fmt.Errorf("sendAggregateFile: unable to parse Location header (file was still uploaded, took %s): %w", elapsedTime, err)
	}

	// Make it so we log a reachable link if the content in Location header is relative
	if locationURL.Scheme == "" {
		locationURL.Scheme = as.aggrecURL.Scheme
	}
	if locationURL.Host == "" {
		locationURL.Host = as.aggrecURL.Host
	}

	as.edm.log.Info("aggregateSender.send: file uploaded", "elapsed", elapsedTime.String(), "url", locationURL.String())

	return nil
}
