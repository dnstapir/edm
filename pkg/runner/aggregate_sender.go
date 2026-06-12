package runner

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/yaronf/httpsign"
)

type realAggregateSender struct {
	log               *slog.Logger
	aggrecURL         *url.URL
	caCertPool        *x509.CertPool
	signingHTTPClient *httpsign.Client
	httpTransport     *http.Transport
	fs                fileSystem
	clock             clock
}

func newAggregateSender(log *slog.Logger, aggrecURL *url.URL, signingJwk jwk.Key, caCertPool *x509.CertPool, getClientCertificate func(*tls.CertificateRequestInfo) (*tls.Certificate, error), fs fileSystem, clock clock) (realAggregateSender, error) {
	var signingKey ed25519.PrivateKey

	err := jwk.Export(signingJwk, &signingKey)
	if err != nil {
		return realAggregateSender{}, fmt.Errorf("newAggregateSender: unable to create ed25519 private key from jwk: %w", err)
	}

	// Create HTTP handler for sending aggregate files to aggrec
	httpTransport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		TLSClientConfig: &tls.Config{
			RootCAs:              caCertPool,
			GetClientCertificate: getClientCertificate,
			MinVersion:           tls.VersionTLS13,
		},
	}
	httpClient := http.Client{
		Transport: httpTransport,
	}

	keyID, _ := signingJwk.KeyID()
	keyAlg, _ := signingJwk.Algorithm()
	log.Info("creating HTTP signer", "key_id", keyID, "key_alg", keyAlg)

	// Create signer and wrapped HTTP client
	signer, err := httpsign.NewEd25519Signer(signingKey,
		httpsign.NewSignConfig().SetKeyID(keyID),
		httpsign.Headers("content-type", "content-length", "content-digest")) // The Content-Digest header will be auto-generated, headers selected by https://github.com/dnstapir/aggregate-receiver/blob/main/aggrec/openapi.yaml
	if err != nil {
		return realAggregateSender{}, fmt.Errorf("newAggregateSender: unable to create signer: %w", err)
	}

	client := httpsign.NewClient(httpClient, httpsign.NewClientConfig().SetSignatureName("sig1").SetSigner(signer)) // sign requests, don't verify responses

	return realAggregateSender{
		log:               log,
		aggrecURL:         aggrecURL,
		caCertPool:        caCertPool,
		signingHTTPClient: client,
		httpTransport:     httpTransport,
		fs:                fs,
		clock:             clock,
	}, nil
}

// Send sends histogram data via signed HTTP message to aggregate-receiver.
func (as realAggregateSender) Send(ctx context.Context, fileName string, ts time.Time, duration time.Duration) error {
	fs := as.fs
	if fs == nil {
		fs = osFileSystem{}
	}
	clock := as.clock
	if clock == nil {
		clock = realClock{}
	}

	fileName = filepath.Clean(fileName)
	file, err := fs.Open(fileName)
	if err != nil {
		return fmt.Errorf("sendAggregateFile: unable to open file: %w", err)
	}
	defer func() {
		if cerr := file.Close(); cerr != nil {
			as.log.Error("sendAggregateFile: close file failed", "filename", fileName, "error", cerr)
		}
	}()

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("sendAggregateFile: unable to stat file: %w", err)
	}
	fileSize := fileInfo.Size()

	// Path based on https://github.com/dnstapir/aggregate-receiver/blob/main/aggrec/openapi.yaml
	histogramURL, err := url.JoinPath(as.aggrecURL.String(), "api", "v1", "aggregate", "histogram")
	if err != nil {
		return fmt.Errorf("sendAggregateFile: unable to join URL paths: %w", err)
	}

	// Send signed HTTP POST message
	req, err := http.NewRequestWithContext(ctx, "POST", histogramURL, bufio.NewReader(file))
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
	// Aggregate-Interval: 2023-11-16T09:24:13+01:00/PT45S
	req.Header.Add("Aggregate-Interval", fmt.Sprintf("%s/%s", ts.Format(time.RFC3339), iso8601Duration(duration)))

	as.log.Info("aggregateSender.send", "filename", fileName, "url", histogramURL)
	startTime := clock.Now()
	res, err := as.signingHTTPClient.Do(req)
	elapsedTime := clock.Now().Sub(startTime)
	if err != nil {
		return fmt.Errorf("sendAggregateFile: unable to send request, elapsed time %s: %w", elapsedTime, err)
	}
	defer res.Body.Close()

	bodyData, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("sendAggregateFile: unable to read response body: %w", err)
	}

	if res.StatusCode != http.StatusCreated {
		as.log.Error(string(bodyData))
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

	as.log.Info("aggregateSender.send: file uploaded", "elapsed", elapsedTime.String(), "url", locationURL.String())

	return nil
}

// CloseIdleConnections closes idle HTTP connections held by the sender.
func (as realAggregateSender) CloseIdleConnections() {
	if as.httpTransport != nil {
		as.httpTransport.CloseIdleConnections()
	}
}

// iso8601Duration formats duration as an ISO 8601 duration string, e.g. "PT1H2M3S".
//
// Sub-second precision is preserved as a fractional seconds component.
// Non-positive durations yield "PT0S".
func iso8601Duration(duration time.Duration) string {
	if duration <= 0 {
		return "PT0S"
	}

	total := int64(duration / time.Second)
	nanoseconds := duration % time.Second

	hours := total / 3600
	total %= 3600
	minutes := total / 60
	seconds := total % 60

	res := "PT"
	if hours > 0 {
		res += strconv.FormatInt(hours, 10) + "H"
	}
	if minutes > 0 {
		res += strconv.FormatInt(minutes, 10) + "M"
	}
	if seconds > 0 || nanoseconds > 0 || res == "PT" {
		if nanoseconds == 0 {
			res += strconv.FormatInt(seconds, 10)
		} else {
			secondsFloat := float64(seconds) + float64(nanoseconds)/float64(time.Second)
			res += strconv.FormatFloat(secondsFloat, 'f', -1, 64)
		}
		res += "S"
	}

	return res
}

func (edm *DnstapMinimiser) setupHistogramSender() error {
	conf := edm.getConfig()

	httpURL, err := url.Parse(conf.HTTPURL)
	if err != nil {
		return fmt.Errorf("setupHistogramSender: unable to parse 'http-url' setting: %w", err)
	}

	httpSigningJwk, err := edm.deps.KeyMaterialLoader.LoadEdDSAJWK(conf.HTTPSigningKeyFile)
	if err != nil {
		return fmt.Errorf("setupHistogramSender: unable to parse jwk from 'http-signing-key-file': %w", err)
	}

	// Leaving these nil will use the OS default CA certs
	var httpCACertPool *x509.CertPool

	if conf.HTTPCAFile != "" {
		// Setup CA cert for validating the aggregate-receiver connection
		httpCACertPool, err = edm.deps.KeyMaterialLoader.LoadCertPool(conf.HTTPCAFile)
		if err != nil {
			return fmt.Errorf("setupHistogramSender: failed to create CA cert pool for '-http-ca-file': %w", err)
		}
	}

	// Build the new sender first so a failed rebuild leaves the existing
	// working sender in place instead of zeroing it.
	newAggregSender, err := edm.deps.AggregateSenderFactory.NewAggregateSender(edm.log, httpURL, httpSigningJwk, httpCACertPool, edm.httpClientCertStore.getClientCertificate, edm.deps.FileSystem, edm.deps.Clock)
	if err != nil {
		return fmt.Errorf("setupHistogramSender: unable to create aggregate sender: %w", err)
	}

	edm.aggregSenderMutex.Lock()
	oldAggregSender := edm.aggregSender
	edm.aggregSender = newAggregSender
	edm.aggregSenderMutex.Unlock()

	if oldAggregSender != nil {
		oldAggregSender.CloseIdleConnections()
	}

	return nil
}
