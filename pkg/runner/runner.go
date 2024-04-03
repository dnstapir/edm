package runner

import (
	"context"
	"crypto/ecdsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"hash"
	"io/fs"
	"log"
	"log/slog"
	"math"
	"net"
	"net/http"
	_ "net/http/pprof" // #nosec G108 -- metricsServer only listens to localhost
	"net/netip"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cockroachdb/pebble"
	dnstap "github.com/dnstap/golang-dnstap"
	"github.com/dnstapir/dtm/pkg/protocols"
	"github.com/eclipse/paho.golang/autopaho"
	"github.com/fsnotify/fsnotify"
	_ "github.com/grafana/pyroscope-go/godeltaprof/http/pprof"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/miekg/dns"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/go-hll"
	"github.com/smhanov/dawg"
	"github.com/spaolacci/murmur3"
	"github.com/spf13/viper"
	"github.com/xitongsys/parquet-go/writer"
	"github.com/yawning/cryptopan"
	"golang.org/x/crypto/argon2"
	"google.golang.org/protobuf/proto"
)

const dawgNotFound = -1

type dtmStatusBits uint64

func (dsb *dtmStatusBits) String() string {

	if *dsb >= dtmStatusMax {
		return fmt.Sprintf("unknown flags in status: %b", *dsb)
	}

	switch *dsb {
	case dtmStatusWellKnownExact:
		return "well-known-exact"
	case dtmStatusWellKnownWildcard:
		return "well-known-wildcard"
	}

	var flags []string
	for flag := dtmStatusWellKnownExact; flag < dtmStatusMax; flag <<= 1 {
		if *dsb&flag != 0 {
			flags = append(flags, flag.String())
		}
	}
	return strings.Join(flags, "|")
}

func (dsb *dtmStatusBits) set(flag dtmStatusBits) {
	*dsb = *dsb | flag
}

const (
	dtmStatusWellKnownExact    dtmStatusBits = 1 << iota // 1
	dtmStatusWellKnownWildcard                           // 2

	// Always leave max at the end to signal unused bits
	dtmStatusMax
)

// Histogram struct implementing description at https://github.com/dnstapir/datasets/blob/main/HistogramReport.fbs
type histogramData struct {
	// The time we started collecting the data contained in the histogram
	StartTime int64 `parquet:"name=start_time, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=true, logicaltype.unit=MICROS"`
	// label fields must be exported as we set them using reflection,
	// otherwise: "panic: reflect: reflect.Value.SetString using value obtained using unexported field"
	// Also store them as pointers so we can signal them being unset as
	// opposed to an empty string
	Label0        *string `parquet:"name=label0, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Label1        *string `parquet:"name=label1, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Label2        *string `parquet:"name=label2, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Label3        *string `parquet:"name=label3, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Label4        *string `parquet:"name=label4, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Label5        *string `parquet:"name=label5, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Label6        *string `parquet:"name=label6, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Label7        *string `parquet:"name=label7, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Label8        *string `parquet:"name=label8, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Label9        *string `parquet:"name=label9, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ACount        int64   `parquet:"name=a_count, type=INT64, convertedtype=UINT_64"`
	AAAACount     int64   `parquet:"name=aaaa_count, type=INT64, convertedtype=UINT_64"`
	MXCount       int64   `parquet:"name=mx_count, type=INT64, convertedtype=UINT_64"`
	NSCount       int64   `parquet:"name=ns_count, type=INT64, convertedtype=UINT_64"`
	OtherCount    int64   `parquet:"name=other_count, type=INT64, convertedtype=UINT_64"`
	NonINCount    int64   `parquet:"name=non_in_count, type=INT64, convertedtype=UINT_64"`
	OKCount       int64   `parquet:"name=ok_count, type=INT64, convertedtype=UINT_64"`
	NXCount       int64   `parquet:"name=nx_count, type=INT64, convertedtype=UINT_64"`
	FailCount     int64   `parquet:"name=fail_count, type=INT64, convertedtype=UINT_64"`
	DTMStatusBits int64   `parquet:"name=dtm_status_bits, type=INT64, convertedtype=UINT_64"`
	// The hll.HLL structs are not expected to be included in the output
	// parquet file, and thus do not need to be exported
	v4ClientHLL           hll.Hll
	v6ClientHLL           hll.Hll
	V4ClientCountHLLBytes *string `parquet:"name=v4client_count, type=BYTE_ARRAY"`
	V6ClientCountHLLBytes *string `parquet:"name=v6client_count, type=BYTE_ARRAY"`
}

type sessionData struct {
	// Would be nice to share the label0-9 fields from histogramData but
	// embedding doesnt seem to work that way:
	// https://github.com/xitongsys/parquet-go/issues/203
	Label0       *string `parquet:"name=label0, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Label1       *string `parquet:"name=label1, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Label2       *string `parquet:"name=label2, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Label3       *string `parquet:"name=label3, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Label4       *string `parquet:"name=label4, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Label5       *string `parquet:"name=label5, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Label6       *string `parquet:"name=label6, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Label7       *string `parquet:"name=label7, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Label8       *string `parquet:"name=label8, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Label9       *string `parquet:"name=label9, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ServerID     *string `parquet:"name=server_id, type=BYTE_ARRAY"`
	QueryTime    *int64  `parquet:"name=query_time, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=true, logicaltype.unit=MICROS"`
	ResponseTime *int64  `parquet:"name=response_time, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=true, logicaltype.unit=MICROS"`
	SourceIPv4   *int32  `parquet:"name=source_ipv4, type=INT32, convertedtype=UINT_32"`
	DestIPv4     *int32  `parquet:"name=dest_ipv4, type=INT32, convertedtype=UINT_32"`
	// IPv6 addresses are split up into a network and host part, for one thing go does not have native uint128 types
	SourceIPv6Network *int64  `parquet:"name=source_ipv6_network, type=INT64, convertedtype=UINT_64"`
	SourceIPv6Host    *int64  `parquet:"name=source_ipv6_host, type=INT64, convertedtype=UINT_64"`
	DestIPv6Network   *int64  `parquet:"name=dest_ipv6_network, type=INT64, convertedtype=UINT_64"`
	DestIPv6Host      *int64  `parquet:"name=dest_ipv6_host, type=INT64, convertedtype=UINT_64"`
	SourcePort        *int32  `parquet:"name=source_port, type=INT32, convertedtype=UINT_16"`
	DestPort          *int32  `parquet:"name=dest_port, type=INT32, convertedtype=UINT_16"`
	DNSProtocol       *int32  `parquet:"name=dns_protocol, type=INT32, convertedtype=UINT_8"`
	QueryMessage      *string `parquet:"name=query_message, type=BYTE_ARRAY"`
	ResponseMessage   *string `parquet:"name=response_message, type=BYTE_ARRAY"`
}

type prevSessions struct {
	sessions     []*sessionData
	rotationTime time.Time
}

func setHistogramLabels(dtm *dnstapMinimiser, labels []string, labelLimit int, hd *histogramData) *histogramData {
	// If labels is nil (the "." zone) we can depend on the zero type of
	// the label fields being nil, so nothing to do
	if labels == nil {
		return hd
	}

	reverseLabels := reverseLabelsBounded(dtm, labels, labelLimit)

	s := reflect.ValueOf(hd).Elem()

	for index := range reverseLabels {
		s.FieldByName("Label" + strconv.Itoa(index)).Set(reflect.ValueOf(&reverseLabels[index]))
	}

	return hd
}

func setSessionLabels(dtm *dnstapMinimiser, labels []string, labelLimit int, sd *sessionData) *sessionData {
	// If labels is nil (the "." zone) we can depend on the zero type of
	// the label fields being nil, so nothing to do
	if labels == nil {
		return sd
	}

	reverseLabels := reverseLabelsBounded(dtm, labels, labelLimit)

	s := reflect.ValueOf(sd).Elem()

	for index := range reverseLabels {
		s.FieldByName("Label" + strconv.Itoa(index)).Set(reflect.ValueOf(&reverseLabels[index]))
	}

	return sd
}

func reverseLabelsBounded(dtm *dnstapMinimiser, labels []string, maxLen int) []string {
	// If labels is nil (the "." zone) there is nothing to do
	if labels == nil {
		return nil
	}

	boundedReverseLabels := []string{}

	remainderElems := 0
	if len(labels) > maxLen {
		remainderElems = len(labels) - maxLen
	}

	// Append all labels except the last one
	for i := len(labels) - 1; i > remainderElems; i-- {
		if dtm.debug {
			dtm.log.Debug("reverseLabelsBounded", "label", labels[i], "index", i)
		}
		boundedReverseLabels = append(boundedReverseLabels, labels[i])
	}

	// If the labels fit inside maxLen then just append the last remaining
	// label as-is
	if len(labels) <= maxLen {
		if dtm.debug {
			dtm.log.Debug("appending final label", "label", labels[0], "index", 0)
		}
		boundedReverseLabels = append(boundedReverseLabels, labels[0])
	} else {
		// If there are more labels than maxLen we need to concatenate
		// them before appending the last element
		if remainderElems > 0 {
			if dtm.debug {
				dtm.log.Debug("building slices of remainders")
			}
			remainderLabels := []string{}
			for i := remainderElems; i >= 0; i-- {
				remainderLabels = append(remainderLabels, labels[i])
			}

			boundedReverseLabels = append(boundedReverseLabels, strings.Join(remainderLabels, "."))
		}

	}
	return boundedReverseLabels
}

func diskCleaner(dtm *dnstapMinimiser, wg *sync.WaitGroup, sentDir string) {
	// We will scan the directory each tick for sent files to remove.
	wg.Add(1)
	defer wg.Done()

	ticker := time.NewTicker(time.Second * 60)
	defer ticker.Stop()

	oneDay := time.Hour * 12

timerLoop:
	for {
		select {
		case <-ticker.C:
			dirEntries, err := os.ReadDir(sentDir)
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					// The directory has not been created yet, this is OK
					continue
				}
				dtm.log.Error("histogramSender: unable to read sent dir", "error", err)
				continue
			}
			for _, dirEntry := range dirEntries {
				if dirEntry.IsDir() {
					continue
				}
				if strings.HasPrefix(dirEntry.Name(), "dns_histogram-") && strings.HasSuffix(dirEntry.Name(), ".parquet") {
					fileInfo, err := dirEntry.Info()
					if err != nil {
						dtm.log.Error("diskCleaner: unable to get fileInfo for filename", "error", err, "filename", dirEntry.Name())
						continue
					}

					if time.Since(fileInfo.ModTime()) > oneDay {
						absPath := filepath.Join(sentDir, dirEntry.Name())
						dtm.log.Info("diskCleaner: removing file", "filename", absPath)
						err = os.Remove(absPath)
						if err != nil {
							dtm.log.Error("diskCleaner: unable to remove sent histogram file", "error", err)
						}
					}
				}
			}
		case <-dtm.stop:
			break timerLoop
		}
	}
	dtm.log.Info("exiting diskCleaner loop")
}

func setCryptopan(dtm *dnstapMinimiser, e fsnotify.Event) {

	dtm.log.Info("setCryptopan: reacting to config file update", "filename", e.Name)

	var aesKeyLen uint32 = 32
	aesKey := argon2.IDKey([]byte(viper.GetString("cryptopan-key")), []byte(viper.GetString("cryptopan-key-salt")), 1, 64*1024, 4, aesKeyLen)

	cpn, err := cryptopan.New(aesKey)
	if err != nil {
		dtm.log.Error("setCryptopan: unable to create new cryptopan instance", "error", err)
		return
	}

	dtm.mutex.Lock()
	dtm.cryptopan = cpn
	dtm.mutex.Unlock()
}

func configUpdater(viperNotifyCh chan fsnotify.Event, dtm *dnstapMinimiser) {
	// The notifications from viper are based on
	// https://github.com/fsnotify/fsnotify which means we can receive
	// multiple events for the same file when someone modifies it. E.g. an
	// editor like vim writing to the file can result in three events
	// (CREATE, WRITE, WRITE) because of how the editor juggles the file
	// during a write.
	//
	// To not let this translate to us updating settings three times when
	// one is enough we wait a short duration for more events to occur
	// before telling things to update.
	//
	// The code below is inspired by the example at:
	// https://github.com/fsnotify/fsnotify/blob/main/cmd/fsnotify/dedup.go

	// Start with creating a timer that will call the update function in the
	// future but stop it so it never runs by default.
	var e fsnotify.Event
	t := time.AfterFunc(math.MaxInt64, func() { setCryptopan(dtm, e) })
	t.Stop()

	for e = range viperNotifyCh {
		// If an event has been recevied this means we now want to
		// enable the timer so the function will be called "soon", but
		// if more events occur we will reset it again. This allows us
		// to wait until events on the file settles down before
		// actually calling the update function.
		t.Reset(100 * time.Millisecond)
	}
}

func Run() {

	// Logger used for all output
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	// This makes any calls to the standard "log" package to use slog as
	// well
	slog.SetDefault(logger)

	if viper.GetString("cryptopan-key") == "" {
		logger.Error("cryptopan setup error", "error", "missing required setting 'cryptopan-key' in config", "configfile", viper.ConfigFileUsed())
		os.Exit(1)
	}

	httpURL, err := url.Parse(viper.GetString("http-url"))
	if err != nil {
		logger.Error("unable to parse 'http-url' setting", "error", err)
		os.Exit(1)
	}

	mqttSigningKey, err := ecdsaPrivateKeyFromFile(viper.GetString("mqtt-signing-key-file"))
	if err != nil {
		logger.Error("unable to parse key material from 'mqtt-signing-key-file'", "error", err)
		os.Exit(1)
	}

	httpSigningKey, err := ecdsaPrivateKeyFromFile(viper.GetString("http-signing-key-file"))
	if err != nil {
		logger.Error("unable to parse key material from 'http-signing-key-file'", "error", err)
		os.Exit(1)
	}

	// Leaving these nil will use the OS default CA certs
	var mqttCACertPool *x509.CertPool
	var httpCACertPool *x509.CertPool

	if viper.GetString("mqtt-ca-file") != "" {
		// Setup CA cert for validating the MQTT connection
		mqttCACertPool, err = certPoolFromFile(viper.GetString("mqtt-ca-file"))
		if err != nil {
			logger.Error("failed to create CA cert pool for '--mqtt-ca-file'", "error", err)
			os.Exit(1)
		}
	}

	// Setup client cert/key for mTLS authentication
	mqttClientCert, err := tls.LoadX509KeyPair(viper.GetString("mqtt-client-cert-file"), viper.GetString("mqtt-client-key-file"))
	if err != nil {
		logger.Error("unable to load x509 mqtt client cert", "error", err)
		os.Exit(1)
	}

	if viper.GetString("http-ca-file") != "" {
		// Setup CA cert for validating the aggregate-receiver connection
		httpCACertPool, err = certPoolFromFile(viper.GetString("http-ca-file"))
		if err != nil {
			logger.Error("failed to create CA cert pool for '-http-ca-file'", "error", err)
			os.Exit(1)
		}
	}

	httpClientCert, err := tls.LoadX509KeyPair(viper.GetString("http-client-cert-file"), viper.GetString("http-client-key-file"))
	if err != nil {
		logger.Error("unable to load x509 HTTP client cert", "error", err)
		os.Exit(1)
	}

	// Create a 32 byte length secret based on the supplied -crypto-pan key,
	// this way the user can supply a -cryptopan-key of any length and
	// we still end up with the 32 byte length expected by AES.
	//
	// Using a proper password KDF (argon2) might be overkill as we are not
	// storing the resulting hash anywhere, but it only affects startup
	// time of a mostly long running tool.
	var aesKeyLen uint32 = 32
	aesKey := argon2.IDKey([]byte(viper.GetString("cryptopan-key")), []byte(viper.GetString("cryptopan-key-salt")), 1, 64*1024, 4, aesKeyLen)

	// Create an instance of the minimiser
	dtm, err := newDnstapMinimiser(logger, aesKey, viper.GetBool("debug"))
	if err != nil {
		logger.Error("unable to init dtm", "error", err)
		os.Exit(1)
	}

	viperNotifyCh := make(chan fsnotify.Event)

	go configUpdater(viperNotifyCh, dtm)

	viper.OnConfigChange(func(e fsnotify.Event) {
		viperNotifyCh <- e
	})

	pdbDir := filepath.Join(viper.GetString("data-dir"), "pebble")
	pdb, err := pebble.Open(pdbDir, &pebble.Options{})
	if err != nil {
		logger.Error("unable to open pebble database", "dir", pdbDir, "error", err)
		os.Exit(1)
	}
	defer func() {
		err = pdb.Close()
		if err != nil {
			dtm.log.Error("unable to close pebble database", "error", err)
		}
	}()

	autopahoConfig, err := newAutoPahoClientConfig(dtm, mqttCACertPool, viper.GetString("mqtt-server"), viper.GetString("mqtt-client-id"), mqttClientCert, uint16(viper.GetInt("mqtt-keepalive")))
	if err != nil {
		logger.Error("unable to create autopaho config", "error", err)
		os.Exit(1)
	}

	autopahoCtx, autopahoCancel := context.WithCancel(context.Background())

	// Connect to the broker - this will return immediately after initiating the connection process
	autopahoCm, err := autopaho.NewConnection(autopahoCtx, autopahoConfig)
	if err != nil {
		panic(err)
	}

	var autopahoWg sync.WaitGroup

	// Setup channel for reading messages to publish
	mqttPubCh := make(chan []byte, 100)

	go runAutoPaho(autopahoCtx, &autopahoWg, autopahoCm, dtm, mqttPubCh, viper.GetString("mqtt-topic"), mqttSigningKey)

	// Setup the dnstap.Input, only one at a time is supported.
	var dti *dnstap.FrameStreamSockInput
	if viper.GetString("input-unix") != "" {
		logger.Info("creating dnstap unix socket", "socket", viper.GetString("input-unix"))
		dti, err = dnstap.NewFrameStreamSockInputFromPath(viper.GetString("input-unix"))
		if err != nil {
			logger.Error("unable to create dnstap unix socket", "error", err)
			os.Exit(1)
		}
	} else if viper.GetString("input-tcp") != "" {
		logger.Info("creating plaintext dnstap TCP socket", "socket", viper.GetString("input-tcp"))
		l, err := net.Listen("tcp", viper.GetString("input-tcp"))
		if err != nil {
			logger.Error("unable to create plaintext dnstap TCP socket", "error", err)
			os.Exit(1)
		}
		dti = dnstap.NewFrameStreamSockInput(l)
	} else if viper.GetString("input-tls") != "" {
		logger.Info("creating encrypted dnstap TLS socket", "socket", viper.GetString("input-tls"))
		dnstapInputCert, err := tls.LoadX509KeyPair(viper.GetString("input-tls-cert-file"), viper.GetString("input-tls-key-file"))
		if err != nil {
			logger.Error("unable to load x509 dnstap listener cert", "error", err)
			os.Exit(1)
		}
		dnstapTLSConfig := &tls.Config{
			Certificates: []tls.Certificate{dnstapInputCert},
			MinVersion:   tls.VersionTLS13,
		}

		// Enable client mTLS (client cert auth) if a CA file was passed:
		if viper.GetString("input-tls-client-ca-file") != "" {
			logger.Info("dnstap socket requiring valid client certs", "ca-file", viper.GetString("input-tls-client-ca-file"))
			inputTLSClientCACertPool, err := certPoolFromFile(viper.GetString("input-tls-client-ca-file"))
			if err != nil {
				logger.Error("failed to create CA cert pool for '-input-tls-client-ca-file': %s", "error", err)
				os.Exit(1)
			}

			dnstapTLSConfig.ClientAuth = tls.RequireAndVerifyClientCert
			dnstapTLSConfig.ClientCAs = inputTLSClientCACertPool
		}

		l, err := tls.Listen("tcp", viper.GetString("input-tls"), dnstapTLSConfig)
		if err != nil {
			logger.Error("unable to create TCP listener", "error", err)
			os.Exit(1)
		}
		dti = dnstap.NewFrameStreamSockInput(l)
	}
	dti.SetTimeout(time.Second * 5)
	dti.SetLogger(log.Default())

	// Set default values for HLL
	err = hll.Defaults(hll.Settings{
		Log2m:             10,
		Regwidth:          4,
		ExplicitThreshold: 0,
		SparseEnabled:     true,
	})
	if err != nil {
		logger.Error("unable to set HLL defaults", "error", err)
		os.Exit(1)
	}

	// We need to keep track of domains that are not on the well-known
	// domain list yet we have seen since we started. To limit the
	// possibility of unbounded memory usage we use a LRU cache instead of
	// something simpler like a map.
	seenQnameLRU, _ := lru.New[string, struct{}](viper.GetInt("qname-seen-entries"))

	aggregSender := newAggregateSender(dtm, httpURL, viper.GetString("http-signing-key-id"), httpSigningKey, httpCACertPool, httpClientCert)

	// Exit gracefully on SIGINT or SIGTERM
	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
		<-sigs

		// We received a signal, tell runMinimiser() to stop
		close(dtm.stop)
	}()

	metricsServer := &http.Server{
		Addr:           "127.0.0.1:2112",
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   31 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		err := metricsServer.ListenAndServe()
		logger.Error("metricsServer failed", "error", err)
	}()

	// Start minimiser
	go dtm.runMinimiser(viper.GetString("well-known-domains"), viper.GetString("data-dir"), mqttPubCh, seenQnameLRU, pdb, viper.GetInt("new-qname-buffer"), aggregSender, autopahoCtx, autopahoCancel, viper.GetBool("disable-session-files"), viper.GetString("debug-dnstap-filename"))

	// Start dnstap.Input
	go dti.ReadInto(dtm.inputChannel)

	// Wait here until runMinimiser() is done
	<-dtm.done

	// Wait for graceful disconnection from MQTT bus
	dtm.log.Info("Run: waiting on MQTT disconnection")
	autopahoWg.Wait()
}

type dnstapMinimiser struct {
	inputChannel chan []byte          // the channel expected to be passed to dnstap ReadInto()
	log          *slog.Logger         // any information logging is sent here
	cryptopan    *cryptopan.Cryptopan // used for pseudonymising IP addresses
	stop         chan struct{}        // close this channel to gracefully stop runMinimiser()
	done         chan struct{}        // block on this channel to make sure output is flushed before exiting
	debug        bool                 // if we should print debug messages during operation
	mutex        sync.RWMutex         // Mutex for protecting updates to fields at runtime
}

func newDnstapMinimiser(logger *slog.Logger, cryptoPanKey []byte, debug bool) (*dnstapMinimiser, error) {
	cpn, err := cryptopan.New(cryptoPanKey)
	if err != nil {
		return nil, fmt.Errorf("newDnstapMinimiser: %w", err)
	}
	dtm := &dnstapMinimiser{}
	dtm.cryptopan = cpn
	dtm.stop = make(chan struct{})
	dtm.done = make(chan struct{})
	// Size 32 matches unexported "const outputChannelSize = 32" in
	// https://github.com/dnstap/golang-dnstap/blob/master/dnstap.go
	dtm.inputChannel = make(chan []byte, 32)
	dtm.log = logger
	dtm.debug = debug

	return dtm, nil
}

type wellKnownDomainsTracker struct {
	mutex sync.RWMutex
	wellKnownDomainsData
}

type wellKnownDomainsData struct {
	// Store a pointer to histogramCounters so we can assign to it without
	// "cannot assign to struct field in map" issues
	m             map[int]*histogramData
	rotationTime  time.Time
	dawgFinder    dawg.Finder
	murmur3Hasher hash.Hash64
}

func newWellKnownDomainsTracker(dawgFinder dawg.Finder) (*wellKnownDomainsTracker, error) {

	// We use a deterministic seed by design to be able to combine HLL
	// datasets.
	murmur3Hasher := murmur3.New64()

	return &wellKnownDomainsTracker{
		wellKnownDomainsData: wellKnownDomainsData{
			m:             map[int]*histogramData{},
			dawgFinder:    dawgFinder,
			murmur3Hasher: murmur3Hasher,
		},
	}, nil
}

// Try to find a domain name string match in DAWG data and return the index as
// well as if it was found based on a suffix string or not.
func (wkd *wellKnownDomainsTracker) dawgIndex(msg *dns.Msg) (int, bool) {
	// Try exact match first
	dawgIndex := wkd.dawgFinder.IndexOf(msg.Question[0].Name)

	if dawgIndex == dawgNotFound {
		// Next try to look up suffix matches, so for the name
		// "www.example.com." we will check for the strings
		// ".example.com." and ".com.".
		for index, end := dns.NextLabel(msg.Question[0].Name, 0); !end; index, end = dns.NextLabel(msg.Question[0].Name, index) {
			dawgIndex = wkd.dawgFinder.IndexOf(msg.Question[0].Name[index-1:])
			if dawgIndex != dawgNotFound {
				return dawgIndex, true
			}
		}
	}

	return dawgIndex, false
}

func (wkd *wellKnownDomainsTracker) isKnown(ipBytes []byte, msg *dns.Msg) bool {

	wkd.mutex.Lock()
	defer wkd.mutex.Unlock()

	dawgIndex, suffixMatch := wkd.dawgIndex(msg)

	// If this is is not a well-known domain just return as fast as
	// possible
	if dawgIndex == dawgNotFound {
		return false
	}

	if _, exists := wkd.m[dawgIndex]; !exists {
		// We leave the label0-9 fields set to nil here. Since this is in
		// the hot path of dealing with dnstap packets the less work we do the
		// better. They are filled in prior to writing out the parquet file.
		wkd.m[dawgIndex] = &histogramData{}

		dsb := new(dtmStatusBits)
		if suffixMatch {
			dsb.set(dtmStatusWellKnownWildcard)
		} else {
			dsb.set(dtmStatusWellKnownExact)
		}
		wkd.m[dawgIndex].DTMStatusBits = int64(*dsb)
	}

	// Create hash from IP address for use in HLL data
	ip, ok := netip.AddrFromSlice(ipBytes)
	if ok {
		wkd.murmur3Hasher.Write(ipBytes) // #nosec G104 -- Write() on hash.Hash never returns an error (https://pkg.go.dev/hash#Hash)
		if ip.Unmap().Is4() {
			wkd.m[dawgIndex].v4ClientHLL.AddRaw(wkd.murmur3Hasher.Sum64())
		} else {
			wkd.m[dawgIndex].v6ClientHLL.AddRaw(wkd.murmur3Hasher.Sum64())
		}
		wkd.murmur3Hasher.Reset()
	}

	// Counters based on header
	switch msg.Rcode {
	case dns.RcodeSuccess:
		wkd.m[dawgIndex].OKCount++
	case dns.RcodeNXRrset:
		wkd.m[dawgIndex].NXCount++
	case dns.RcodeServerFailure:
		wkd.m[dawgIndex].FailCount++
	}

	// Counters based on question class and type
	if msg.Question[0].Qclass == dns.ClassINET {
		switch msg.Question[0].Qtype {
		case dns.TypeA:
			wkd.m[dawgIndex].ACount++
		case dns.TypeAAAA:
			wkd.m[dawgIndex].AAAACount++
		case dns.TypeMX:
			wkd.m[dawgIndex].MXCount++
		case dns.TypeNS:
			wkd.m[dawgIndex].NSCount++
		default:
			wkd.m[dawgIndex].OtherCount++
		}
	} else {
		wkd.m[dawgIndex].NonINCount++
	}

	return true
}

func (wkd *wellKnownDomainsTracker) rotateTracker(dawgFile string, rotationTime time.Time) (*wellKnownDomainsData, error) {

	dawgFinder, err := dawg.Load(dawgFile)
	if err != nil {
		return nil, fmt.Errorf("rotateTracker: dawg.Load(): %w", err)
	}

	prevWKD := &wellKnownDomainsData{}

	// Swap the map in use so we can write parquet data outside of the write lock
	wkd.mutex.Lock()
	prevWKD.m = wkd.m
	prevWKD.dawgFinder = wkd.dawgFinder
	wkd.m = map[int]*histogramData{}
	wkd.dawgFinder = dawgFinder
	wkd.mutex.Unlock()

	prevWKD.rotationTime = rotationTime

	return prevWKD, nil
}

// Check if we have already seen this qname since we started.
func qnameSeen(dtm *dnstapMinimiser, msg *dns.Msg, seenQnameLRU *lru.Cache[string, struct{}], seenQnameLRUEvicted prometheus.Counter, pdb *pebble.DB) bool {
	// NOTE: This looks like it might be a race (calling
	// Get() followed by separate Add()) but since we want
	// to keep often looked-up names in the cache we need to
	// use Get() for updating recent-ness, and there is no
	// GetOrAdd() method available. However, it should be
	// safe for multiple threads to call Add() as this will
	// only move an already added entry to the front of the
	// eviction list which should be OK.

	_, ok := seenQnameLRU.Get(msg.Question[0].Name)
	if ok {
		// It exists in the LRU cache
		return true
	}
	// Add it to the LRU
	evicted := seenQnameLRU.Add(msg.Question[0].Name, struct{}{})
	if evicted {
		seenQnameLRUEvicted.Inc()
	}

	// It was not in the LRU cache, does it exist in pebble (on disk)?
	_, closer, err := pdb.Get([]byte(msg.Question[0].Name))
	if err == nil {
		// The value exists in pebble
		if err := closer.Close(); err != nil {
			dtm.log.Error("unable to close pebble get", "error", err)
		}
		return true
	}

	// If the key does not exist in pebble we insert it
	if errors.Is(err, pebble.ErrNotFound) {
		if err := pdb.Set([]byte(msg.Question[0].Name), []byte{}, pebble.Sync); err != nil {
			dtm.log.Error("unable to insert key in pebble", "error", err)
		}
		return false
	}

	// Some other error occured
	dtm.log.Error("unable to get key from pebble", "error", err)
	return false
}

// runMinimiser reads frames from the inputChannel, doing any modifications and
// then passes them on to a dnstap.Output. To gracefully stop
// runMinimiser() you need to close the dtm.stop channel.
func (dtm *dnstapMinimiser) runMinimiser(dawgFile string, dataDir string, mqttPubCh chan []byte, seenQnameLRU *lru.Cache[string, struct{}], pdb *pebble.DB, newQnameBuffer int, aggSender aggregateSender, autopahoCtx context.Context, autopahoCancel context.CancelFunc, disableSessionFiles bool, debugDnstapFilename string) {

	dnstapProcessed := promauto.NewCounter(prometheus.CounterOpts{
		Name: "dtm_processed_dnstap_total",
		Help: "The total number of processed dnstap packets",
	})

	newQnameQueued := promauto.NewCounter(prometheus.CounterOpts{
		Name: "dtm_new_qname_queued_total",
		Help: "The total number of queued new_qname events",
	})

	newQnameDiscarded := promauto.NewCounter(prometheus.CounterOpts{
		Name: "dtm_new_qname_discarded_total",
		Help: "The total number of discarded new_qname events",
	})

	seenQnameLRUEvicted := promauto.NewCounter(prometheus.CounterOpts{
		Name: "dtm_seen_qname_lru_evicted_total",
		Help: "The total number of times something was evicted from the new_qname LRU cache",
	})

	dt := &dnstap.Dnstap{}

	// Labels 0-9
	labelLimit := 10

	// Keep track of if we have recorded any dnstap packets in session data
	var session_updated bool

	// Setup channels for feeding writers and data senders that should do
	// their work outside the main minimiser loop. They are buffered to
	// to not block the loop if writing/sending data is slow.
	// NOTE: Remember to close all of these channels at the end of the
	// minimiser loop, otherwise the program can hang on shutdown.
	sessionWriterCh := make(chan *prevSessions, 100)
	histogramWriterCh := make(chan *wellKnownDomainsData, 100)
	newQnamePublisherCh := make(chan *protocols.EventsMqttMessageNewQnameJson, newQnameBuffer)

	var wg sync.WaitGroup

	// Write histogram file to an outbox dir where it will get picked up by
	// the histogram sender. Upon being sent it will be moved to the sent dir.
	outboxDir := filepath.Join(dataDir, "parquet", "histograms", "outbox")
	sentDir := filepath.Join(dataDir, "parquet", "histograms", "sent")

	// Start record writers and data senders in the background
	go sessionWriter(dtm, sessionWriterCh, dataDir, &wg)
	go histogramWriter(dtm, histogramWriterCh, labelLimit, outboxDir, &wg)
	go histogramSender(dtm, outboxDir, sentDir, aggSender, &wg)
	go newQnamePublisher(dtm, newQnamePublisherCh, mqttPubCh, &wg, autopahoCtx)

	go monitorChannelLen(newQnamePublisherCh)
	go diskCleaner(dtm, &wg, sentDir)

	dawgFinder, err := dawg.Load(dawgFile)
	if err != nil {
		dtm.log.Error("unable to load DAWG file", "error", err.Error())
		os.Exit(1)
	}

	wkdTracker, err := newWellKnownDomainsTracker(dawgFinder)
	if err != nil {
		dtm.log.Error(err.Error())
		os.Exit(1)
	}

	sessions := []*sessionData{}

	// Keep in mind that this file is unbuffered. We could wrap it in a
	// bufio.NewWriter() if we want more performance out of it, but since
	// it is meant for debugging purposes it is probably better to keep it
	// unbuffered and more "reactive". Otherwise it is hard to be sure if
	// you are not seeing anything in the log because packets are being
	// missed, or you are just waiting on the buffer to be flushed.
	var debugDnstapFile *os.File
	if debugDnstapFilename != "" {
		// Make gosec happy
		debugDnstapFilename := filepath.Clean(debugDnstapFilename)
		debugDnstapFile, err = os.OpenFile(debugDnstapFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			dtm.log.Error("unable to open debug dnstap file", "error", err.Error(), "filename", debugDnstapFilename)
			os.Exit(1)
		}
		defer func() {
			err := debugDnstapFile.Close()
			if err != nil {
				dtm.log.Error("unable to close debug dnstap file", "error", err, "filename", debugDnstapFile.Name())
			}
		}()
	}

	ticker := time.NewTicker(timeUntilNextMinute())
	defer ticker.Stop()

minimiserLoop:
	for {
		select {
		case frame := <-dtm.inputChannel:
			dnstapProcessed.Inc()
			if err := proto.Unmarshal(frame, dt); err != nil {
				dtm.log.Error("dnstapMinimiser.runMinimiser: proto.Unmarshal() failed, returning", "error", err)
				break minimiserLoop
			}

			isQuery := strings.HasSuffix(dnstap.Message_Type_name[int32(*dt.Message.Type)], "_QUERY")

			// For now we only care about response type dnstap packets
			if isQuery {
				continue
			}

			if debugDnstapFile != nil {
				out, ok := dnstap.JSONFormat(dt)
				if !ok {
					dtm.log.Error("unable to format dnstap debug log")
				} else {
					_, err := debugDnstapFile.Write(out)
					if err != nil {
						dtm.log.Error("unable to write to dnstap debug file", "error", err, "filename", debugDnstapFile.Name())
					}
				}
			}

			if dtm.debug {
				dtm.log.Debug("dnstapMinimiser.runMinimiser: modifying dnstap message")
			}

			// Keep around the unpseudonymised client IP for HLL
			// data, be careful with logging or otherwise handling
			// this IP as it is sensitive.
			dangerRealClientIP := make([]byte, len(dt.Message.QueryAddress))
			copy(dangerRealClientIP, dt.Message.QueryAddress)

			dtm.pseudonymiseDnstap(dt)

			msg, timestamp := parsePacket(dt, isQuery)

			// Create a less specific timestamp for data sent to
			// core to make precise tracking harder.
			truncatedTimestamp := timestamp.Truncate(time.Minute)

			// For cases where we were unable to unpack the DNS message we
			// skip parsing.
			if msg == nil || len(msg.Question) == 0 {
				dtm.log.Error("unable to parse dnstap message, or no question section, skipping parsing")
				continue
			}

			if _, ok := dns.IsDomainName(msg.Question[0].Name); !ok {
				dtm.log.Error("unable to parse question name, skipping parsing")
				continue
			}

			// We pass on the client address for cardinality
			// measurements.
			if wkdTracker.isKnown(dangerRealClientIP, msg) {
				if dtm.debug {
					dtm.log.Debug("skipping well-known domain", "domain", msg.Question[0].Name)
				}
				continue
			}

			if !qnameSeen(dtm, msg, seenQnameLRU, seenQnameLRUEvicted, pdb) {
				newQname := protocols.NewQnameEvent(msg, truncatedTimestamp)

				// If the queue is full we skip sending new_qname events on the bus
				select {
				case newQnamePublisherCh <- &newQname:
					newQnameQueued.Inc()
				default:
					// If the publisher channel is full we skip creating an event.
					newQnameDiscarded.Inc()
				}
			}

			if !disableSessionFiles {
				session := newSession(dtm, dt, msg, isQuery, labelLimit, timestamp)

				sessions = append(sessions, session)

				// Since we have appended at least one session in the
				// sessions slice at this point we have things to write
				// out.
				session_updated = true
			}
		case ts := <-ticker.C:
			// We want to tick at the start of each minute
			ticker.Reset(timeUntilNextMinute())

			if session_updated {
				ps := &prevSessions{
					sessions:     sessions,
					rotationTime: ts,
				}

				sessions = []*sessionData{}

				// We have reset the sessions slice
				session_updated = false

				sessionWriterCh <- ps
			}

			prevWKD, err := wkdTracker.rotateTracker(dawgFile, ts)
			if err != nil {
				dtm.log.Error("unable to rotate histogram map", "error", err)
				continue
			}

			// Only write out parquet file if there is something to write
			if len(prevWKD.m) > 0 {
				histogramWriterCh <- prevWKD
			}

		case <-dtm.stop:
			// Make sure writers have completed their work
			close(sessionWriterCh)
			close(histogramWriterCh)
			close(newQnamePublisherCh)

			// Stop the MQTT publisher
			dtm.log.Info("runMinimiser: stopping MQTT publisher")
			autopahoCancel()

			// Wait for all workers to exit
			dtm.log.Info("runMinimiser: waiting for workers to exit")
			wg.Wait()

			break minimiserLoop
		}
	}
	// Signal main() that we are done and ready to exit
	dtm.log.Info("runMinimiser: signaling we are done")
	close(dtm.done)
}

func monitorChannelLen(newQnamePublisherCh chan *protocols.EventsMqttMessageNewQnameJson) {
	newQnameChannelLen := promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dtm_new_qname_ch_len",
		Help: "The number of new_qname events in the channel buffer",
	})

	for {
		newQnameChannelLen.Set(float64(len(newQnamePublisherCh)))
		time.Sleep(time.Second * 1)
	}
}

func newSession(dtm *dnstapMinimiser, dt *dnstap.Dnstap, msg *dns.Msg, isQuery bool, labelLimit int, timestamp time.Time) *sessionData {
	sd := &sessionData{}

	if dt.Message.QueryPort != nil {
		qp := int32(*dt.Message.QueryPort)
		sd.SourcePort = &qp
	}

	if dt.Message.ResponsePort != nil {
		rp := int32(*dt.Message.ResponsePort)
		sd.DestPort = &rp
	}

	setSessionLabels(dtm, dns.SplitDomainName(msg.Question[0].Name), labelLimit, sd)

	if isQuery {
		qms := string(dt.Message.QueryMessage)
		sd.QueryMessage = &qms

		ms := timestamp.UnixMicro()
		sd.QueryTime = &ms
	} else {
		rms := string(dt.Message.ResponseMessage)
		sd.ResponseMessage = &rms

		ms := timestamp.UnixMicro()
		sd.ResponseTime = &ms
	}

	if len(dt.Identity) != 0 {
		sID := string(dt.Identity)
		sd.ServerID = &sID
	}

	switch *dt.Message.SocketFamily {
	case dnstap.SocketFamily_INET:
		if dt.Message.QueryAddress != nil {
			sourceIPInt, err := ipBytesToInt(dt.Message.QueryAddress)
			if err != nil {
				dtm.log.Error("unable to create uint32 from dt.Message.QueryAddress", "error", err)
			} else {
				i32SourceIPInt := int32(sourceIPInt)
				sd.SourceIPv4 = &i32SourceIPInt
			}
		}

		if dt.Message.ResponseAddress != nil {
			destIPInt, err := ipBytesToInt(dt.Message.ResponseAddress)
			if err != nil {
				dtm.log.Error("unable to create uint32 from dt.Message.ResponseAddress", "error", err)
			} else {
				i32DestIPInt := int32(destIPInt)
				sd.DestIPv4 = &i32DestIPInt
			}
		}
	case dnstap.SocketFamily_INET6:
		if dt.Message.QueryAddress != nil {
			sourceIPIntNetwork, sourceIPIntHost, err := ip6BytesToInt(dt.Message.QueryAddress)
			if err != nil {
				dtm.log.Error("unable to create uint64 variables from dt.Message.QueryAddress", "error", err)
			} else {
				i64SourceIntNetwork := int64(sourceIPIntNetwork)
				i64SourceIntHost := int64(sourceIPIntHost)
				sd.SourceIPv6Network = &i64SourceIntNetwork
				sd.SourceIPv6Host = &i64SourceIntHost
			}
		}

		if dt.Message.ResponseAddress != nil {
			dipIntNetwork, dipIntHost, err := ip6BytesToInt(dt.Message.ResponseAddress)
			if err != nil {
				dtm.log.Error("unable to create uint64 variables from dt.Message.ResponseAddress", "error", err)
			} else {
				i64dIntNetwork := int64(dipIntNetwork)
				i64dIntHost := int64(dipIntHost)
				sd.SourceIPv6Network = &i64dIntNetwork
				sd.SourceIPv6Host = &i64dIntHost
			}
		}
	default:
		dtm.log.Error("packet is neither INET or INET6")
	}

	sd.DNSProtocol = (*int32)(dt.Message.SocketProtocol)

	return sd
}

func sessionWriter(dtm *dnstapMinimiser, ch chan *prevSessions, dataDir string, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()
	for ps := range ch {
		err := writeSessionParquet(dtm, ps, dataDir)
		if err != nil {
			dtm.log.Error("sessionWriter", "error", err.Error())
		}
	}

	dtm.log.Info("sessionStructWriter: exiting loop")
}

func histogramWriter(dtm *dnstapMinimiser, ch chan *wellKnownDomainsData, labelLimit int, outboxDir string, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()
	for prevWellKnownDomainsData := range ch {
		err := writeHistogramParquet(dtm, prevWellKnownDomainsData, labelLimit, outboxDir)
		if err != nil {
			dtm.log.Error("histogramWriter", "error", err.Error())
		}

	}
	dtm.log.Info("histogramWriter: exiting loop")
}

func renameFile(dtm *dnstapMinimiser, src string, dst string) error {
	dstDir := filepath.Dir(dst)

	// We are prepared for the destination directory not existing and will
	// create it if needed and retry the rename in this case.
	for {
		err := os.Rename(src, dst)
		if err == nil {
			// Rename went well, we are done
			return nil
		}

		if errors.Is(err, fs.ErrNotExist) {
			// If the destionation directory does not exist we will
			// need to create it and then retry the Rename() in the
			// next iteration of the loop.
			err = os.MkdirAll(dstDir, 0750)
			if err != nil {
				return fmt.Errorf("renameFile: unable to create destination dir: %s: %w", dstDir, err)
			}
			dtm.log.Info("renameFile: created directory", "dir", dstDir)
		} else {
			// Some other error occured
			return fmt.Errorf("renameFile: unable to rename file, src: %s, dst: %s: %w", src, dst, err)
		}
	}
}

func createFile(dtm *dnstapMinimiser, dst string) (*os.File, error) {
	dstDir := filepath.Dir(dst)

	// Make gosec happy
	dst = filepath.Clean(dst)

	// We are prepared for the destination directory not existing and will
	// create it if needed and retry the creation in this case.
	for {
		outFile, err := os.Create(dst)
		if err == nil {
			// Creation went well, we are done
			return outFile, nil
		}

		if errors.Is(err, fs.ErrNotExist) {
			// If the destionation directory does not exist we will
			// need to create it and then retry the file Create()
			// the next iteration of the loop.
			err = os.MkdirAll(dstDir, 0750)
			if err != nil {
				return nil, fmt.Errorf("createFile: unable to create destination dir: %s: %w", dstDir, err)
			}
			dtm.log.Info("createFile: created directory", "dir", dstDir)
		} else {
			// Some other error occured
			return nil, fmt.Errorf("createFile: unable to create file, dst: %s: %w", dst, err)
		}
	}
}

func histogramSender(dtm *dnstapMinimiser, outboxDir string, sentDir string, aggSender aggregateSender, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	// We will scan the outbox directory each tick for histogram parquet
	// files to send
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

timerLoop:
	for {
		select {
		case <-ticker.C:
			dirEntries, err := os.ReadDir(outboxDir)
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					// The directory has not been created yet, this is OK
					continue
				}
				dtm.log.Error("histogramSender: unable to read outbox dir", "error", err)
				continue
			}
			for _, dirEntry := range dirEntries {
				if dirEntry.IsDir() {
					continue
				}
				if strings.HasPrefix(dirEntry.Name(), "dns_histogram-") && strings.HasSuffix(dirEntry.Name(), ".parquet") {
					startTS, stopTS, err := timestampsFromFilename(dirEntry.Name())
					if err != nil {
						dtm.log.Error("histogramSender: unable to parse timestamps from histogram filename", "error", err)
						continue
					}
					duration := stopTS.Sub(startTS)

					absPath := filepath.Join(outboxDir, dirEntry.Name())
					absPathSent := filepath.Join(sentDir, dirEntry.Name())
					err = aggSender.send(absPath, startTS, duration)
					if err != nil {
						dtm.log.Error("histogramSender: unable to send histogram file", "error", err)
					}
					err = renameFile(dtm, absPath, absPathSent)
					if err != nil {
						dtm.log.Error("histogramSender: unable to rename sent histogram file", "error", err)
					}
				}
			}
		case <-dtm.stop:
			break timerLoop
		}
	}
	dtm.log.Info("histogramSender: exiting loop")
}

func timestampsFromFilename(name string) (time.Time, time.Time, error) {
	// expected name format: dns_histogram-2023-11-29T13-50-00Z_2023-11-29T13-51-00Z.parquet
	trimmedName := strings.TrimSuffix(name, ".parquet")
	nameParts := strings.SplitN(trimmedName, "-", 2)
	times := strings.Split(nameParts[1], "_")
	startTime, err := time.Parse("2006-01-02T15-04-05Z07:00", times[0])
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("timestampFromFilename: unable to parse startTime: %w", err)
	}
	stopTime, err := time.Parse("2006-01-02T15-04-05Z07:00", times[1])
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("timestampFromFilename: unable to parse stopTime: %w", err)
	}

	return startTime, stopTime, nil
}

func newQnamePublisher(dtm *dnstapMinimiser, inputCh chan *protocols.EventsMqttMessageNewQnameJson, mqttPubCh chan []byte, wg *sync.WaitGroup, autopahoCtx context.Context) {
	wg.Add(1)
	defer wg.Done()

	for newQname := range inputCh {
		newQnameJSON, err := json.Marshal(newQname)
		if err != nil {
			dtm.log.Error("unable to create json for new_qname event", "error", err)
			continue
		}

		select {
		case mqttPubCh <- newQnameJSON:
		case <-autopahoCtx.Done():
			dtm.log.Info("newQnamePublisher: the MQTT connection is shutting down, stop writing")
			// No need to break out of for loop here because
			// inputCh is already closed in runMinimiser()
		}
	}
	close(mqttPubCh)
	dtm.log.Info("newQnamePublisher: exiting loop")
}

func parsePacket(dt *dnstap.Dnstap, isQuery bool) (*dns.Msg, time.Time) {
	var t time.Time
	var err error
	var queryAddress, responseAddress string

	qa := net.IP(dt.Message.QueryAddress)
	ra := net.IP(dt.Message.ResponseAddress)

	// Query address: 10.10.10.10:31337 or ?
	if qa != nil {
		queryAddress = qa.String() + ":" + strconv.FormatUint(uint64(*dt.Message.QueryPort), 10)
	} else {
		queryAddress = "?"
	}

	// Response address: 10.10.10.10:31337 or ?
	if ra != nil {
		responseAddress = ra.String() + ":" + strconv.FormatUint(uint64(*dt.Message.ResponsePort), 10)
	} else {
		responseAddress = "?"
	}
	msg := new(dns.Msg)
	if isQuery {
		err = msg.Unpack(dt.Message.QueryMessage)
		if err != nil {
			log.Printf("unable to unpack query message (%s -> %s): %s", queryAddress, responseAddress, err)
			msg = nil
		}
		t = time.Unix(int64(*dt.Message.QueryTimeSec), int64(*dt.Message.QueryTimeNsec))
	} else {
		err = msg.Unpack(dt.Message.ResponseMessage)
		if err != nil {
			log.Printf("unable to unpack response message (%s <- %s): %s", queryAddress, responseAddress, err)
			msg = nil
		}
		t = time.Unix(int64(*dt.Message.ResponseTimeSec), int64(*dt.Message.ResponseTimeNsec))
	}

	return msg, t
}

func ipBytesToInt(ip4Bytes []byte) (uint32, error) {
	ip, ok := netip.AddrFromSlice(ip4Bytes)
	if !ok {
		return 0, fmt.Errorf("ipBytesToInt: unable to parse bytes")
	}

	// Make sure we are dealing with 4 byte IPv4 address data (and deal with IPv4-in-IPv6 addresses)
	ip4 := ip.As4()

	ipInt := binary.BigEndian.Uint32(ip4[:])

	return ipInt, nil
}

func ip6BytesToInt(ip6Bytes []byte) (uint64, uint64, error) {
	ip, ok := netip.AddrFromSlice(ip6Bytes)
	if !ok {
		return 0, 0, fmt.Errorf("ip6BytesToInt: unable to parse bytes")
	}

	ip16 := ip.As16()

	ipIntNetwork := binary.BigEndian.Uint64(ip16[:8])
	ipIntHost := binary.BigEndian.Uint64(ip16[8:])

	return ipIntNetwork, ipIntHost, nil
}

func writeSessionParquet(dtm *dnstapMinimiser, ps *prevSessions, dataDir string) error {
	// Write session file to a sessions dir where it will be read by clickhouse
	sessionsDir := filepath.Join(dataDir, "parquet", "sessions")

	startTime := getStartTimeFromRotationTime(ps.rotationTime)

	absoluteTmpFileName, absoluteFileName := buildParquetFilenames(sessionsDir, "dns_session_block", startTime, ps.rotationTime)

	absoluteTmpFileName = filepath.Clean(absoluteTmpFileName) // Make gosec happy
	dtm.log.Info("writing out session parquet file", "filename", absoluteTmpFileName)

	outFile, err := createFile(dtm, absoluteTmpFileName)
	if err != nil {
		return fmt.Errorf("writeSessionParquet: unable to open histogram file: %w", err)
	}
	fileOpen := true
	writeFailed := false
	defer func() {
		// Closing a *os.File twice returns an error, so only do it if
		// we have not already tried to close it.
		if fileOpen {
			err := outFile.Close()
			if err != nil {
				dtm.log.Error("writeSessionParquet: unable to do deferred close of histogram outFile", "error", err)
			}
		}
		if writeFailed {
			dtm.log.Info("writeSessionParquet: cleaning up file because write failed", "filename", outFile.Name())
			err = os.Remove(outFile.Name())
			if err != nil {
				dtm.log.Error("writeSessionParquet: unable to remove histogram outFile", "error", err, "filename", outFile.Name())
			}
		}
	}()

	parquetWriter, err := writer.NewParquetWriterFromWriter(outFile, new(sessionData), 4)
	if err != nil {
		return fmt.Errorf("writeSessionParquet: unable to create parquet writer: %w", err)
	}

	for _, sessionData := range ps.sessions {
		err = parquetWriter.Write(*sessionData)
		if err != nil {
			writeFailed = true
			return fmt.Errorf("writeSessionParquet: unable to call Write() on parquet writer: %w", err)
		}
	}

	err = parquetWriter.WriteStop()
	if err != nil {
		writeFailed = true
		return fmt.Errorf("writeSessionParquet: unable to call WriteStop() on parquet writer: %w", err)
	}

	// We need to close the file before renaming it
	err = outFile.Close()
	// at this point we do not want the defer to close the file for us when returning
	fileOpen = false
	if err != nil {
		writeFailed = true
		return fmt.Errorf("writeSessionParquet: unable to call Close() on parquet writer: %w", err)
	}

	// Atomically rename the file to its real name so it can be picked up by the histogram sender
	dtm.log.Info("renaming session file", "from", absoluteTmpFileName, "to", absoluteFileName)
	err = os.Rename(absoluteTmpFileName, absoluteFileName)
	if err != nil {
		return fmt.Errorf("writeSessionParquet: unable to rename output file: %w", err)
	}

	return nil
}

func buildParquetFilenames(baseDir string, baseName string, timeStart time.Time, timeStop time.Time) (string, string) {
	// Use timestamp for files, replace ":" with "-" to not have to escape
	// characters in the shell, e.g: 2009-11-10T23-00-00Z
	startTS := timestampToFileString(timeStart.UTC())
	stopTS := timestampToFileString(timeStop.UTC())
	fileName := fmt.Sprintf("%s-%s_%s.parquet", baseName, startTS, stopTS)

	// Write output to a .tmp file so we can atomically rename it to the real
	// name when the file has been written in full
	tmpFileName := fileName + ".tmp"

	absoluteFileName := filepath.Join(baseDir, fileName)
	absoluteTmpFileName := filepath.Join(baseDir, tmpFileName)

	return absoluteTmpFileName, absoluteFileName
}

func timestampToFileString(ts time.Time) string {
	// Use timestamp for files, replace ":" with "-" to not have to escape
	// characters in the shell, e.g: 2009-11-10T23-00-00Z
	timeString := strings.ReplaceAll(ts.Format(time.RFC3339), ":", "-")

	return timeString
}

func getStartTimeFromRotationTime(rotationTime time.Time) time.Time {
	// The ticker used to interrupt minimiserLoop is hardcoded to tick at
	// the start of every minute so we can assume the duration we have
	// captured dnstap packets for is 1 minute which should be always true
	// except for the very first collection at startup based on what
	// second the program started, but in that case we just pretend we have
	// the full minute.
	return rotationTime.Add(-time.Second * 60)
}

func writeHistogramParquet(dtm *dnstapMinimiser, prevWellKnownDomainsData *wellKnownDomainsData, labelLimit int, outboxDir string) error {
	startTime := getStartTimeFromRotationTime(prevWellKnownDomainsData.rotationTime)

	absoluteTmpFileName, absoluteFileName := buildParquetFilenames(outboxDir, "dns_histogram", startTime, prevWellKnownDomainsData.rotationTime)

	dtm.log.Info("writing out histogram file", "filename", absoluteTmpFileName)

	absoluteTmpFileName = filepath.Clean(absoluteTmpFileName)
	outFile, err := createFile(dtm, absoluteTmpFileName)
	if err != nil {
		return fmt.Errorf("writeHistogramParquet: unable to open histogram file: %w", err)
	}
	fileOpen := true
	writeFailed := false
	defer func() {
		// Closing a *os.File twice returns an error, so only do it if
		// we have not already tried to close it.
		if fileOpen {
			err := outFile.Close()
			if err != nil {
				dtm.log.Error("writeHistogramParquet: unable to do deferred close of histogram outFile", "error", err)
			}
		}
		if writeFailed {
			dtm.log.Info("writeHistogramParquet: cleaning up file because write failed", "filename", outFile.Name())
			err = os.Remove(outFile.Name())
			if err != nil {
				dtm.log.Error("writeHistogramParquet: unable to remove histogram outFile", "error", err, "filename", outFile.Name())
			}
		}
	}()

	parquetWriter, err := writer.NewParquetWriterFromWriter(outFile, new(histogramData), 4)
	if err != nil {
		return fmt.Errorf("writeHistogramParquet: unable to create parquet writer: %w", err)
	}

	startTimeMicro := startTime.UnixMicro()
	for index, hGramData := range prevWellKnownDomainsData.m {
		domain, err := prevWellKnownDomainsData.dawgFinder.AtIndex(index)
		if err != nil {
			return fmt.Errorf("writeHistogramParquet: unable to find DAWG index %d: %w", index, err)
		}

		labels := dns.SplitDomainName(domain)

		// Setting the labels now when we are out of the hot path.
		setHistogramLabels(dtm, labels, labelLimit, hGramData)
		hGramData.StartTime = startTimeMicro

		// Write out the bytes from our hll data structures
		v4ClientHLLString := string(hGramData.v4ClientHLL.ToBytes())
		v6ClientHLLString := string(hGramData.v6ClientHLL.ToBytes())
		hGramData.V4ClientCountHLLBytes = &v4ClientHLLString
		hGramData.V6ClientCountHLLBytes = &v6ClientHLLString

		err = parquetWriter.Write(hGramData)
		if err != nil {
			writeFailed = true
			return fmt.Errorf("writeHistogramParquet: unable to call Write() on parquet writer: %w", err)
		}
	}

	err = parquetWriter.WriteStop()
	if err != nil {
		writeFailed = true
		return fmt.Errorf("writeHistogramParquet: unable to call WriteStop() on parquet writer: %w", err)
	}

	// We need to close the file before renaming it
	err = outFile.Close()
	// at this point we do not want the defer to close the file for us when returning
	fileOpen = false
	if err != nil {
		writeFailed = true
		return fmt.Errorf("writeHistogramParquet: unable to call Close() on parquet writer: %w", err)
	}

	// Atomically rename the file to its real name so it can be picked up by the histogram sender
	dtm.log.Info("renaming histogram file", "from", absoluteTmpFileName, "to", absoluteFileName)
	err = os.Rename(absoluteTmpFileName, absoluteFileName)
	if err != nil {
		return fmt.Errorf("writeHistogramParquet: unable to rename output file: %w", err)
	}

	return nil
}

func ecdsaPrivateKeyFromFile(fileName string) (*ecdsa.PrivateKey, error) {
	fileName = filepath.Clean(fileName)
	keyBytes, err := os.ReadFile(fileName)
	if err != nil {
		return nil, fmt.Errorf("ecdsaPrivateKeyFromFile: unable to read ECDSA private key file: %w", err)
	}

	pemBlock, _ := pem.Decode(keyBytes)
	if pemBlock == nil || pemBlock.Type != "EC PRIVATE KEY" {
		return nil, fmt.Errorf("ecdsaPrivateKeyFromFile: failed to decode PEM block containing ECDSA private key")
	}
	privateKey, err := x509.ParseECPrivateKey(pemBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("unable to parse key material from: %w", err)
	}

	return privateKey, nil
}

func certPoolFromFile(fileName string) (*x509.CertPool, error) {
	fileName = filepath.Clean(fileName)
	cert, err := os.ReadFile(fileName)
	if err != nil {
		return nil, fmt.Errorf("certPoolFromFile: unable to read file: %w", err)
	}
	certPool := x509.NewCertPool()
	ok := certPool.AppendCertsFromPEM([]byte(cert))
	if !ok {
		return nil, fmt.Errorf("certPoolFromFile: failed to append certs from pem: %w", err)
	}

	return certPool, nil
}

// Pseudonymise IP address fields in a dnstap message
func (dtm *dnstapMinimiser) pseudonymiseDnstap(dt *dnstap.Dnstap) {
	var err error

	// Lock is used here because the cryptopan instance can get updated at runtime.
	dtm.mutex.RLock()

	if dt.Message.QueryAddress != nil {
		dt.Message.QueryAddress, err = pseudonymiseIP(dtm, dt.Message.QueryAddress)
		if err != nil {
			dtm.log.Error("pseudonymiseDnstap: unable to parse dt.Message.QueryAddress", "error", err)
		}
	}
	if dt.Message.ResponseAddress != nil {
		dt.Message.ResponseAddress, err = pseudonymiseIP(dtm, dt.Message.ResponseAddress)
		if err != nil {
			dtm.log.Error("pseudonymiseDnstap: unable to parse dt.Message.ResponseAddress", "error", err)
		}
	}

	dtm.mutex.RUnlock()
}

// Pseudonymise IP address, even on error the returned []byte is usable (zeroed address)
func pseudonymiseIP(dtm *dnstapMinimiser, ipBytes []byte) ([]byte, error) {
	addr, ok := netip.AddrFromSlice(ipBytes)
	if !ok {
		// Replace address with zeroes since we do not know if
		// the contained junk is somehow sensitive
		return make([]byte, len(ipBytes)), errors.New("unable to parse addr")
	} else {
		anonymisedAddr, ok := netip.AddrFromSlice(dtm.cryptopan.Anonymize(addr.AsSlice()))
		if !ok {
			// Replace address with zeroes here as well
			// since we do not know if the contained junk
			// is somehow sensitive.
			return make([]byte, len(ipBytes)), errors.New("unable to anonymise addr")
		}
		// cryptopan.Anonymize() returns IPv4 addresses via net.IPv4(),
		// meaning we will get IPv4 addresses mapped to IPv6, e.g.
		// ::ffff:127.0.0.1. It is easier to handle these as native
		// IPv4 addresses in our system so call Unmap() on it.
		return anonymisedAddr.Unmap().AsSlice(), nil
	}
}

func timeUntilNextMinute() time.Duration {
	return time.Second * time.Duration(60-time.Now().Second())
}
