package runner

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"log/slog"
	"math"
	"net"
	"net/http"
	_ "net/http/pprof" // #nosec G108 -- pprofServer only listens to localhost
	"net/netip"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cockroachdb/pebble"
	dnstap "github.com/dnstap/golang-dnstap"
	"github.com/dnstapir/edm/pkg/protocols"
	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/autopaho/queue/file"
	"github.com/fsnotify/fsnotify"
	"github.com/go-playground/validator/v10"
	_ "github.com/grafana/pyroscope-go/godeltaprof/http/pprof" // revive linter: keep blank import close to where it is used for now.
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/miekg/dns"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/go-hll"
	"github.com/smhanov/dawg"
	"github.com/spaolacci/murmur3"
	"github.com/spf13/viper"
	"github.com/yawning/cryptopan"
	"go4.org/netipx"
	"golang.org/x/crypto/argon2"
	"google.golang.org/protobuf/proto"
)

// use a single instance of Validate, it caches struct info
var validate = validator.New(validator.WithRequiredStructEnabled())

type config struct {
	ConfigFile                string `mapstructure:"config-file" validate:"required"`
	DisableSessionFiles       bool   `mapstructure:"disable-session-files"`
	DisableHistogramSender    bool   `mapstructure:"disable-histogram-sender"`
	DisableMQTT               bool   `mapstructure:"disable-mqtt"`
	DisableMQTTFilequeue      bool   `mapstructure:"disable-mqtt-filequeue"`
	InputUnix                 string `mapstructure:"input-unix" validate:"required_without_all=InputTCP InputTLS,excluded_with=InputTCP InputTLS"`
	InputTCP                  string `mapstructure:"input-tcp" validate:"required_without_all=InputUnix InputTLS,excluded_with=InputUnix InputTLS"`
	InputTLS                  string `mapstructure:"input-tls" validate:"required_without_all=InputUnix InputTCP,excluded_with=InputUnix InputTCP"`
	InputTLSCertFile          string `mapstructure:"input-tls-cert-file" validate:"required_with=InputTLS"`
	InputTLSKeyFile           string `mapstructure:"input-tls-key-file" validate:"required_with=InputTLS"`
	InputTLSClientCAFile      string `mapstructure:"input-tls-client-ca-file" validate:"required_with=InputTLS"`
	CryptopanKey              string `mapstructure:"cryptopan-key" validate:"required"`
	CryptopanKeySalt          string `mapstructure:"cryptopan-key-salt" validate:"required"`
	WellKnownDomainsFile      string `mapstructure:"well-known-domains-file" validate:"required"`
	IgnoredClientIPsFile      string `mapstructure:"ignored-client-ips-file"`
	IgnoredQuestionNamesFile  string `mapstructure:"ignored-question-names-file"`
	DataDir                   string `mapstructure:"data-dir" validate:"required"`
	MinimiserWorkers          int    `mapstructure:"minimiser-workers" validate:"required"`
	MQTTSigningKeyFile        string `mapstructure:"mqtt-signing-key-file" validate:"required_without=DisableMQTT"`
	MQTTClientKeyFile         string `mapstructure:"mqtt-client-key-file" validate:"required_without=DisableMQTT"`
	MQTTClientCertFile        string `mapstructure:"mqtt-client-cert-file" validate:"required_without=DisableMQTT"`
	MQTTServer                string `mapstructure:"mqtt-server" validate:"required_without=DisableMQTT"`
	MQTTCAFile                string `mapstructure:"mqtt-ca-file"`
	MQTTKeepalive             uint16 `mapstructure:"mqtt-keepalive" validate:"required_without=DisableMQTT"`
	QnameSeenEntries          int    `mapstructure:"qname-seen-entries"`
	CryptopanAddressEntries   int    `mapstructure:"cryptopan-address-entries"`
	NewQnameBuffer            int    `mapstructure:"newqname-buffer"`
	HTTPCAFile                string `mapstructure:"http-ca-file"`
	HTTPSigningKeyFile        string `mapstructure:"http-signing-key-file" validate:"required_without=DisableHistogramSender"`
	HTTPClientKeyFile         string `mapstructure:"http-client-key-file" validate:"required_without=DisableHistogramSender"`
	HTTPClientCertFile        string `mapstructure:"http-client-cert-file" validate:"required_without=DisableHistogramSender"`
	HTTPURL                   string `mapstructure:"http-url" validate:"required_without=DisableHistogramSender"`
	Debug                     bool   `mapstructure:"debug"`
	DebugDnstapFilename       string `mapstructure:"debug-dnstap-filename"`
	DebugEnableBlockProfiling bool   `mapstructure:"debug-enable-blockprofiling"`
	DebugEnableMutexProfiling bool   `mapstructure:"debug-enable-mutexprofiling"`
}

const dawgNotFound = -1

type edmStatusBits uint64

func (dsb *edmStatusBits) String() string {
	if *dsb >= edmStatusMax {
		return fmt.Sprintf("unknown flags in status: %b", *dsb)
	}

	switch *dsb {
	case edmStatusWellKnownExact:
		return "well-known-exact"
	case edmStatusWellKnownWildcard:
		return "well-known-wildcard"
	}

	var flags []string
	for flag := edmStatusWellKnownExact; flag < edmStatusMax; flag <<= 1 {
		if *dsb&flag != 0 {
			flags = append(flags, flag.String())
		}
	}
	return strings.Join(flags, "|")
}

func (dsb *edmStatusBits) set(flag edmStatusBits) {
	*dsb = *dsb | flag
}

const (
	edmStatusWellKnownExact    edmStatusBits = 1 << iota // 1
	edmStatusWellKnownWildcard                           // 2

	// Always leave max at the end to signal unused bits
	edmStatusMax
)

// Histogram struct implementing description at https://github.com/dnstapir/datasets/blob/main/HistogramReport.md
type histogramData struct {
	StartTime int64 `parquet:"start_time,timestamp(microsecond)"`
	dnsLabels
	// The time we started collecting the data contained in the histogram
	ACount          uint64 `parquet:"a_count"`
	AAAACount       uint64 `parquet:"aaaa_count"`
	MXCount         uint64 `parquet:"mx_count"`
	NSCount         uint64 `parquet:"ns_count"`
	OtherTypeCount  uint64 `parquet:"other_type_count"`
	NonINCount      uint64 `parquet:"non_in_count"`
	OKCount         uint64 `parquet:"ok_count"`
	NXCount         uint64 `parquet:"nx_count"`
	FailCount       uint64 `parquet:"fail_count"`
	OtherRcodeCount uint64 `parquet:"other_rcode_count"`
	EDMStatusBits   uint64 `parquet:"edm_status_bits"`
	// The hll.Hll structs are not expected to be included in the output
	// parquet file, and thus do not need to be exported
	v4ClientHLL hll.Hll
	v6ClientHLL hll.Hll
	// Would probably be cleaner to use a []byte instead of string with
	// struct tag "bytes" here, but it seems the parquet-go library does
	// not handle "optional" []byte fields correctly right now, see:
	// https://github.com/parquet-go/parquet-go/issues/303
	V4ClientCountHLLBytes string `parquet:"v4client_count,bytes,optional"`
	V6ClientCountHLLBytes string `parquet:"v6client_count,bytes,optional"`
}

// We need to create the session data schema by hand instead of basing it of
// the sessionData struct directly because we have uint16 fields for ports and
// these are not currently supported, see:
// https://github.com/parquet-go/parquet-go/pull/122
//
// One drawback of writing out the schema like this is due to the use of a map
// in the parquet.Group we can not control the ordering of the fields, they are
// sorted however, see:
// Issue regarding order:
// https://github.com/parquet-go/parquet-go/issues/43
// Commit that makes the map sorted:
// https://github.com/parquet-go/parquet-go/commit/035e69db6792fdc9089e238084bebe39e26c74b0
var sessionDataSchema = parquet.NewSchema(
	"sessionData",
	parquet.Group{
		"label0":              parquet.Optional(parquet.String()),
		"label1":              parquet.Optional(parquet.String()),
		"label2":              parquet.Optional(parquet.String()),
		"label3":              parquet.Optional(parquet.String()),
		"label4":              parquet.Optional(parquet.String()),
		"label5":              parquet.Optional(parquet.String()),
		"label6":              parquet.Optional(parquet.String()),
		"label7":              parquet.Optional(parquet.String()),
		"label8":              parquet.Optional(parquet.String()),
		"label9":              parquet.Optional(parquet.String()),
		"server_id":           parquet.Optional(parquet.Leaf(parquet.ByteArrayType)),
		"query_time":          parquet.Optional(parquet.Timestamp(parquet.Microsecond)),
		"response_time":       parquet.Optional(parquet.Timestamp(parquet.Microsecond)),
		"source_ipv4":         parquet.Optional(parquet.Uint(32)),
		"dest_ipv4":           parquet.Optional(parquet.Uint(32)),
		"source_ipv6_network": parquet.Optional(parquet.Uint(64)),
		"source_ipv6_host":    parquet.Optional(parquet.Uint(64)),
		"dest_ipv6_network":   parquet.Optional(parquet.Uint(64)),
		"dest_ipv6_host":      parquet.Optional(parquet.Uint(64)),
		"source_port":         parquet.Optional(parquet.Uint(16)),
		"dest_port":           parquet.Optional(parquet.Uint(16)),
		"dns_protocol":        parquet.Optional(parquet.Uint(8)),
		"query_message":       parquet.Optional(parquet.Leaf(parquet.ByteArrayType)),
		"response_message":    parquet.Optional(parquet.Leaf(parquet.ByteArrayType)),
	},
)

type dnsLabels struct {
	// Store label fields as pointers so we can signal them being unset as
	// opposed to an empty string
	Label0 *string `parquet:"label0"`
	Label1 *string `parquet:"label1"`
	Label2 *string `parquet:"label2"`
	Label3 *string `parquet:"label3"`
	Label4 *string `parquet:"label4"`
	Label5 *string `parquet:"label5"`
	Label6 *string `parquet:"label6"`
	Label7 *string `parquet:"label7"`
	Label8 *string `parquet:"label8"`
	Label9 *string `parquet:"label9"`
}

type sessionData struct {
	dnsLabels
	ServerID     *string `parquet:"server_id"`
	QueryTime    *int64  `parquet:"query_time"`
	ResponseTime *int64  `parquet:"response_time"`
	SourceIPv4   *int32  `parquet:"source_ipv4"`
	DestIPv4     *int32  `parquet:"dest_ipv4"`
	// IPv6 addresses are split up into a network and host part, for one thing go does not have native uint128 types
	SourceIPv6Network *int64  `parquet:"source_ipv6_network"`
	SourceIPv6Host    *int64  `parquet:"source_ipv6_host"`
	DestIPv6Network   *int64  `parquet:"dest_ipv6_network"`
	DestIPv6Host      *int64  `parquet:"dest_ipv6_host"`
	SourcePort        *int32  `parquet:"source_port"`
	DestPort          *int32  `parquet:"dest_port"`
	DNSProtocol       *int32  `parquet:"dns_protocol"`
	QueryMessage      *string `parquet:"query_message"`
	ResponseMessage   *string `parquet:"response_message"`
}

type prevSessions struct {
	sessions     []*sessionData
	rotationTime time.Time
}

type certStore struct {
	cert *tls.Certificate
	mtx  sync.RWMutex
}

// Implements tls.Config.GetClientCertificate
func (cs *certStore) getClientCertificate(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()

	if cs.cert == nil {
		return nil, errors.New("no client certificate loaded, you need to call setCert()")
	}

	return cs.cert, nil
}

func (cs *certStore) setCert(certPath string, keyPath string) error {
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return fmt.Errorf("unable to load x509 cert: %w", err)
	}
	cs.mtx.Lock()
	cs.cert = &cert
	cs.mtx.Unlock()

	return nil
}

func newCertStore() *certStore {
	return &certStore{}
}

func (edm *dnstapMinimiser) setLabels(labels []string, labelLimit int, l *dnsLabels) {
	// If labels is nil (the "." zone) we can depend on the zero type of
	// the label fields being nil, so nothing to do
	if labels == nil {
		return
	}

	reverseLabels := edm.reverseLabelsBounded(labels, labelLimit)

	for index := range reverseLabels {
		switch index {
		case 0:
			l.Label0 = &reverseLabels[index]
		case 1:
			l.Label1 = &reverseLabels[index]
		case 2:
			l.Label2 = &reverseLabels[index]
		case 3:
			l.Label3 = &reverseLabels[index]
		case 4:
			l.Label4 = &reverseLabels[index]
		case 5:
			l.Label5 = &reverseLabels[index]
		case 6:
			l.Label6 = &reverseLabels[index]
		case 7:
			l.Label7 = &reverseLabels[index]
		case 8:
			l.Label8 = &reverseLabels[index]
		case 9:
			l.Label9 = &reverseLabels[index]
		}
	}
}

func (edm *dnstapMinimiser) reverseLabelsBounded(labels []string, maxLen int) []string {
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
		boundedReverseLabels = append(boundedReverseLabels, labels[i])
	}

	// If the labels fit inside maxLen then just append the last remaining
	// label as-is
	if len(labels) <= maxLen {
		boundedReverseLabels = append(boundedReverseLabels, labels[0])
	} else {
		// If there are more labels than maxLen we need to concatenate
		// them before appending the last element
		if remainderElems > 0 {
			remainderLabels := []string{}
			for i := remainderElems; i >= 0; i-- {
				remainderLabels = append(remainderLabels, labels[i])
			}

			boundedReverseLabels = append(boundedReverseLabels, strings.Join(remainderLabels, "."))
		}
	}
	return boundedReverseLabels
}

func (edm *dnstapMinimiser) diskCleaner(wg *sync.WaitGroup, sentDir string) {
	// We will scan the directory each tick for sent files to remove.
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
				edm.log.Error("histogramSender: unable to read sent dir", "error", err)
				continue
			}
			for _, dirEntry := range dirEntries {
				if dirEntry.IsDir() {
					continue
				}
				if strings.HasPrefix(dirEntry.Name(), "dns_histogram-") && strings.HasSuffix(dirEntry.Name(), ".parquet") {
					fileInfo, err := dirEntry.Info()
					if err != nil {
						edm.log.Error("diskCleaner: unable to get fileInfo for filename", "error", err, "filename", dirEntry.Name())
						continue
					}

					if time.Since(fileInfo.ModTime()) > oneDay {
						absPath := filepath.Join(sentDir, dirEntry.Name())
						edm.log.Info("diskCleaner: removing file", "filename", absPath)
						err = os.Remove(absPath)
						if err != nil {
							edm.log.Error("diskCleaner: unable to remove sent histogram file", "error", err)
						}
					}
				}
			}
		case <-edm.ctx.Done():
			break timerLoop
		}
	}
	edm.log.Info("exiting diskCleaner loop")
}

// Create a 32 byte length secret based on the supplied -crypto-pan key,
// this way the user can supply a -cryptopan-key of any length and
// we still end up with the 32 byte length expected by AES.
//
// Using a proper password KDF (argon2) might be overkill as we are not
// storing the resulting hash anywhere, but it only affects startup/key
// rotation time of a mostly long running tool.
func getCryptopanAESKey(key string, salt string) []byte {
	var aesKeyLen uint32 = 32
	aesKey := argon2.IDKey([]byte(key), []byte(salt), 1, 64*1024, 4, aesKeyLen)
	return aesKey
}

func (edm *dnstapMinimiser) setCryptopan(key string, salt string, cacheEntries int) error {
	var cpnCache *lru.Cache[netip.Addr, netip.Addr]
	var err error

	if cacheEntries != 0 {
		cpnCache, err = lru.New[netip.Addr, netip.Addr](cacheEntries)
		if err != nil {
			return fmt.Errorf("setCryptopan: unable to create cache: %w", err)
		}
	}

	cpn, err := createCryptopan(key, salt)
	if err != nil {
		return fmt.Errorf("setCryptopan: unable to create cryptopan: %w", err)
	}

	edm.cryptopanMutex.Lock()
	edm.cryptopan = cpn
	edm.cryptopanCache = cpnCache
	edm.cryptopanMutex.Unlock()

	return nil
}

func configUpdater(viperNotifyCh chan fsnotify.Event, edm *dnstapMinimiser) {
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
	t := time.AfterFunc(math.MaxInt64, func() {
		edm.log.Info("configUpdater: reacting to config file update", "filename", e.Name)

		err := edm.updateConfig()
		if err != nil {
			edm.log.Error("configUpdater: unable to update edm config", "error", err)
			return
		}

		conf := edm.getConfig()

		err = edm.setCryptopan(conf.CryptopanKey, conf.CryptopanKeySalt, conf.CryptopanAddressEntries)
		if err != nil {
			edm.log.Error("configUpdater: unable to update cryptopan instance", "error", err)
		}
	})
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

func setHllDefaults() error {
	err := hll.Defaults(hll.Settings{
		Log2m:             10,
		Regwidth:          4,
		ExplicitThreshold: 0,
		SparseEnabled:     true,
	})

	return err
}

func (edm *dnstapMinimiser) setupHistogramSender(httpClientCertStore *certStore) {
	conf := edm.getConfig()

	httpURL, err := url.Parse(conf.HTTPURL)
	if err != nil {
		edm.log.Error("unable to parse 'http-url' setting", "error", err)
		os.Exit(1)
	}

	httpSigningJwk, err := edDsaJWKFromFile(conf.HTTPSigningKeyFile)
	if err != nil {
		edm.log.Error("unable to parse jwk from 'http-signing-key-file'", "error", err)
		os.Exit(1)
	}

	// Leaving these nil will use the OS default CA certs
	var httpCACertPool *x509.CertPool

	if conf.HTTPCAFile != "" {
		// Setup CA cert for validating the aggregate-receiver connection
		httpCACertPool, err = certPoolFromFile(conf.HTTPCAFile)
		if err != nil {
			edm.log.Error("failed to create CA cert pool for '-http-ca-file'", "error", err)
			os.Exit(1)
		}
	}

	edm.aggregSender, err = edm.newAggregateSender(httpURL, httpSigningJwk, httpCACertPool, httpClientCertStore)
	if err != nil {
		edm.log.Error("unable to create aggregate sender", "error", err)
		os.Exit(1)
	}
}

func (edm *dnstapMinimiser) setupMQTT(mqttClientCertStore *certStore) {
	conf := edm.getConfig()

	mqttJWK, err := edDsaJWKFromFile(conf.MQTTSigningKeyFile)
	if err != nil {
		edm.log.Error("unable to parse jwk from 'mqtt-signing-key-file'", "error", err)
		os.Exit(1)
	}

	// Leaving these nil will use the OS default CA certs
	var mqttCACertPool *x509.CertPool

	if conf.MQTTCAFile != "" {
		// Setup CA cert for validating the MQTT connection
		mqttCACertPool, err = certPoolFromFile(conf.MQTTCAFile)
		if err != nil {
			edm.log.Error("failed to create CA cert pool for '--mqtt-ca-file'", "error", err)
			os.Exit(1)
		}
	}

	var mqttFileQueue *file.Queue
	if !conf.DisableMQTTFilequeue {
		mqttQueueDir := filepath.Join(conf.DataDir, "mqtt", "queue")

		err = os.MkdirAll(mqttQueueDir, 0o750)
		if err != nil {
			edm.log.Error("unable to create MQTT queue dir", "error", err, "queue_dir", mqttQueueDir)
			os.Exit(1)
		}

		mqttFileQueue, err = file.New(filepath.Join(conf.DataDir, "mqtt", "queue"), "queue", ".msg")
		if err != nil {
			edm.log.Error("unable to init MQTT queue file based queue", "error", err)
			os.Exit(1)
		}
	}

	mqttClientID := mqttJWK.KeyID() + "-edm"

	edm.log.Info("creating MQTT client", "mqtt_client_id", mqttClientID)

	autopahoConfig, err := edm.newAutoPahoClientConfig(mqttCACertPool, conf.MQTTServer, mqttClientID, mqttClientCertStore, conf.MQTTKeepalive, mqttFileQueue)
	if err != nil {
		edm.log.Error("unable to create autopaho config", "error", err)
		os.Exit(1)
	}

	edm.autopahoCtx, edm.autopahoCancel = context.WithCancel(context.Background())

	autopahoCm, err := autopaho.NewConnection(edm.autopahoCtx, autopahoConfig)
	if err != nil {
		edm.log.Error("unable to create autopaho connection manager", "error", err)
		os.Exit(1)
	}

	// Setup channel for reading messages to publish
	edm.mqttPubCh = make(chan []byte, 100)

	// Connect to the broker - this will return immediately after initiating the connection process
	edm.autopahoWg.Add(1)
	go edm.runAutoPaho(autopahoCm, mqttJWK, mqttFileQueue != nil)
}

func (edm *dnstapMinimiser) setIgnoredQuestionNames(ignoredQuestionsFileName string) error {
	if ignoredQuestionsFileName == "" {
		edm.ignoredQuestionsMutex.Lock()
		if edm.ignoredQuestions != nil {
			err := edm.ignoredQuestions.Close()
			if err != nil {
				edm.log.Error("setIgnoredQuestionNames: failed closing edm.ignoredQuestions for unset filename", "error", err)
			}
			edm.ignoredQuestions = nil
		}
		edm.ignoredQuestionsMutex.Unlock()
		return nil
	}

	dawgFinder, _, err := loadDawgFile(ignoredQuestionsFileName)
	if err != nil {
		return fmt.Errorf("setIgnoredQuestionsNames: unable to load dawg file '%s': %w", ignoredQuestionsFileName, err)
	}

	// We only use the dawg file if there exists at least one name
	// in it. Since the file can be empty we must also be prepared to set
	// our edm field to nil so we do not keep using an old list.
	edm.ignoredQuestionsMutex.Lock()
	if edm.ignoredQuestions != nil {
		err = edm.ignoredQuestions.Close()
		if err != nil {
			edm.log.Error("setIgnoredQuestionNames: failed closing edm.ignoredQuestions", "error", err)
		}
	}
	if dawgFinder.NumAdded() > 0 {
		edm.ignoredQuestions = dawgFinder
	} else {
		edm.ignoredQuestions = nil
	}
	edm.ignoredQuestionsMutex.Unlock()

	if dawgFinder.NumAdded() > 0 {
		edm.log.Info("setIgnoredQuestionNames: DNS question ignore list loaded", "filename", ignoredQuestionsFileName, "num_names", dawgFinder.NumAdded())
	} else {
		edm.log.Info("setIgnoredQuestionNames: DNS question ignore list empty, no question names will be ignored", "filename", ignoredQuestionsFileName, "num_names", dawgFinder.NumAdded())
	}

	return nil
}

func (edm *dnstapMinimiser) setIgnoredClientIPs(ignoredClientsFileName string) error {
	if ignoredClientsFileName == "" {
		edm.ignoredClientsIPSetMutex.Lock()
		edm.ignoredClientsIPSet = nil
		edm.ignoredClientCIDRsParsed = 0
		edm.ignoredClientsIPSetMutex.Unlock()
		return nil
	}

	fh, err := os.Open(filepath.Clean(ignoredClientsFileName))
	if err != nil {
		return fmt.Errorf("setIgnoredClientsIPs: unable to open file: %w", err)
	}
	defer func() {
		err := fh.Close()
		if err != nil {
			edm.log.Error("setIgnoredClientIPs: failed closing fh", "filename", ignoredClientsFileName, "error", err)
		}
	}()

	var b netipx.IPSetBuilder

	var numCIDRs uint64
	scanner := bufio.NewScanner(fh)
	for scanner.Scan() {
		if scanner.Text() == "" || strings.HasPrefix(scanner.Text(), "#") {
			// Skip empty lines and comments
			continue
		}
		prefix, err := netip.ParsePrefix(scanner.Text())
		if err != nil {
			return fmt.Errorf("setIgnoredClientIPs: unable to parse ignored prefix '%s'", scanner.Text())
		}
		b.AddPrefix(prefix)
		numCIDRs++
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("setIgnoredClientIPs: error reading from '%s': %w", ignoredClientsFileName, err)
	}

	// Starts out as nil. We only set it to an initialized IPSet if there is at
	// least one ignored client CIDR present in the input file.
	var ipset *netipx.IPSet
	if numCIDRs > 0 {
		ipset, err = b.IPSet()
		if err != nil {
			return fmt.Errorf("setIgnoredClientIPs: IPSet creation failed: %w", err)
		}
	}

	edm.ignoredClientsIPSetMutex.Lock()
	edm.ignoredClientsIPSet = ipset
	edm.ignoredClientCIDRsParsed = numCIDRs
	edm.ignoredClientsIPSetMutex.Unlock()

	if ipset != nil {
		edm.log.Info("setIgnoredClientIPs: DNS client ignore list loaded", "filename", ignoredClientsFileName, "num_cidrs", numCIDRs)
	} else {
		edm.log.Info("setIgnoredClientIPs: DNS client ignore list empty, no clients will be ignored", "filename", ignoredClientsFileName, "num_cidrs", numCIDRs)
	}

	return nil
}

func (edm *dnstapMinimiser) getNumIgnoredClientCIDRs() uint64 {
	edm.ignoredClientsIPSetMutex.RLock()
	defer edm.ignoredClientsIPSetMutex.RUnlock()

	return edm.ignoredClientCIDRsParsed
}

func (edm *dnstapMinimiser) fsEventWatcher() {
	// Like in
	// https://github.com/fsnotify/fsnotify/blob/main/cmd/fsnotify/dedup.go
	// we keep a timer per registered filename
	timers := map[string]*time.Timer{}
	timersMutex := new(sync.Mutex)

	callbackHandler := func(callbacks []func(string) error, name string) func() {
		return func() {
			for _, callback := range callbacks {
				err := callback(name)
				if err != nil {
					edm.log.Error("fsEventWatcher: callback error", "filename", name, "error", err)
				}
			}

			// Cleanup expired timer
			timersMutex.Lock()
			delete(timers, name)
			timersMutex.Unlock()
		}
	}

	for {
		select {
		case event, ok := <-edm.fsWatcher.Events:
			if !ok {
				// watcher is closed
				return
			}

			if !event.Has(fsnotify.Write) && !event.Has(fsnotify.Create) {
				continue
			}

			cleanName := filepath.Clean(event.Name)

			edm.fsWatcherMutex.RLock()
			callbacks, ok := edm.fsWatcherFuncs[cleanName]
			edm.fsWatcherMutex.RUnlock()
			if !ok {
				if edm.debug {
					edm.log.Info("skipping event for unregistered file", "op", event.Op.String(), "filename", cleanName)
				}
				continue
			}

			timersMutex.Lock()
			t, ok := timers[cleanName]
			timersMutex.Unlock()
			if !ok {
				t = time.AfterFunc(math.MaxInt64, callbackHandler(callbacks, cleanName))
				t.Stop()

				timersMutex.Lock()
				timers[cleanName] = t
				timersMutex.Unlock()
			}

			t.Reset(100 * time.Millisecond)
		case err, ok := <-edm.fsWatcher.Errors:
			if !ok {
				// watcher is closed
				return
			}
			edm.log.Error("fsEventWatcher: error received", "error", err)
		}
	}
}

func (edm *dnstapMinimiser) registerFSWatcher(filename string, callback func(string) error) error {
	// Adding the same dir multiple times is a no-op, so it is OK to
	// add multiple files from the same directory.
	err := edm.fsWatcher.Add(filepath.Dir(filename))
	if err != nil {
		return fmt.Errorf("registerFSWatcher: unable to add directory '%s': %w", filepath.Dir(filename), err)
	}

	edm.fsWatcherMutex.Lock()
	edm.fsWatcherFuncs[filename] = append(edm.fsWatcherFuncs[filename], callback)
	edm.fsWatcherMutex.Unlock()

	return nil
}

type edmConfiger interface {
	getConfig() (config, error)
}

type testConfiger struct {
	CryptopanKey            string
	CryptopanKeySalt        string
	CryptopanAddressEntries int
	Debug                   bool
	DisableHistogramSender  bool
	DisableMQTT             bool
}

func (tc testConfiger) getConfig() (config, error) {
	return config{
		CryptopanKey:            tc.CryptopanKey,
		CryptopanKeySalt:        tc.CryptopanKeySalt,
		CryptopanAddressEntries: tc.CryptopanAddressEntries,
		Debug:                   tc.Debug,
		DisableHistogramSender:  tc.DisableHistogramSender,
		DisableMQTT:             tc.DisableMQTT,
	}, nil
}

type viperConfiger struct{}

func (vc viperConfiger) getConfig() (config, error) {
	conf := config{}
	err := viper.UnmarshalExact(&conf)
	if err != nil {
		return config{}, fmt.Errorf("getViperConfig: unable to unmarshal config: %w", err)
	}

	err = validate.Struct(conf)
	if err != nil {
		return config{}, fmt.Errorf("getViperConfig: unable to validate config: %w", err)
	}

	return conf, nil
}

func (edm *dnstapMinimiser) updateConfig() error {
	edm.confMutex.Lock()
	defer edm.confMutex.Unlock()

	conf, err := edm.configer.getConfig()
	if err != nil {
		return err
	}

	edm.conf = conf

	return nil
}

func (edm *dnstapMinimiser) getConfig() config {
	edm.confMutex.RLock()
	conf := edm.conf
	edm.confMutex.RUnlock()

	return conf
}

func Run(logger *slog.Logger, loggerLevel *slog.LevelVar) {
	// Create an instance of the minimiser
	vc := viperConfiger{}
	edm, err := newDnstapMinimiser(logger, vc)
	if err != nil {
		logger.Error("unable to init edm", "error", err)
		os.Exit(1)
	}
	defer edm.stop()
	defer edm.fsWatcher.Close()

	// Create startConf for some initial setup. Other edm methods that need
	// to read the config should call edm.getConfig() internally so they
	// get the latest config.
	startConf := edm.getConfig()

	if startConf.DebugEnableBlockProfiling {
		logger.Info("enabling blocking profiling")
		runtime.SetBlockProfileRate(int(time.Millisecond))
	}
	if startConf.DebugEnableMutexProfiling {
		logger.Info("enabling mutex profiling")
		runtime.SetMutexProfileFraction(100)
	}

	if startConf.Debug {
		loggerLevel.Set(slog.LevelDebug)
	}

	err = edm.setIgnoredClientIPs(startConf.IgnoredClientIPsFile)
	if err != nil {
		logger.Error("unable to configure ignored client IPs", "error", err)
		os.Exit(1)
	}

	err = edm.registerFSWatcher(startConf.IgnoredClientIPsFile, edm.setIgnoredClientIPs)
	if err != nil {
		logger.Error("unable to register fsWatcher callback", "filename", startConf.IgnoredClientIPsFile, "error", err)
		os.Exit(1)
	}

	err = edm.setIgnoredQuestionNames(startConf.IgnoredQuestionNamesFile)
	if err != nil {
		logger.Error("unable to configure ignored question names", "error", err)
		os.Exit(1)
	}

	err = edm.registerFSWatcher(startConf.IgnoredQuestionNamesFile, edm.setIgnoredQuestionNames)
	if err != nil {
		logger.Error("unable to register fsWatcher callback", "filename", startConf.IgnoredQuestionNamesFile, "error", err)
		os.Exit(1)
	}

	viperNotifyCh := make(chan fsnotify.Event)

	go configUpdater(viperNotifyCh, edm)

	viper.OnConfigChange(func(e fsnotify.Event) {
		viperNotifyCh <- e
	})

	pdbDir := filepath.Join(startConf.DataDir, "pebble")
	pdb, err := pebble.Open(pdbDir, &pebble.Options{})
	if err != nil {
		logger.Error("unable to open pebble database", "dir", pdbDir, "error", err)
		os.Exit(1)
	}
	defer func() {
		err = pdb.Close()
		if err != nil {
			edm.log.Error("unable to close pebble database", "error", err)
		}
	}()

	if !edm.histogramSenderDisabled {
		// Setup client cert/key for mTLS authentication
		httpClientCertStore := newCertStore()
		err = httpClientCertStore.setCert(startConf.HTTPClientCertFile, startConf.HTTPClientKeyFile)
		if err != nil {
			edm.log.Error("unable to load x509 HTTP client cert", "error", err)
			os.Exit(1)
		}

		edm.setupHistogramSender(httpClientCertStore)

		err = edm.registerFSWatcher(startConf.HTTPClientCertFile, func(filename string) error {
			conf := edm.getConfig()
			edm.log.Info("reloading HTTP cert store because file was modified", "filename", filename)
			err := httpClientCertStore.setCert(conf.HTTPClientCertFile, conf.HTTPClientKeyFile)
			return err
		})
		if err != nil {
			logger.Error("unable to register fsWatcher callback", "filename", startConf.HTTPClientCertFile, "error", err)
			os.Exit(1)
		}
	}

	if !edm.mqttDisabled {
		// Setup client cert/key for mTLS authentication
		mqttClientCertStore := newCertStore()
		err = mqttClientCertStore.setCert(startConf.MQTTClientCertFile, startConf.MQTTClientKeyFile)
		if err != nil {
			edm.log.Error("unable to load x509 mqtt client cert", "error", err)
			os.Exit(1)
		}

		edm.setupMQTT(mqttClientCertStore)

		err = edm.registerFSWatcher(startConf.MQTTClientCertFile, func(filename string) error {
			conf := edm.getConfig()
			edm.log.Info("reloading MQTT cert store because file was modified", "filename", filename)
			err := mqttClientCertStore.setCert(conf.MQTTClientCertFile, conf.MQTTClientKeyFile)
			return err
		})
		if err != nil {
			logger.Error("unable to register fsWatcher callback", "filename", startConf.MQTTClientCertFile, "error", err)
			os.Exit(1)
		}
	}

	go edm.fsEventWatcher()

	// Setup the dnstap.Input, only one at a time is supported.
	var dti *dnstap.FrameStreamSockInput
	if startConf.InputUnix != "" {
		logger.Info("creating dnstap unix socket", "socket", startConf.InputUnix)
		dti, err = dnstap.NewFrameStreamSockInputFromPath(startConf.InputUnix)
		if err != nil {
			logger.Error("unable to create dnstap unix socket", "error", err)
			os.Exit(1)
		}
	} else if startConf.InputTCP != "" {
		logger.Info("creating plaintext dnstap TCP socket", "socket", startConf.InputTCP)
		l, err := net.Listen("tcp", startConf.InputTCP)
		if err != nil {
			logger.Error("unable to create plaintext dnstap TCP socket", "error", err)
			os.Exit(1)
		}
		dti = dnstap.NewFrameStreamSockInput(l)
	} else if startConf.InputTLS != "" {
		logger.Info("creating encrypted dnstap TLS socket", "socket", startConf.InputTLS)
		dnstapInputCert, err := tls.LoadX509KeyPair(startConf.InputTLSCertFile, startConf.InputTLSKeyFile)
		if err != nil {
			logger.Error("unable to load x509 dnstap listener cert", "error", err)
			os.Exit(1)
		}
		dnstapTLSConfig := &tls.Config{
			Certificates: []tls.Certificate{dnstapInputCert},
			MinVersion:   tls.VersionTLS13,
		}

		// Enable client mTLS (client cert auth) if a CA file was passed:
		if startConf.InputTLSClientCAFile != "" {
			logger.Info("dnstap socket requiring valid client certs", "ca-file", startConf.InputTLSClientCAFile)
			inputTLSClientCACertPool, err := certPoolFromFile(startConf.InputTLSClientCAFile)
			if err != nil {
				logger.Error("failed to create CA cert pool for '-input-tls-client-ca-file': %s", "error", err)
				os.Exit(1)
			}

			dnstapTLSConfig.ClientAuth = tls.RequireAndVerifyClientCert
			dnstapTLSConfig.ClientCAs = inputTLSClientCACertPool
		}

		l, err := tls.Listen("tcp", startConf.InputTLS, dnstapTLSConfig)
		if err != nil {
			logger.Error("unable to create TCP listener", "error", err)
			os.Exit(1)
		}
		dti = dnstap.NewFrameStreamSockInput(l)
	}
	dti.SetTimeout(time.Second * 5)
	dti.SetLogger(log.Default())

	err = setHllDefaults()
	if err != nil {
		logger.Error("unable to set Hll defaults", "error", err)
		os.Exit(1)
	}

	// We need to keep track of domains that are not on the well-known
	// domain list yet we have seen since we started. To limit the
	// possibility of unbounded memory usage we use a LRU cache instead of
	// something simpler like a map.
	seenQnameLRU, err := lru.New[string, struct{}](startConf.QnameSeenEntries)
	if err != nil {
		logger.Error("unable to create seen-qname LRU", "error", err)
		os.Exit(1)
	}

	// Uses the default mux which is modified by importing net/http/pprof
	pprofServer := &http.Server{
		Addr:         "127.0.0.1:6060",
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 31 * time.Second,
	}

	go func() {
		err := pprofServer.ListenAndServe()
		logger.Error("pprofServer failed", "error", err)
	}()

	metricsMux := http.NewServeMux()
	metricsServer := &http.Server{
		Addr:           "127.0.0.1:2112",
		Handler:        metricsMux,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	// Setup custom promHandler since we want to use our per-edm registry
	metricsMux.Handle("/metrics", promhttp.InstrumentMetricHandler(edm.promReg, promhttp.HandlerFor(edm.promReg, promhttp.HandlerOpts{Registry: edm.promReg})))
	go func() {
		err := metricsServer.ListenAndServe()
		logger.Error("metricsServer failed", "error", err)
	}()

	var wg sync.WaitGroup

	// Write histogram file to an outbox dir where it will get picked up by
	// the histogram sender. Upon being sent it will be moved to the sent dir.
	dataDir := startConf.DataDir
	outboxDir := filepath.Join(dataDir, "parquet", "histograms", "outbox")
	sentDir := filepath.Join(dataDir, "parquet", "histograms", "sent")

	go edm.monitorChannelLen()

	// Labels 0-9
	labelLimit := 10

	// Start record writers and data senders in the background
	wg.Add(1)
	go edm.sessionWriter(dataDir, &wg)
	wg.Add(1)
	go edm.histogramWriter(labelLimit, outboxDir, &wg)
	if !edm.histogramSenderDisabled {
		wg.Add(1)
		go edm.histogramSender(outboxDir, sentDir, &wg)
	}
	if !edm.mqttDisabled {
		wg.Add(1)
		go edm.newQnamePublisher(&wg)
	}

	wg.Add(1)
	go edm.diskCleaner(&wg, sentDir)

	dawgFile := startConf.WellKnownDomainsFile

	dawgFinder, dawgModTime, err := loadDawgFile(dawgFile)
	if err != nil {
		edm.log.Error("Run: loadDawgFile failed", "error", err)
		os.Exit(1)
	}

	wkdTracker, err := newWellKnownDomainsTracker(dawgFinder, dawgModTime)
	if err != nil {
		edm.log.Error(err.Error())
		os.Exit(1)
	}

	debugDnstapFilename := startConf.DebugDnstapFilename

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
		debugDnstapFile, err = os.OpenFile(debugDnstapFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			edm.log.Error("unable to open debug dnstap file", "error", err.Error(), "filename", debugDnstapFilename)
			os.Exit(1)
		}
		defer func() {
			err := debugDnstapFile.Close()
			if err != nil {
				edm.log.Error("unable to close debug dnstap file", "error", err, "filename", debugDnstapFile.Name())
			}
		}()
	}

	// Start data collector
	wg.Add(1)
	go edm.dataCollector(&wg, wkdTracker, dawgFile)

	var minimiserWg sync.WaitGroup

	numMinimiserWorkers := startConf.MinimiserWorkers
	if numMinimiserWorkers <= 0 {
		numMinimiserWorkers = runtime.GOMAXPROCS(0)
	}

	// Start minimiser
	for minimiserID := 0; minimiserID < numMinimiserWorkers; minimiserID++ {
		edm.log.Info("Run: starting minimiser worker", "minimiser_id", minimiserID)
		minimiserWg.Add(1)
		go edm.runMinimiser(minimiserID, &minimiserWg, seenQnameLRU, pdb, startConf.DisableSessionFiles, debugDnstapFile, labelLimit, wkdTracker)
	}

	// Start dnstap.Input
	go dti.ReadInto(edm.inputChannel)

	// Wait here until all instances of runMinimiser() is done
	minimiserWg.Wait()

	// Tell collector it is time to stop reading data
	close(wkdTracker.stop)

	// Make sure writers have completed their work
	close(edm.newQnamePublisherCh)

	// Stop the MQTT publisher
	if !edm.mqttDisabled {
		edm.log.Info("Run: stopping MQTT publisher")
		edm.autopahoCancel()
	}

	// Wait for all workers to exit
	edm.log.Info("Run: waiting for other workers to exit")
	wg.Wait()

	// Wait for graceful disconnection from MQTT bus
	if !edm.mqttDisabled {
		edm.log.Info("Run: waiting on MQTT disconnection")
		edm.autopahoWg.Wait()
	}
}

type dnstapMinimiser struct {
	configer                  edmConfiger
	conf                      config
	confMutex                 sync.RWMutex
	inputChannel              chan []byte          // the channel expected to be passed to dnstap ReadInto()
	log                       *slog.Logger         // any information logging is sent here
	cryptopan                 *cryptopan.Cryptopan // used for pseudonymising IP addresses
	cryptopanCache            *lru.Cache[netip.Addr, netip.Addr]
	cryptopanMutex            sync.RWMutex // Mutex for protecting updates cryptopan at runtime
	promReg                   *prometheus.Registry
	promCryptopanCacheHit     prometheus.Counter
	promCryptopanCacheEvicted prometheus.Counter
	promDnstapProcessed       prometheus.Counter
	promNewQnameQueued        prometheus.Counter
	promNewQnameDiscarded     prometheus.Counter
	promSeenQnameLRUEvicted   prometheus.Counter
	promNewQnameChannelLen    prometheus.Gauge
	promClientIPIgnored       prometheus.Counter
	promClientIPIgnoredError  prometheus.Counter
	promQuestionNameIgnored   prometheus.Counter
	ctx                       context.Context
	stop                      context.CancelFunc // call this to gracefully stop runMinimiser()
	debug                     bool               // if we should print debug messages during operation
	sessionWriterCh           chan *prevSessions
	histogramWriterCh         chan *wellKnownDomainsData
	newQnamePublisherCh       chan *protocols.NewQnameJSON
	sessionCollectorCh        chan *sessionData
	histogramSenderDisabled   bool
	aggregSender              aggregateSender
	mqttDisabled              bool
	mqttPubCh                 chan []byte
	autopahoCtx               context.Context
	autopahoCancel            context.CancelFunc
	autopahoWg                sync.WaitGroup
	ignoredClientsIPSet       *netipx.IPSet
	ignoredClientCIDRsParsed  uint64
	ignoredClientsIPSetMutex  sync.RWMutex // Mutex for protecting updates to ignored client IPs at runtime
	ignoredQuestions          dawg.Finder
	ignoredQuestionsMutex     sync.RWMutex
	fsWatcher                 *fsnotify.Watcher
	fsWatcherFuncs            map[string][]func(string) error
	fsWatcherMutex            sync.RWMutex
}

func createCryptopan(key string, salt string) (*cryptopan.Cryptopan, error) {
	cryptopanKey := getCryptopanAESKey(key, salt)

	cpn, err := cryptopan.New(cryptopanKey)
	if err != nil {
		return nil, fmt.Errorf("createCryptopan: %w", err)
	}

	return cpn, nil
}

func newDnstapMinimiser(logger *slog.Logger, edmConf edmConfiger) (*dnstapMinimiser, error) {
	edm := &dnstapMinimiser{
		configer: edmConf,
	}

	err := edm.updateConfig()
	if err != nil {
		return nil, fmt.Errorf("newDnstapMinimiser: unable to set config: %w", err)
	}

	conf := edm.getConfig()

	err = edm.setCryptopan(conf.CryptopanKey, conf.CryptopanKeySalt, conf.CryptopanAddressEntries)
	if err != nil {
		return nil, fmt.Errorf("newDnstapMinimiser: %w", err)
	}

	// Exit gracefully on SIGINT or SIGTERM
	edm.ctx, edm.stop = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	// Use separate prometheus registry for each edm instance, otherwise
	// trying to run tests where each test do their own call to
	// newDnstapMinimiser() will panic:
	// ===
	// panic: duplicate metrics collector registration attempted
	// ===
	// Some more info at https://github.com/prometheus/client_golang/issues/716
	promReg := prometheus.NewRegistry()

	// Mimic default collectors used by the global prometheus instance
	promReg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	promReg.MustRegister(collectors.NewGoCollector())

	edm.promCryptopanCacheHit = promauto.With(promReg).NewCounter(prometheus.CounterOpts{
		Name: "edm_cryptopan_lru_hit_total",
		Help: "The total number of times we got a hit in the cryptopan address LRU cache",
	})

	edm.promCryptopanCacheEvicted = promauto.With(promReg).NewCounter(prometheus.CounterOpts{
		Name: "edm_cryptopan_lru_evicted_total",
		Help: "The total number of times something was evicted from the cryptopan address LRU cache",
	})

	edm.promDnstapProcessed = promauto.With(promReg).NewCounter(prometheus.CounterOpts{
		Name: "edm_processed_dnstap_total",
		Help: "The total number of processed dnstap packets",
	})

	edm.promNewQnameQueued = promauto.With(promReg).NewCounter(prometheus.CounterOpts{
		Name: "edm_new_qname_queued_total",
		Help: "The total number of queued new_qname events",
	})

	edm.promNewQnameDiscarded = promauto.With(promReg).NewCounter(prometheus.CounterOpts{
		Name: "edm_new_qname_discarded_total",
		Help: "The total number of discarded new_qname events",
	})

	edm.promSeenQnameLRUEvicted = promauto.With(promReg).NewCounter(prometheus.CounterOpts{
		Name: "edm_seen_qname_lru_evicted_total",
		Help: "The total number of times something was evicted from the new_qname LRU cache",
	})

	edm.promNewQnameChannelLen = promauto.With(promReg).NewGauge(prometheus.GaugeOpts{
		Name: "edm_new_qname_ch_len",
		Help: "The number of new_qname events in the channel buffer",
	})

	edm.promClientIPIgnored = promauto.With(promReg).NewCounter(prometheus.CounterOpts{
		Name: "edm_ignored_client_ip_total",
		Help: "The total number of times we have ignored a dnstap packet because of client IP",
	})

	edm.promClientIPIgnoredError = promauto.With(promReg).NewCounter(prometheus.CounterOpts{
		Name: "edm_ignored_client_ip_error_total",
		Help: "The total number of times we have ignored a dnstap packet because of client IP error, should always be 0",
	})

	edm.promQuestionNameIgnored = promauto.With(promReg).NewCounter(prometheus.CounterOpts{
		Name: "edm_ignored_question_name_total",
		Help: "The total number of times we have ignored a dnstap packet because of the name in the question section",
	})

	edm.promReg = promReg
	// Size 32 matches unexported "const outputChannelSize = 32" in
	// https://github.com/dnstap/golang-dnstap/blob/master/dnstap.go
	edm.inputChannel = make(chan []byte, 32)
	edm.log = logger
	edm.debug = conf.Debug
	edm.histogramSenderDisabled = conf.DisableHistogramSender
	edm.mqttDisabled = conf.DisableMQTT

	edm.fsWatcher, err = fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("newDnstapMinimiser: unable to create fsWatcher: %w", err)
	}

	edm.fsWatcherFuncs = map[string][]func(string) error{}

	// Setup channels for feeding writers and data senders that should do
	// their work outside the main minimiser loop. They are buffered to
	// to not block the loop if writing/sending data is slow.
	// NOTE: Remember to close all of these channels at the end of the
	// minimiser loop, otherwise the program can hang on shutdown.
	edm.sessionWriterCh = make(chan *prevSessions, 100)
	edm.histogramWriterCh = make(chan *wellKnownDomainsData, 100)
	edm.newQnamePublisherCh = make(chan *protocols.NewQnameJSON, conf.NewQnameBuffer)
	edm.sessionCollectorCh = make(chan *sessionData, 100)

	return edm, nil
}

type wellKnownDomainsTracker struct {
	mutex sync.RWMutex
	wellKnownDomainsData
	updateCh    chan wkdUpdate
	dawgModTime time.Time
	retryCh     chan wkdUpdate
	stop        chan struct{}
	retryerDone chan struct{}
}

type wellKnownDomainsData struct {
	// Store a pointer to histogramData so we can assign to it without
	// "cannot assign to struct field in map" issues
	m             map[int]*histogramData
	rotationTime  time.Time
	dawgFinder    dawg.Finder
	dawgIsRotated bool
}

func newWellKnownDomainsTracker(dawgFinder dawg.Finder, dawgModTime time.Time) (*wellKnownDomainsTracker, error) {
	return &wellKnownDomainsTracker{
		wellKnownDomainsData: wellKnownDomainsData{
			m:          map[int]*histogramData{},
			dawgFinder: dawgFinder,
		},
		updateCh:    make(chan wkdUpdate, 10000),
		retryCh:     make(chan wkdUpdate, 10000),
		dawgModTime: dawgModTime,
		stop:        make(chan struct{}),
		retryerDone: make(chan struct{}),
	}, nil
}

// Try to find a domain name string match in DAWG data and return the index as
// well as if it was found based on a suffix string or not.
func getDawgIndex(dawgFinder dawg.Finder, name string) (int, bool) {
	// Ignore capitalisation in labels
	name = strings.ToLower(name)

	// Try exact match first
	dawgIndex := dawgFinder.IndexOf(name)

	if dawgIndex == dawgNotFound {
		// Next try to look up suffix matches, so for the name
		// "www.example.com." we will check for the strings
		// ".example.com." and ".com.".
		for index, end := dns.NextLabel(name, 0); !end; index, end = dns.NextLabel(name, index) {
			dawgIndex = dawgFinder.IndexOf(name[index-1:])
			if dawgIndex != dawgNotFound {
				return dawgIndex, true
			}
		}
	}

	return dawgIndex, false
}

type wkdUpdate struct {
	// embed histogramData so we automatically have access to all the
	// fields we may want to increment with an update message.
	histogramData
	dawgIndex   int
	suffixMatch bool
	hllHash     uint64
	ip          netip.Addr
	msg         *dns.Msg
	dawgModTime time.Time
	retry       int
	retryLimit  int
}

func (wkd *wellKnownDomainsTracker) lookup(msg *dns.Msg) (int, bool, time.Time) {
	wkd.mutex.RLock()
	defer wkd.mutex.RUnlock()

	dawgIndex, suffixMatch := getDawgIndex(wkd.dawgFinder, msg.Question[0].Name)

	return dawgIndex, suffixMatch, wkd.dawgModTime
}

func (wkd *wellKnownDomainsTracker) updateRetryer(edm *dnstapMinimiser, wg *sync.WaitGroup) {
	defer wg.Done()

	for wu := range wkd.retryCh {
		wu.retry++
		if wu.retry >= wu.retryLimit {
			edm.log.Info("ignoring wkd update since retry counter hit retry limit", "retry", wu.retry, "retry_limit", wu.retryLimit)
			continue
		}

		dawgIndex, suffixMatch, dawgModTime := wkd.lookup(wu.msg)
		if dawgIndex == dawgNotFound {
			edm.log.Info("ignoring wkd update because name does not exist in updated wkd tracker", "update_dawg_modtime", wkd.dawgModTime, "wkd_dawg_modtime", wkd.dawgModTime)
			continue
		}

		// Refresh the update to match new dawg version
		wu.dawgIndex = dawgIndex
		wu.suffixMatch = suffixMatch
		wu.dawgModTime = dawgModTime

		if edm.debug {
			edm.log.Debug("resending refreshed wkd update", "retry_counter", wu.retry)
		}
		wkd.updateCh <- wu
	}

	edm.log.Info("updateRetryer: exiting loop")
	close(wkd.retryerDone)
}

func (wkd *wellKnownDomainsTracker) sendUpdate(ipBytes []byte, msg *dns.Msg, dawgIndex int, suffixMatch bool, dawgModTime time.Time) {
	wu := wkdUpdate{
		dawgIndex:   dawgIndex,
		suffixMatch: suffixMatch,
		dawgModTime: dawgModTime,
		hllHash:     0,
		retryLimit:  10,
		msg:         msg,
	}

	// Create hash from IP address for use in HLL data
	ip, ok := netip.AddrFromSlice(ipBytes)
	if ok {
		// We use a deterministic seed by design to be able to combine HLL
		// datasets.
		wu.hllHash = murmur3.Sum64(ipBytes)
		wu.ip = ip
	}

	// Counters based on header
	switch msg.Rcode {
	case dns.RcodeSuccess:
		wu.OKCount++
	case dns.RcodeNameError:
		wu.NXCount++
	case dns.RcodeServerFailure:
		wu.FailCount++
	default:
		wu.OtherRcodeCount++
	}

	// Counters based on question class and type
	if msg.Question[0].Qclass == dns.ClassINET {
		switch msg.Question[0].Qtype {
		case dns.TypeA:
			wu.ACount++
		case dns.TypeAAAA:
			wu.AAAACount++
		case dns.TypeMX:
			wu.MXCount++
		case dns.TypeNS:
			wu.NSCount++
		default:
			wu.OtherTypeCount++
		}
	} else {
		wu.NonINCount++
	}

	wkd.updateCh <- wu
}

func (wkd *wellKnownDomainsTracker) rotateTracker(edm *dnstapMinimiser, dawgFile string, rotationTime time.Time) (*wellKnownDomainsData, error) {
	dawgFileChanged := false
	var dawgFinder dawg.Finder

	fileInfo, err := os.Stat(dawgFile)
	if err != nil {
		return nil, fmt.Errorf("rotateTracker: unable to stat dawgFile '%s': %w", dawgFile, err)
	}

	if fileInfo.ModTime() != wkd.dawgModTime {
		dawgFinder, err = dawg.Load(dawgFile)
		if err != nil {
			return nil, fmt.Errorf("rotateTracker: dawg.Load(): %w", err)
		}
		dawgFileChanged = true
		edm.log.Info("dawg file modificatiom changed, will reload file", "prev_time", wkd.dawgModTime, "cur_time", fileInfo.ModTime())
	}

	prevWKD := &wellKnownDomainsData{}

	// Swap the map in use so we can write parquet data outside of the write lock
	wkd.mutex.Lock()
	prevWKD.m = wkd.m
	prevWKD.dawgFinder = wkd.dawgFinder
	wkd.m = map[int]*histogramData{}
	if dawgFileChanged {
		wkd.dawgFinder = dawgFinder
		wkd.dawgModTime = fileInfo.ModTime()
		prevWKD.dawgIsRotated = true
	}
	wkd.mutex.Unlock()

	prevWKD.rotationTime = rotationTime

	return prevWKD, nil
}

// Check if we have already seen this qname since we started.
func (edm *dnstapMinimiser) qnameSeen(msg *dns.Msg, seenQnameLRU *lru.Cache[string, struct{}], pdb *pebble.DB) bool {
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
		edm.promSeenQnameLRUEvicted.Inc()
	}

	// It was not in the LRU cache, does it exist in pebble (on disk)?
	_, closer, err := pdb.Get([]byte(msg.Question[0].Name))
	if err == nil {
		// The value exists in pebble
		if err := closer.Close(); err != nil {
			edm.log.Error("unable to close pebble get", "error", err)
		}
		return true
	}

	// If the key does not exist in pebble we insert it
	if errors.Is(err, pebble.ErrNotFound) {
		if err := pdb.Set([]byte(msg.Question[0].Name), []byte{}, pebble.Sync); err != nil {
			edm.log.Error("unable to insert key in pebble", "error", err)
		}
		return false
	}

	// Some other error occured
	edm.log.Error("unable to get key from pebble", "error", err)
	return false
}

func (edm *dnstapMinimiser) clientIPIsIgnored(dt *dnstap.Dnstap) bool {
	// edm.ignoredClientsIPSet can be modified at runtime so wrap everything
	// in a RO lock
	edm.ignoredClientsIPSetMutex.RLock()
	defer edm.ignoredClientsIPSetMutex.RUnlock()

	if edm.ignoredClientsIPSet != nil {
		clientIP, ok := netip.AddrFromSlice(dt.Message.QueryAddress)
		if !ok {
			// If we have a list of clients to
			// ignore but are not able to
			// understand the QueryAddress lets err
			// on the side of caution and ignore
			// such packets as well while making
			// noise in logs so it can be investigated
			edm.log.Error("unable to parse QueryAddress for ignore-checking, ignoring dnstap packet to be safe, please investigate")
			edm.promClientIPIgnoredError.Inc()
			return true
		}

		if edm.ignoredClientsIPSet.Contains(clientIP) {
			edm.promClientIPIgnored.Inc()
			return true
		}
	}
	return false
}

func (edm *dnstapMinimiser) questionIsIgnored(msg *dns.Msg) bool {
	// edm.ignoredQuestions can be modified at runtime so wrap everything
	// in a RO lock
	edm.ignoredQuestionsMutex.RLock()
	defer edm.ignoredQuestionsMutex.RUnlock()

	if edm.ignoredQuestions != nil {
		// While uncommon, if there happens to be multiple questions in
		// the packet we will consider the message ignored if any of them matches the
		// ignore list.
		for _, question := range msg.Question {
			dawgIndex, _ := getDawgIndex(edm.ignoredQuestions, question.Name)
			if dawgIndex != dawgNotFound {
				edm.promQuestionNameIgnored.Inc()
				return true
			}
		}
	}
	return false
}

// runMinimiser is the main loop of the program, it reads dnstap from
// inputChannel and decides what further processing to do.
// To gracefully stop runMinimiser() you can call edm.stop().
func (edm *dnstapMinimiser) runMinimiser(minimiserID int, wg *sync.WaitGroup, seenQnameLRU *lru.Cache[string, struct{}], pdb *pebble.DB, disableSessionFiles bool, debugDnstapFile *os.File, labelLimit int, wkdTracker *wellKnownDomainsTracker) {
	defer wg.Done()

	dt := &dnstap.Dnstap{}

minimiserLoop:
	for {
		select {
		case frame := <-edm.inputChannel:
			edm.promDnstapProcessed.Inc()
			if err := proto.Unmarshal(frame, dt); err != nil {
				edm.log.Error("dnstapMinimiser.runMinimiser: proto.Unmarshal() failed, returning", "error", err, "minimiser_id", minimiserID)
				break minimiserLoop
			}

			// Keep in mind that this outputs the unmodified dnstap
			// data, so it contains sensitive information.
			if debugDnstapFile != nil {
				out, ok := dnstap.JSONFormat(dt)
				if !ok {
					edm.log.Error("unable to format dnstap debug log")
				} else {
					_, err := debugDnstapFile.Write(out)
					if err != nil {
						edm.log.Error("unable to write to dnstap debug file", "error", err, "filename", debugDnstapFile.Name(), "minimiser_id", minimiserID)
					}
				}
			}

			isQuery := strings.HasSuffix(dnstap.Message_Type_name[int32(*dt.Message.Type)], "_QUERY")

			// For now we only care about response type dnstap packets
			if isQuery {
				continue
			}

			if edm.clientIPIsIgnored(dt) {
				continue
			}

			// Keep around the unpseudonymised client IP for HLL
			// data, be careful with logging or otherwise handling
			// this IP as it is sensitive.
			dangerRealClientIP := make([]byte, len(dt.Message.QueryAddress))
			copy(dangerRealClientIP, dt.Message.QueryAddress)

			edm.pseudonymiseDnstap(dt)

			msg, timestamp := edm.parsePacket(dt, isQuery)

			// Create a less specific timestamp for data sent to
			// core to make precise tracking harder.
			truncatedTimestamp := timestamp.Truncate(time.Minute)

			// For cases where we were unable to unpack the DNS message we
			// skip parsing.
			if msg == nil {
				edm.log.Error("unable to parse dnstap message, skipping parsing", "minimiser_id", minimiserID)
				continue
			}

			if len(msg.Question) == 0 {
				edm.log.Error("no question section in dnstap message, skipping parsing", "minimiser_id", minimiserID)
				continue
			}

			for i, question := range msg.Question {
				if _, ok := dns.IsDomainName(question.Name); !ok {
					edm.log.Error("question name is invalid, skipping parsing", "minimiser_id", minimiserID, "question_index", i)
					continue
				}
			}

			if edm.questionIsIgnored(msg) {
				continue
			}

			// We pass on the client address for cardinality
			// measurements.
			dawgIndex, suffixMatch, dawgModTime := wkdTracker.lookup(msg)
			if dawgIndex != dawgNotFound {
				wkdTracker.sendUpdate(dangerRealClientIP, msg, dawgIndex, suffixMatch, dawgModTime)
				continue
			}

			if !edm.qnameSeen(msg, seenQnameLRU, pdb) {
				if !edm.mqttDisabled {
					newQname := protocols.NewQnameEvent(msg, truncatedTimestamp)

					select {
					case edm.newQnamePublisherCh <- &newQname:
						edm.promNewQnameQueued.Inc()
					default:
						// If the publisher channel is full we skip creating an event.
						edm.promNewQnameDiscarded.Inc()
					}
				}
			}

			if !disableSessionFiles {
				session := edm.newSession(dt, msg, isQuery, labelLimit, timestamp)
				edm.sessionCollectorCh <- session
			}
		case <-edm.ctx.Done():
			break minimiserLoop
		}
	}
	edm.log.Info("runMinimiser: exiting loop", "minimiser_id", minimiserID)
}

func (edm *dnstapMinimiser) monitorChannelLen() {
	for {
		edm.promNewQnameChannelLen.Set(float64(len(edm.newQnamePublisherCh)))
		time.Sleep(time.Second * 1)
	}
}

func (edm *dnstapMinimiser) newSession(dt *dnstap.Dnstap, msg *dns.Msg, isQuery bool, labelLimit int, timestamp time.Time) *sessionData {
	sd := &sessionData{}

	if dt.Message.QueryPort != nil {
		if *dt.Message.QueryPort > math.MaxInt32 {
			edm.log.Error("dt.Message.QueryPort is too large for int32, setting port to 0", "value", *dt.Message.QueryPort)
			var qp int32
			sd.SourcePort = &qp
		} else {
			qp := int32(*dt.Message.QueryPort) // #nosec G115 -- QueryPort is defined as 16-bit number and is used in parquet field with type=INT32, convertedType=UINT_16, https://github.com/securego/gosec/issues/1212#issuecomment-2739574884
			sd.SourcePort = &qp
		}
	}

	if dt.Message.ResponsePort != nil {
		if *dt.Message.ResponsePort > math.MaxInt32 {
			edm.log.Error("dt.Message.ResponsePort is too large for int32, setting port to 0", "value", *dt.Message.ResponsePort)
			var rp int32
			sd.DestPort = &rp
		} else {
			rp := int32(*dt.Message.ResponsePort) // #nosec G115 -- ResponsePort is defined as 16-bit number and is used in parquet field with type=INT32, convertedType=UINT_16, https://github.com/securego/gosec/issues/1212#issuecomment-2739574884
			sd.DestPort = &rp
		}
	}

	edm.setLabels(dns.SplitDomainName(msg.Question[0].Name), labelLimit, &sd.dnsLabels)

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
				edm.log.Error("unable to create uint32 from dt.Message.QueryAddress", "error", err)
			} else {
				i32SourceIPInt := int32(sourceIPInt) // #nosec G115 -- Used in parquet struct with convertedType=UINT_32
				sd.SourceIPv4 = &i32SourceIPInt
			}
		}

		if dt.Message.ResponseAddress != nil {
			destIPInt, err := ipBytesToInt(dt.Message.ResponseAddress)
			if err != nil {
				edm.log.Error("unable to create uint32 from dt.Message.ResponseAddress", "error", err)
			} else {
				i32DestIPInt := int32(destIPInt) // #nosec G115 -- Used in parquet struct with convertedType=UINT_32
				sd.DestIPv4 = &i32DestIPInt
			}
		}
	case dnstap.SocketFamily_INET6:
		if dt.Message.QueryAddress != nil {
			sourceIPIntNetwork, sourceIPIntHost, err := ip6BytesToInt(dt.Message.QueryAddress)
			if err != nil {
				edm.log.Error("unable to create uint64 variables from dt.Message.QueryAddress", "error", err)
			} else {
				i64SourceIntNetwork := int64(sourceIPIntNetwork) // #nosec G115 -- Used in parquet struct with convertedType=UINT_64
				i64SourceIntHost := int64(sourceIPIntHost)       // #nosec G115 -- Used in parquet struct with convertedType=UINT_64
				sd.SourceIPv6Network = &i64SourceIntNetwork
				sd.SourceIPv6Host = &i64SourceIntHost
			}
		}

		if dt.Message.ResponseAddress != nil {
			dipIntNetwork, dipIntHost, err := ip6BytesToInt(dt.Message.ResponseAddress)
			if err != nil {
				edm.log.Error("unable to create uint64 variables from dt.Message.ResponseAddress", "error", err)
			} else {
				i64dIntNetwork := int64(dipIntNetwork) // #nosec G115 -- Used in parquet struct with convertedType=UINT_64
				i64dIntHost := int64(dipIntHost)       // #nosec G115 -- Used in parquet struct with convertedType=UINT_64
				sd.DestIPv6Network = &i64dIntNetwork
				sd.DestIPv6Host = &i64dIntHost
			}
		}
	default:
		edm.log.Error("packet is neither INET or INET6")
	}

	sd.DNSProtocol = (*int32)(dt.Message.SocketProtocol)

	return sd
}

func (edm *dnstapMinimiser) sessionWriter(dataDir string, wg *sync.WaitGroup) {
	defer wg.Done()

	edm.log.Info("sessionStructWriter: starting")

	for ps := range edm.sessionWriterCh {
		err := edm.writeSessionParquet(ps, dataDir)
		if err != nil {
			edm.log.Error("sessionWriter", "error", err.Error())
		}
	}

	edm.log.Info("sessionStructWriter: exiting loop")
}

func (edm *dnstapMinimiser) histogramWriter(labelLimit int, outboxDir string, wg *sync.WaitGroup) {
	defer wg.Done()

	edm.log.Info("histogramWriter: starting")

	for prevWellKnownDomainsData := range edm.histogramWriterCh {
		err := edm.writeHistogramParquet(prevWellKnownDomainsData, labelLimit, outboxDir)
		if err != nil {
			edm.log.Error("histogramWriter", "error", err.Error())
		}

	}
	edm.log.Info("histogramWriter: exiting loop")
}

func (edm *dnstapMinimiser) renameFile(src string, dst string) error {
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
			err = os.MkdirAll(dstDir, 0o750)
			if err != nil {
				return fmt.Errorf("renameFile: unable to create destination dir: %s: %w", dstDir, err)
			}
			edm.log.Info("renameFile: created directory", "dir", dstDir)
		} else {
			// Some other error occured
			return fmt.Errorf("renameFile: unable to rename file, src: %s, dst: %s: %w", src, dst, err)
		}
	}
}

func (edm *dnstapMinimiser) createFile(dst string) (*os.File, error) {
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
			err = os.MkdirAll(dstDir, 0o750)
			if err != nil {
				return nil, fmt.Errorf("createFile: unable to create destination dir: %s: %w", dstDir, err)
			}
			edm.log.Info("createFile: created directory", "dir", dstDir)
		} else {
			// Some other error occured
			return nil, fmt.Errorf("createFile: unable to create file, dst: %s: %w", dst, err)
		}
	}
}

func (edm *dnstapMinimiser) histogramSender(outboxDir string, sentDir string, wg *sync.WaitGroup) {
	defer wg.Done()

	edm.log.Info("histogramSender: starting")

	backoffDuration := time.Second * 15

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
				edm.log.Error("histogramSender: unable to read outbox dir", "error", err)
				continue
			}
			for _, dirEntry := range dirEntries {
				if dirEntry.IsDir() {
					continue
				}
				if strings.HasPrefix(dirEntry.Name(), "dns_histogram-") && strings.HasSuffix(dirEntry.Name(), ".parquet") {
					startTS, stopTS, err := timestampsFromFilename(dirEntry.Name())
					if err != nil {
						edm.log.Error("histogramSender: unable to parse timestamps from histogram filename", "error", err)
						continue
					}
					duration := stopTS.Sub(startTS)

					absPath := filepath.Join(outboxDir, dirEntry.Name())
					absPathSent := filepath.Join(sentDir, dirEntry.Name())
					err = edm.aggregSender.send(absPath, startTS, duration)
					if err != nil {
						edm.log.Error("histogramSender: unable to send histogram file", "error", err, "backoff_duration", backoffDuration)
						time.Sleep(backoffDuration)
						continue
					}
					err = edm.renameFile(absPath, absPathSent)
					if err != nil {
						edm.log.Error("histogramSender: unable to rename sent histogram file", "error", err)
					}
				}
			}
		case <-edm.ctx.Done():
			break timerLoop
		}
	}
	edm.log.Info("histogramSender: exiting loop")
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

func (edm *dnstapMinimiser) newQnamePublisher(wg *sync.WaitGroup) {
	defer wg.Done()

	edm.log.Info("newQnamePublisher: starting")

	for newQname := range edm.newQnamePublisherCh {
		newQnameJSON, err := json.Marshal(newQname)
		if err != nil {
			edm.log.Error("unable to create json for new_qname event", "error", err)
			continue
		}

		select {
		case edm.mqttPubCh <- newQnameJSON:
		case <-edm.autopahoCtx.Done():
			edm.log.Info("newQnamePublisher: the MQTT connection is shutting down, stop writing")
			// No need to break out of for loop here because
			// edm.newQnamePublisherCh is already closed in Run()
		}
	}
	close(edm.mqttPubCh)
	edm.log.Info("newQnamePublisher: exiting loop")
}

func (edm *dnstapMinimiser) parsePacket(dt *dnstap.Dnstap, isQuery bool) (*dns.Msg, time.Time) {
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
			edm.log.Error("unable to unpack query message", "error", err, "query_address", queryAddress, "response_address", responseAddress)
			msg = nil
		}
		if *dt.Message.QueryTimeSec > math.MaxInt64 {
			edm.log.Error("dt.Message.QueryTimeSec is too large for int64, setting time to 0", "value", *dt.Message.QueryTimeSec)
			*dt.Message.QueryTimeSec = 0
			*dt.Message.QueryTimeNsec = 0
		}
		t = time.Unix(int64(*dt.Message.QueryTimeSec), int64(*dt.Message.QueryTimeNsec)).UTC() // #nosec G115 -- Will be zeroed out above if too large, https://github.com/securego/gosec/issues/1212#issuecomment-2739574884
	} else {
		err = msg.Unpack(dt.Message.ResponseMessage)
		if err != nil {
			edm.log.Error("unable to unpack response message", "error", err, "query_address", queryAddress, "response_address", responseAddress)
			msg = nil
		}
		if *dt.Message.ResponseTimeSec > math.MaxInt64 {
			edm.log.Error("dt.Message.ResponseTimeSec is too large for int64, setting time to 0", "value", *dt.Message.ResponseTimeSec)
			*dt.Message.ResponseTimeSec = 0
			*dt.Message.ResponseTimeNsec = 0
		}
		t = time.Unix(int64(*dt.Message.ResponseTimeSec), int64(*dt.Message.ResponseTimeNsec)).UTC() // #nosec G115 -- Will be zeroed out above if too large, https://github.com/securego/gosec/issues/1212#issuecomment-2739574884
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

func (edm *dnstapMinimiser) writeSessionParquet(ps *prevSessions, dataDir string) error {
	// Write session file to a sessions dir where it will be read by clickhouse
	sessionsDir := filepath.Join(dataDir, "parquet", "sessions")

	startTime := getStartTimeFromRotationTime(ps.rotationTime)

	absoluteTmpFileName, absoluteFileName := buildParquetFilenames(sessionsDir, "dns_session_block", startTime, ps.rotationTime)

	absoluteTmpFileName = filepath.Clean(absoluteTmpFileName) // Make gosec happy
	edm.log.Info("writing out session parquet file", "filename", absoluteTmpFileName)

	outFile, err := edm.createFile(absoluteTmpFileName)
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
				edm.log.Error("writeSessionParquet: unable to do deferred close of histogram outFile", "error", err)
			}
		}
		if writeFailed {
			edm.log.Info("writeSessionParquet: cleaning up file because write failed", "filename", outFile.Name())
			err = os.Remove(outFile.Name())
			if err != nil {
				edm.log.Error("writeSessionParquet: unable to remove histogram outFile", "error", err, "filename", outFile.Name())
			}
		}
	}()

	snappyCodec := parquet.LookupCompressionCodec(format.Snappy)
	parquetWriter := parquet.NewGenericWriter[sessionData](outFile, sessionDataSchema, parquet.Compression(snappyCodec))

	for _, sd := range ps.sessions {
		_, err = parquetWriter.Write([]sessionData{*sd})
		if err != nil {
			writeFailed = true
			return fmt.Errorf("writeSessionParquet: unable to call Write() on parquet writer: %w", err)
		}
	}

	err = parquetWriter.Close()
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
	edm.log.Info("renaming session file", "from", absoluteTmpFileName, "to", absoluteFileName)
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

func (edm *dnstapMinimiser) writeHistogramParquet(prevWellKnownDomainsData *wellKnownDomainsData, labelLimit int, outboxDir string) error {
	if prevWellKnownDomainsData.dawgIsRotated {
		defer func() {
			err := prevWellKnownDomainsData.dawgFinder.Close()
			if err != nil {
				edm.log.Error("writeHistogramParquet: unable to close dawgFinder", "error", err)
			} else {
				edm.log.Info("closed rotated dawgFinder instance")
			}
		}()
	}

	startTime := getStartTimeFromRotationTime(prevWellKnownDomainsData.rotationTime)

	absoluteTmpFileName, absoluteFileName := buildParquetFilenames(outboxDir, "dns_histogram", startTime, prevWellKnownDomainsData.rotationTime)

	edm.log.Info("writing out histogram file", "filename", absoluteTmpFileName)

	absoluteTmpFileName = filepath.Clean(absoluteTmpFileName)
	outFile, err := edm.createFile(absoluteTmpFileName)
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
				edm.log.Error("writeHistogramParquet: unable to do deferred close of histogram outFile", "error", err)
			}
		}
		if writeFailed {
			edm.log.Info("writeHistogramParquet: cleaning up file because write failed", "filename", outFile.Name())
			err = os.Remove(outFile.Name())
			if err != nil {
				edm.log.Error("writeHistogramParquet: unable to remove histogram outFile", "error", err, "filename", outFile.Name())
			}
		}
	}()

	snappyCodec := parquet.LookupCompressionCodec(format.Snappy)
	parquetWriter := parquet.NewGenericWriter[histogramData](outFile, parquet.Compression(snappyCodec))

	startTimeMicro := startTime.UnixMicro()
	for index, hGramData := range prevWellKnownDomainsData.m {
		domain, err := prevWellKnownDomainsData.dawgFinder.AtIndex(index)
		if err != nil {
			return fmt.Errorf("writeHistogramParquet: unable to find DAWG index %d: %w", index, err)
		}

		labels := dns.SplitDomainName(domain)

		// Setting the labels now when we are out of the hot path.
		edm.setLabels(labels, labelLimit, &hGramData.dnsLabels)
		hGramData.StartTime = startTimeMicro

		// Write out the bytes from our hll data structures
		hGramData.V4ClientCountHLLBytes = string(hGramData.v4ClientHLL.ToBytes())
		hGramData.V6ClientCountHLLBytes = string(hGramData.v6ClientHLL.ToBytes())

		_, err = parquetWriter.Write([]histogramData{*hGramData})
		if err != nil {
			writeFailed = true
			return fmt.Errorf("writeHistogramParquet: unable to call Write() on parquet writer: %w", err)
		}
	}

	err = parquetWriter.Close()
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
	edm.log.Info("renaming histogram file", "from", absoluteTmpFileName, "to", absoluteFileName)
	err = os.Rename(absoluteTmpFileName, absoluteFileName)
	if err != nil {
		return fmt.Errorf("writeHistogramParquet: unable to rename output file: %w", err)
	}

	return nil
}

func edDsaJWKFromFile(fileName string) (jwk.Key, error) {
	fileName = filepath.Clean(fileName)

	keyFile, err := os.ReadFile(fileName)
	if err != nil {
		return nil, fmt.Errorf("error reading signing key file: %w", err)
	}

	jwkKey, err := jwk.ParseKey(keyFile)
	if err != nil {
		return nil, fmt.Errorf("error parsing signing jwk file: %w", err)
	}

	err = jwkKey.Set(jwk.AlgorithmKey, jwa.EdDSA)
	if err != nil {
		return nil, fmt.Errorf("error setting EdDSA algo for jwk key: %w", err)
	}

	return jwkKey, nil
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
func (edm *dnstapMinimiser) pseudonymiseDnstap(dt *dnstap.Dnstap) {
	var err error

	// Lock is used here because the cryptopan instance can get updated at runtime.
	edm.cryptopanMutex.RLock()

	if dt.Message.QueryAddress != nil {
		dt.Message.QueryAddress, err = edm.pseudonymiseIP(dt.Message.QueryAddress)
		if err != nil {
			edm.log.Error("pseudonymiseDnstap: unable to parse dt.Message.QueryAddress", "error", err)
		}
	}
	if dt.Message.ResponseAddress != nil {
		dt.Message.ResponseAddress, err = edm.pseudonymiseIP(dt.Message.ResponseAddress)
		if err != nil {
			edm.log.Error("pseudonymiseDnstap: unable to parse dt.Message.ResponseAddress", "error", err)
		}
	}
	edm.cryptopanMutex.RUnlock()
}

// Pseudonymise IP address, even on error the returned []byte is usable (zeroed address)
func (edm *dnstapMinimiser) pseudonymiseIP(ipBytes []byte) ([]byte, error) {
	addr, ok := netip.AddrFromSlice(ipBytes)
	if !ok {
		// Replace address with zeroes since we do not know if
		// the contained junk is somehow sensitive
		return make([]byte, len(ipBytes)), errors.New("unable to parse addr")
	}

	var pseudonymisedAddr netip.Addr
	var cacheHit bool

	if edm.cryptopanCache != nil {
		pseudonymisedAddr, cacheHit = edm.cryptopanCache.Get(addr)
	}

	if cacheHit {
		edm.promCryptopanCacheHit.Inc()
	} else {
		// Not in cache or cache disabled, calculate the pseudonymised IP
		pseudonymisedAddr, ok = netip.AddrFromSlice(edm.cryptopan.Anonymize(addr.AsSlice()))
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
		pseudonymisedAddr = pseudonymisedAddr.Unmap()

		if edm.cryptopanCache != nil {
			evicted := edm.cryptopanCache.Add(addr, pseudonymisedAddr)
			if evicted {
				edm.promCryptopanCacheEvicted.Inc()
			}
		}
	}

	return pseudonymisedAddr.AsSlice(), nil
}

func timeUntilNextMinute() time.Duration {
	return time.Second * time.Duration(60-time.Now().Second())
}

// runMinimiser generates data and it is collected into datasets here
func (edm *dnstapMinimiser) dataCollector(wg *sync.WaitGroup, wkd *wellKnownDomainsTracker, dawgFile string) {
	defer wg.Done()

	// Keep track of if we have recorded any dnstap packets in session data
	var sessionUpdated bool

	// Start retryer, handles instances where the received update has a
	// dawgModTime that is no longer valid becuase it has been rotated.
	var retryerWg sync.WaitGroup
	retryerWg.Add(1)
	go wkd.updateRetryer(edm, &retryerWg)

	sessions := []*sessionData{}

	ticker := time.NewTicker(timeUntilNextMinute())
	defer ticker.Stop()

	retryChannelClosed := false

collectorLoop:
	for {
		select {
		case sd := <-edm.sessionCollectorCh:
			sessions = append(sessions, sd)
			sessionUpdated = true

		case wu := <-wkd.updateCh:
			// It is possible an update sitting in the queue has
			// been created with an outdated dawgModTime due to a
			// call to rotateTracker(). If this is the case we need
			// to do a new lookup against the new dawg to make sure
			// we have the correct index number (or if it is even
			// present in the new dawg).
			if wu.dawgModTime != wkd.dawgModTime {
				if !retryChannelClosed {
					wkd.retryCh <- wu
				} else {
					edm.log.Info("discarding retry of wkd update because we are shutting down")
				}
				continue
			}

			if _, exists := wkd.m[wu.dawgIndex]; !exists {
				// We leave the label0-9 fields set to nil here. Since this is in
				// the hot path of dealing with dnstap packets the less work we do the
				// better. They are filled in prior to writing out the parquet file.
				wkd.m[wu.dawgIndex] = &histogramData{}

				esb := new(edmStatusBits)
				if wu.suffixMatch {
					esb.set(edmStatusWellKnownWildcard)
				} else {
					esb.set(edmStatusWellKnownExact)
				}
				wkd.m[wu.dawgIndex].EDMStatusBits = uint64(*esb)
			}

			wkd.m[wu.dawgIndex].OKCount += wu.OKCount
			wkd.m[wu.dawgIndex].NXCount += wu.NXCount
			wkd.m[wu.dawgIndex].FailCount += wu.FailCount
			wkd.m[wu.dawgIndex].ACount += wu.ACount
			wkd.m[wu.dawgIndex].AAAACount += wu.AAAACount
			wkd.m[wu.dawgIndex].MXCount += wu.MXCount
			wkd.m[wu.dawgIndex].NSCount += wu.NSCount
			wkd.m[wu.dawgIndex].OtherTypeCount += wu.OtherTypeCount
			wkd.m[wu.dawgIndex].OtherRcodeCount += wu.OtherRcodeCount
			wkd.m[wu.dawgIndex].NonINCount += wu.NonINCount

			if wu.ip.IsValid() {
				if wu.ip.Unmap().Is4() {
					wkd.m[wu.dawgIndex].v4ClientHLL.AddRaw(wu.hllHash)
				} else {
					wkd.m[wu.dawgIndex].v6ClientHLL.AddRaw(wu.hllHash)
				}
			}

		case ts := <-ticker.C:
			// We want to tick at the start of each minute
			ticker.Reset(timeUntilNextMinute())

			if sessionUpdated {
				ps := &prevSessions{
					sessions:     sessions,
					rotationTime: ts,
				}

				sessions = []*sessionData{}

				// We have reset the sessions slice
				sessionUpdated = false

				edm.sessionWriterCh <- ps
			}

			prevWKD, err := wkd.rotateTracker(edm, dawgFile, ts)
			if err != nil {
				edm.log.Error("unable to rotate histogram map", "error", err)
				continue
			}

			// Only write out parquet file if there is something to write
			if len(prevWKD.m) > 0 {
				edm.histogramWriterCh <- prevWKD
			}
		case <-wkd.stop:
			// Tell retryer to stop
			edm.log.Info("dataCollector: telling update retryer to stop")
			close(wkd.retryCh)
			retryChannelClosed = true
			// set stop channel to nil so we do not attempt to
			// read from it again in this select statement now that
			// it is closed.
			wkd.stop = nil
		case <-wkd.retryerDone:
			edm.log.Info("dataCollector: update retryer is done")
			break collectorLoop
		}
	}

	// Close the channels we write to
	close(edm.sessionWriterCh)
	close(edm.histogramWriterCh)

	edm.log.Info("dataCollector: exiting loop")
}

func loadDawgFile(dawgFile string) (dawg.Finder, time.Time, error) {
	dawgFileInfo, err := os.Stat(dawgFile)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("loadDawgFile: unable to stat dawg file '%s': %w", dawgFile, err)
	}

	dawgFinder, err := dawg.Load(dawgFile)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("loadDawgFile: unable to load DAWG file: %w", err)
	}

	return dawgFinder, dawgFileInfo.ModTime(), nil
}
