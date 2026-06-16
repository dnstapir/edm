package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dnstapir/edm/pkg/protocols"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/yawning/cryptopan"
	"go4.org/netipx"
)

var (
	// ErrDnstapMinimiserRunning is returned when Run is called concurrently.
	ErrDnstapMinimiserRunning = errors.New("dnstap minimiser is already running")
	// ErrDnstapMinimiserAlreadyRun is returned when Run is called after a prior run.
	ErrDnstapMinimiserAlreadyRun = errors.New("dnstap minimiser has already run")
	// ErrNilConfigProvider is returned when a nil ConfigProvider is supplied.
	ErrNilConfigProvider = errors.New("nil config provider")
	// ErrNilLogger is returned when a nil logger is supplied.
	ErrNilLogger = errors.New("nil logger")
	// ErrNilRunContext is returned when Run is called with a nil context.
	ErrNilRunContext = errors.New("nil run context")
	// ErrInvalidConfig is wrapped by every error returned from
	// [Config.Validate] so callers can match configuration validation
	// failures with [errors.Is].
	ErrInvalidConfig = errors.New("invalid configuration")

	errNoClientCertificate      = errors.New("no client certificate loaded")
	errEmptyDawgFile            = errors.New("dawg file is empty")
	errNoInputConfigured        = errors.New("no dnstap input configured")
	errMultipleInputsConfigured = errors.New("only one dnstap input may be configured")
	errNotEdDSAJWK              = errors.New("JWK is not an EdDSA (Ed25519/Ed448) key")
	errAppendCertsFromPEM       = errors.New("failed to append certs from PEM")
)

// Run lifecycle states tracked in DnstapMinimiser.state. The only
// transitions are idle → running (Run's entry CAS) and running → done
// (Run's exit), encoding the single-use contract in one atomic word.
const (
	runStateIdle int32 = iota
	runStateRunning
	runStateDone
)

// DnstapMinimiserOption customizes a [DnstapMinimiser] at construction time.
type DnstapMinimiserOption func(*dnstapMinimiserOptions)

type dnstapMinimiserOptions struct {
	deps        dependencies
	loggerLevel *slog.LevelVar
}

// WithLoggerLevel sets the mutable log level used by [DnstapMinimiser.Run].
func WithLoggerLevel(loggerLevel *slog.LevelVar) DnstapMinimiserOption {
	return func(options *dnstapMinimiserOptions) {
		options.loggerLevel = loggerLevel
	}
}

// withDependencies replaces external runner functionality.
//
// Nil fields in deps are filled with production implementations.
func withDependencies(deps dependencies) DnstapMinimiserOption {
	return func(options *dnstapMinimiserOptions) {
		options.deps = deps
	}
}

// Run starts the minimiser and blocks until it stops.
//
// Run is not reentrant. It returns startup and runtime errors directly. When
// ctx is cancelled after startup, workers drain in shutdown order and Run
// returns nil.
func (edm *DnstapMinimiser) Run(ctx context.Context) error {
	if ctx == nil {
		return ErrNilRunContext
	}

	if !edm.state.CompareAndSwap(runStateIdle, runStateRunning) {
		if edm.state.Load() == runStateRunning {
			return ErrDnstapMinimiserRunning
		}
		return ErrDnstapMinimiserAlreadyRun
	}

	ctx, stop := context.WithCancel(ctx)
	var configUpdaterWg sync.WaitGroup
	defer func() {
		stop()
		configUpdaterWg.Wait()
		edm.state.Store(runStateDone)
	}()

	// Shutdown ordering is load-bearing:
	// minimisers exit → close wkdTracker.stop → close newQnamePublisherCh →
	// (if MQTT) mqttCancel → configUpdater exits →
	// wg.Wait → (if MQTT) autopahoWg.Wait.

	// Create startConf for some initial setup. Other edm methods that need
	// to read the config should call edm.getConfig() internally so they
	// get the latest config.
	startConf := edm.getConfig()

	if startConf.DebugEnableBlockProfiling {
		edm.log.Info("enabling blocking profiling")
		runtime.SetBlockProfileRate(int(time.Millisecond))
	}
	if startConf.DebugEnableMutexProfiling {
		edm.log.Info("enabling mutex profiling")
		runtime.SetMutexProfileFraction(100)
	}

	if startConf.Debug {
		if edm.loggerLevel != nil {
			edm.loggerLevel.Set(slog.LevelDebug)
		}
	}

	// Clear any DAWG copies orphaned by a previous process before the first
	// staged load below reads from the staging directory.
	if err := edm.prepareDawgStaging(); err != nil {
		return fmt.Errorf("unable to prepare DAWG staging directory: %w", err)
	}

	if err := edm.setIgnoredClientIPs(); err != nil {
		return fmt.Errorf("unable to configure ignored client IPs: %w", err)
	}

	if err := edm.setIgnoredQuestionNames(); err != nil {
		return fmt.Errorf("unable to configure ignored question names: %w", err)
	}

	// Configuration is reloaded on SIGHUP (systemctl reload). The channel
	// buffer of 1 combined with signal.Notify's non-blocking send coalesces
	// signals arriving while a reload is already in progress.
	hupCh := make(chan os.Signal, 1)
	signal.Notify(hupCh, syscall.SIGHUP)
	defer signal.Stop(hupCh)

	configUpdaterWg.Add(1)
	go func() {
		defer configUpdaterWg.Done()
		configUpdater(ctx, hupCh, edm)
	}()

	pdbDir := filepath.Join(startConf.DataDir, "pebble")
	seenStore, err := edm.deps.SeenQnameStoreFactory.OpenSeenQnameStore(pdbDir)
	if err != nil {
		return fmt.Errorf("unable to open pebble database %q: %w", pdbDir, err)
	}
	defer func() {
		if err := seenStore.Close(); err != nil {
			edm.log.Error("unable to close pebble database", "error", err)
		}
	}()

	if !startConf.DisableHistogramSender {
		if err := edm.loadHTTPClientCert(); err != nil {
			return fmt.Errorf("unable to load x509 HTTP client cert: %w", err)
		}

		if err := edm.setupHistogramSender(); err != nil {
			return fmt.Errorf("unable to setup histogram sender: %w", err)
		}
	}

	var mqttCtx context.Context
	var mqttCancel context.CancelFunc
	if !startConf.DisableMQTT {
		if err := edm.loadMQTTClientCert(); err != nil {
			return fmt.Errorf("unable to load x509 mqtt client cert: %w", err)
		}

		// The MQTT pipeline context is deliberately detached from Run's
		// ctx: cancelling Run must not kill the publisher before the
		// minimisers have drained and newQnamePublisherCh has been closed
		// (see the shutdown ordering above). The pipeline is stopped by the
		// explicit mqttCancel() on the shutdown path below; the deferred
		// mqttCancel() covers the error returns in between.
		mqttCtx, mqttCancel = context.WithCancel(context.Background())
		if err := edm.setupMQTT(mqttCtx); err != nil {
			mqttCancel()
			return fmt.Errorf("unable to setup mqtt: %w", err)
		}
		defer mqttCancel()
	}

	dti, err := edm.setupDnstapInput(edm.log, startConf)
	if err != nil {
		return fmt.Errorf("unable to setup dnstap input: %w", err)
	}
	defer func() {
		if err := dti.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			edm.log.Error("Run: dnstap input close error", "error", err)
		}
	}()

	// We need to keep track of domains that are not on the well-known
	// domain list yet we have seen since we started. To limit the
	// possibility of unbounded memory usage we use a LRU cache instead of
	// something simpler like a map.
	seenQnameLRU, err := lru.New[string, struct{}](startConf.QnameSeenEntries)
	if err != nil {
		return fmt.Errorf("unable to create seen-qname LRU: %w", err)
	}

	var wg sync.WaitGroup

	// The HTTP servers are tracked in their own WaitGroup so the deferred
	// cleanup below can stop them and wait for their goroutines on every
	// return path, including the early error returns during setup.
	var serverWg sync.WaitGroup

	pprofServer := newPprofServer(edm.deps.PprofListenAddr)
	serverWg.Add(1)
	go func() {
		defer serverWg.Done()
		err := edm.deps.HTTPServerRunner.ListenAndServeHTTP(pprofServer)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			edm.log.Error("pprofServer error", "error", err)
		}
	}()

	metricsServer := edm.newMetricsServer(ctx, edm.deps.MetricsListenAddr, startConf.EnableManualParquetRotation)
	serverWg.Add(1)
	go func() {
		defer serverWg.Done()
		err := edm.deps.HTTPServerRunner.ListenAndServeHTTP(metricsServer)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			edm.log.Error("metricsServer error", "error", err)
		}
	}()

	// Gracefully shut down both HTTP servers whenever Run returns (early
	// error paths included), giving each its own deadline so the second
	// shutdown never inherits an exhausted context, then wait for the
	// listener goroutines to exit.
	defer func() {
		pprofCtx, pprofCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := pprofServer.Shutdown(pprofCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			edm.log.Error("pprofServer shutdown error", "error", err)
		}
		pprofCancel()

		metricsCtx, metricsCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := metricsServer.Shutdown(metricsCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			edm.log.Error("metricsServer shutdown error", "error", err)
		}
		metricsCancel()

		serverWg.Wait()
	}()

	// Write histogram file to an outbox dir where it will get picked up by
	// the histogram sender. Upon being sent it will be moved to the sent dir.
	dataDir := startConf.DataDir
	outboxDir := filepath.Join(dataDir, "parquet", "histograms", "outbox")
	sentDir := filepath.Join(dataDir, "parquet", "histograms", "sent")

	wg.Add(1)
	go edm.monitorChannelLen(ctx, &wg)

	// Start record writers and data senders in the background
	wg.Add(1)
	go edm.sessionWriter(dataDir, &wg)
	wg.Add(1)
	go edm.histogramWriter(defaultLabelLimit, outboxDir, &wg)
	wg.Add(1)
	go edm.histogramSender(ctx, outboxDir, sentDir, &wg)
	if !startConf.DisableMQTT {
		wg.Add(1)
		go edm.newQnamePublisher(mqttCtx, &wg)
	}

	wg.Add(1)
	go edm.diskCleaner(ctx, &wg, sentDir)

	dawgFile := startConf.WellKnownDomainsFile

	dawgFinder, dawgModTime, err := edm.loadDawgFileStaged(dawgFile)
	if err != nil {
		return fmt.Errorf("DawgLoader.LoadDawgFile failed: %w", err)
	}

	wkdTracker, err := newWellKnownDomainsTracker(dawgFinder, dawgModTime)
	if err != nil {
		return fmt.Errorf("newWellKnownDomainsTracker failed: %w", err)
	}

	debugDnstapFilename := startConf.DebugDnstapFilename

	// Keep in mind that this file is unbuffered. We could wrap it in a
	// bufio.NewWriter() if we want more performance out of it, but since
	// it is meant for debugging purposes it is probably better to keep it
	// unbuffered and more "reactive". Otherwise it is hard to be sure if
	// you are not seeing anything in the log because packets are being
	// missed, or you are just waiting on the buffer to be flushed.
	var debugDnstapFile fsFile
	if debugDnstapFilename != "" {
		// Make gosec happy
		debugDnstapFilename := filepath.Clean(debugDnstapFilename)
		debugDnstapFile, err = edm.deps.FileSystem.OpenFile(debugDnstapFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			return fmt.Errorf("unable to open debug dnstap file %q: %w", debugDnstapFilename, err)
		}
		defer func() {
			if err := debugDnstapFile.Close(); err != nil {
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

	// Per-worker Crypto-PAn caches. Each worker holds its own LRU so the
	// pseudonymise hot path takes no shared lock. Created before any
	// worker starts so a creation failure is a startup error instead of a
	// silently dead worker.
	cryptopanCaches := make([]*lru.Cache[netip.Addr, netip.Addr], numMinimiserWorkers)
	if startConf.CryptopanAddressEntries != 0 {
		for i := range cryptopanCaches {
			cryptopanCaches[i], err = lru.New[netip.Addr, netip.Addr](startConf.CryptopanAddressEntries)
			if err != nil {
				return fmt.Errorf("unable to create per-worker cryptopan cache: %w", err)
			}
		}
	}

	// Start minimiser
	edm.reloadMinimiserMutex.Lock()
	for minimiserID := 0; minimiserID < numMinimiserWorkers; minimiserID++ {
		edm.log.Info("Run: starting minimiser worker", "minimiser_id", minimiserID)
		// This append aims to map 1:1 between offset in the resulting
		// slice and minimiserID of a worker, so edm.reloadMinimiserConfigCh[3]
		// should be a channel sending to the worker with minimiserID 3.
		// The channel is also handed to the worker directly so it never
		// reads the slice, which Run is still appending to.
		//
		// Capacity is set to 1 so we can do a send without hanging if
		// the worker is currently busy.
		reloadConfigCh := make(chan struct{}, 1)
		edm.reloadMinimiserConfigCh = append(edm.reloadMinimiserConfigCh, reloadConfigCh)
		minimiserWg.Add(1)
		go edm.runMinimiser(ctx, minimiserID, &minimiserWg, reloadConfigCh, cryptopanCaches[minimiserID], seenQnameLRU, seenStore, debugDnstapFile, defaultLabelLimit, wkdTracker)
	}
	edm.reloadMinimiserMutex.Unlock()

	// The single producer goroutine below is synchronized by
	// dnstapInputWg.Wait(), which makes its dnstapInputErr write visible
	// before the error is read on the shutdown path.
	var dnstapInputErr error
	var dnstapInputWg sync.WaitGroup
	dnstapInputWg.Add(1)
	go func() {
		defer dnstapInputWg.Done()
		if err := dti.ReadInto(ctx, edm.inputChannel); err != nil {
			dnstapInputErr = err
			stop()
		}
	}()

	// Wait here until all instances of runMinimiser() is done
	minimiserWg.Wait()
	dnstapInputWg.Wait()

	// Tell collector it is time to stop reading data
	close(wkdTracker.stop)

	// Make sure writers have completed their work
	close(edm.newQnamePublisherCh)

	// Stop the MQTT publisher
	if !startConf.DisableMQTT {
		edm.log.Info("Run: stopping MQTT publisher")
		mqttCancel()
	}

	// Wait out any in-flight config update so a SIGHUP arriving during
	// shutdown never reloads state that later teardown steps are releasing.
	// The deferred signal.Stop detaches hupCh afterwards; signals delivered
	// past this point are ignored.
	configUpdaterWg.Wait()

	// Wait for all workers to exit. The HTTP servers are shut down by the
	// deferred cleanup registered when they were started.
	edm.log.Info("Run: waiting for other workers to exit")
	wg.Wait()

	// Wait for graceful disconnection from MQTT bus
	if !startConf.DisableMQTT {
		edm.log.Info("Run: waiting on MQTT disconnection")
		edm.autopahoWg.Wait()
	}

	if dnstapInputErr != nil &&
		!errors.Is(dnstapInputErr, context.Canceled) &&
		!errors.Is(dnstapInputErr, context.DeadlineExceeded) {
		return fmt.Errorf("dnstap input failed: %w", dnstapInputErr)
	}
	return nil
}

// DnstapMinimiser runs the Edge DNSTAP Minimiser service.
//
// Construct instances with [NewDnstapMinimiser]. A DnstapMinimiser is
// single-use: one instance supports exactly one [DnstapMinimiser.Run]
// lifecycle. Concurrent Run calls return [ErrDnstapMinimiserRunning]; calls
// after a prior Run has started return [ErrDnstapMinimiserAlreadyRun].
type DnstapMinimiser struct {
	configer     ConfigProvider
	conf         Config
	confMutex    sync.RWMutex
	deps         dependencies
	loggerLevel  *slog.LevelVar
	state        atomic.Int32 // run lifecycle: runStateIdle → runStateRunning → runStateDone
	inputChannel chan []byte  // the channel passed to DNSTAP input readers
	log          *slog.Logger // any information logging is sent here

	// Cryptopan instance is held in an atomic.Pointer so the hot path
	// reads it without locking. setCryptopan swaps the pointer and
	// bumps cryptopanGen; per-worker caches compare their last-seen
	// generation against this and Purge when it changes.
	cryptopan                 atomic.Pointer[cryptopan.Cryptopan]
	cryptopanGen              atomic.Uint64
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
	promDNSParseError         prometheus.Counter
	promEmptyQuestionSection  prometheus.Counter
	promInvalidQuestionName   prometheus.Counter
	debug                     bool // if we should print debug messages during operation
	sessionWriterCh           chan *prevSessions
	histogramWriterCh         chan *wellKnownDomainsData
	parquetRotationRequestCh  chan parquetRotationRequest
	newQnamePublisherCh       chan *protocols.NewQnameJSON
	sessionCollectorCh        chan *sessionData
	aggregSenderMutex         sync.RWMutex
	aggregSender              aggregateSender
	mqttPubCh                 chan []byte
	mqttSignedCh              chan []byte
	autopahoWg                sync.WaitGroup
	// Hot-path lookups (clientIPIsIgnored, questionIsIgnored) read these
	// without locking. Reload writers atomic.Store a fresh value and leave the
	// old value for the GC to reclaim. For ignoredQuestions the dawgFinderHolder
	// wrapper is needed because dawg.Finder is an interface and atomic.Pointer
	// wants a concrete type; its old finder is deliberately NOT Close()d on
	// swap, since Close() (munmap) would race with hot-path readers still
	// holding the old pointer. Reloads are rare and the mmap is small, so that
	// bounded leak is acceptable. ignoredClientsIPSet is plain heap memory with
	// no Close, reclaimed by the GC like any other dropped pointer.
	ignoredClientsIPSet           atomic.Pointer[netipx.IPSet]
	ignoredClientCIDRsParsed      atomic.Uint64
	ignoredQuestions              atomic.Pointer[dawgFinderHolder]
	dawgReloadRequested           atomic.Bool // set on SIGHUP, consumed by rotateTracker
	httpClientCertStore           *certStore  // client cert/key for mTLS authentication
	mqttClientCertStore           *certStore  // client cert/key for mTLS authentication
	reloadMinimiserMutex          sync.RWMutex
	reloadMinimiserConfigCh       []chan struct{}
	reloadHistogramSenderConfigCh chan struct{}
	seenQnameMutex                sync.Mutex
}

// NewDnstapMinimiser constructs a DnstapMinimiser.
//
// The returned service is ready to run but has no active run context until
// [DnstapMinimiser.Run] is called.
func NewDnstapMinimiser(provider ConfigProvider, logger *slog.Logger, opts ...DnstapMinimiserOption) (*DnstapMinimiser, error) {
	if provider == nil {
		return nil, ErrNilConfigProvider
	}
	if logger == nil {
		return nil, ErrNilLogger
	}

	var options dnstapMinimiserOptions
	for _, opt := range opts {
		opt(&options)
	}
	options.deps = fillDependencies(options.deps)

	edm := &DnstapMinimiser{
		configer:    provider,
		deps:        options.deps,
		loggerLevel: options.loggerLevel,
	}

	err := edm.updateConfig()
	if err != nil {
		return nil, fmt.Errorf("NewDnstapMinimiser: unable to set config: %w", err)
	}

	conf := edm.getConfig()

	err = edm.setCryptopan(conf.CryptopanKey, conf.CryptopanKeySalt, conf.CryptopanAddressEntries)
	if err != nil {
		return nil, fmt.Errorf("NewDnstapMinimiser: %w", err)
	}

	// Use separate prometheus registry for each edm instance, otherwise
	// trying to run tests where each test do their own call to
	// NewDnstapMinimiser() will panic:
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

	edm.promDNSParseError = promauto.With(promReg).NewCounter(prometheus.CounterOpts{
		Name: "edm_ignored_dns_parse_error_total",
		Help: "The total number of times we have ignored a dnstap packet because we were unable to parse the DNS data",
	})

	edm.promEmptyQuestionSection = promauto.With(promReg).NewCounter(prometheus.CounterOpts{
		Name: "edm_ignored_empty_question_section_total",
		Help: "The total number of times we have ignored a dnstap packet because it had no entries in the question section",
	})

	edm.promInvalidQuestionName = promauto.With(promReg).NewCounter(prometheus.CounterOpts{
		Name: "edm_ignored_invalid_question_name_total",
		Help: "The total number of times we have ignored a dnstap packet because it contained an invalid name in the question section",
	})

	edm.promReg = promReg
	// Buffer enough frames to absorb scheduling jitter under high QPS.
	// A 1024-frame buffer keeps producers from stalling without growing
	// memory meaningfully for typical dnstap frame sizes.
	edm.inputChannel = make(chan []byte, 1024)
	edm.log = logger
	edm.debug = conf.Debug

	// Capacity is set to 1 so we can do a send without hanging if
	// the histogram sender is currently busy.
	edm.reloadHistogramSenderConfigCh = make(chan struct{}, 1)

	// Prepare client cert/key storage for mTLS authentication
	edm.httpClientCertStore = newCertStore()
	edm.mqttClientCertStore = newCertStore()

	// Setup channels for the MQTT publish pipeline. mqttPubCh holds
	// unsigned events from minimisers; mqttSignedCh holds signed
	// envelopes ready for paho to publish.
	edm.mqttPubCh = make(chan []byte, 1024)
	edm.mqttSignedCh = make(chan []byte, 1024)

	// Setup channels for feeding writers and data senders that should do
	// their work outside the main minimiser loop. They are buffered to
	// to not block the loop if writing/sending data is slow.
	// NOTE: Remember to close all of these channels at the end of the
	// minimiser loop, otherwise the program can hang on shutdown.
	edm.sessionWriterCh = make(chan *prevSessions, 100)
	edm.histogramWriterCh = make(chan *wellKnownDomainsData, 100)
	edm.parquetRotationRequestCh = make(chan parquetRotationRequest, 1)
	edm.newQnamePublisherCh = make(chan *protocols.NewQnameJSON, conf.NewQnameBuffer)
	edm.sessionCollectorCh = make(chan *sessionData, 100)

	return edm, nil
}

func (edm *DnstapMinimiser) monitorChannelLen(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := edm.deps.Clock.NewTicker(edm.deps.MonitorChannelInterval)
	defer ticker.Stop()

	edm.log.Info("monitorChannelLen: starting")
	for {
		select {
		case <-ticker.C():
			edm.promNewQnameChannelLen.Set(float64(len(edm.newQnamePublisherCh)))
		case <-ctx.Done():
			edm.log.Info("monitorChannelLen: exiting loop")
			return
		}
	}
}
