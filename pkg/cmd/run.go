package cmd

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/dnstapir/edm/pkg/runner"
)

// newRunFlagSet builds the flagset for the "run" subcommand.
//
// Flag values are bound directly to fields of conf, which is first reset to
// [runner.DefaultConfig] so the flag defaults and the provider's base layer
// cannot diverge.
func newRunFlagSet(conf *runner.Config) (fs *flag.FlagSet) {
	*conf = runner.DefaultConfig()
	fs = flag.NewFlagSet("run", flag.ContinueOnError)

	fs.StringVar(&conf.ConfigFile, "config-file", "", "config file for sensitive information (default is $HOME/.dnstapir-edm.toml)")

	fs.BoolVar(&conf.DisableSessionFiles, "disable-session-files", conf.DisableSessionFiles, "do not write out session parquet files")
	fs.BoolVar(&conf.DisableHistogramSender, "disable-histogram-sender", conf.DisableHistogramSender, "do not check for histogram files to upload to core")
	fs.BoolVar(&conf.DisableMQTT, "disable-mqtt", conf.DisableMQTT, "disable MQTT message sending")
	fs.BoolVar(&conf.DisableMQTTFilequeue, "disable-mqtt-filequeue", conf.DisableMQTTFilequeue, "disable MQTT file based queue")
	fs.BoolVar(&conf.EnableManualParquetRotation, "enable-manual-parquet-rotation", conf.EnableManualParquetRotation, "enable localhost HTTP endpoint for manually rotating session and histogram parquet files")
	fs.BoolVar(&conf.PebbleSync, "pebble-sync", conf.PebbleSync, "fsync seen-qname pebble writes")

	fs.StringVar(&conf.InputUnix, "input-unix", conf.InputUnix, "create unix socket for reading dnstap (e.g. /var/lib/unbound/dnstap.sock)")
	fs.StringVar(&conf.InputTCP, "input-tcp", conf.InputTCP, "create TCP socket for reading dnstap (e.g. '127.0.0.1:53535')")
	fs.StringVar(&conf.InputTLS, "input-tls", conf.InputTLS, "create TLS TCP socket for reading dnstap (e.g. '127.0.0.1:53535')")
	fs.StringVar(&conf.InputTLSCertFile, "input-tls-cert-file", conf.InputTLSCertFile, "file containing cert used for TLS TCP socket")
	fs.StringVar(&conf.InputTLSKeyFile, "input-tls-key-file", conf.InputTLSKeyFile, "file containing key used for TLS TCP socket")
	fs.StringVar(&conf.InputTLSClientCAFile, "input-tls-client-ca-file", conf.InputTLSClientCAFile, "file containing CA used for client cert allowed to connect to TLS TCP socket")

	fs.StringVar(&conf.CryptopanKey, "cryptopan-key", conf.CryptopanKey, "override the secret used for Crypto-PAn pseudonymization")
	fs.StringVar(&conf.CryptopanKeySalt, "cryptopan-key-salt", conf.CryptopanKeySalt, "the salt used for key derivation")
	fs.StringVar(&conf.WellKnownDomainsFile, "well-known-domains-file", conf.WellKnownDomainsFile, "the DAWG file used for filtering well-known domains")
	fs.StringVar(&conf.IgnoredClientIPsFile, "ignored-client-ips-file", conf.IgnoredClientIPsFile, "file containing a newline separated list of IPv4/IPv6 CIDRs of DNS clients that will be ignored")
	fs.StringVar(&conf.IgnoredQuestionNamesFile, "ignored-question-names-file", conf.IgnoredQuestionNamesFile, "a DAWG file containing question section names that will be ignored")
	fs.StringVar(&conf.DataDir, "data-dir", conf.DataDir, "directory where output data is written")
	fs.IntVar(&conf.MinimiserWorkers, "minimiser-workers", conf.MinimiserWorkers, "how many minimiser workers to start (0 means same as GOMAXPROCS)")

	fs.StringVar(&conf.MQTTSigningKeyFile, "mqtt-signing-key-file", conf.MQTTSigningKeyFile, "ECSDSA key used for signing MQTT messages")
	fs.StringVar(&conf.MQTTClientKeyFile, "mqtt-client-key-file", conf.MQTTClientKeyFile, "ECSDSA client key used for authenticating to MQTT bus")
	fs.StringVar(&conf.MQTTClientCertFile, "mqtt-client-cert-file", conf.MQTTClientCertFile, "ECSDSA client cert used for authenticating to MQTT bus")
	fs.StringVar(&conf.MQTTServer, "mqtt-server", conf.MQTTServer, "MQTT server we will publish events to")
	fs.StringVar(&conf.MQTTCAFile, "mqtt-ca-file", conf.MQTTCAFile, "CA cert used for validating MQTT TLS connection, defaults to using OS CA certs")
	// Stdlib flag has no Uint16Var; parse mqtt-keepalive by hand. The
	// default is already set by DefaultConfig above.
	fs.Func("mqtt-keepalive", fmt.Sprintf("Keepalive interval for MQTT connection (default %d)", conf.MQTTKeepalive), func(s string) error {
		v, err := strconv.ParseUint(s, 10, 16)
		if err == nil {
			conf.MQTTKeepalive = uint16(v)
		}
		return err
	})

	fs.IntVar(&conf.QnameSeenEntries, "qname-seen-entries", conf.QnameSeenEntries, "Number of 'seen' qnames stored in LRU cache, need to be changed based on RAM")
	fs.IntVar(&conf.CryptopanAddressEntries, "cryptopan-address-entries", conf.CryptopanAddressEntries, "Number of cryptopan pseudonymised addresses stored in LRU cache, 0 disables the cache, need to be changed based on RAM")
	fs.IntVar(&conf.NewQnameBuffer, "newqname-buffer", conf.NewQnameBuffer, "Number of slots in new_qname publisher channel, if this is filled up we skip new_qname events")
	fs.IntVar(&conf.HistogramHLLExplicitThreshold, "histogram-hll-explicit-threshold", conf.HistogramHLLExplicitThreshold, "When the number of unique IP addresses is beyond this threshold we will include HLL data for a domain in the histogram parquet file")

	fs.StringVar(&conf.HTTPCAFile, "http-ca-file", conf.HTTPCAFile, "CA cert used for validating aggregate-receiver connection, defaults to using OS CA certs")
	fs.StringVar(&conf.HTTPSigningKeyFile, "http-signing-key-file", conf.HTTPSigningKeyFile, "ECSDSA key used for signing HTTP messages to aggregate-receiver")
	fs.StringVar(&conf.HTTPClientKeyFile, "http-client-key-file", conf.HTTPClientKeyFile, "ECSDSA client key used for authenticating to aggregate-receiver")
	fs.StringVar(&conf.HTTPClientCertFile, "http-client-cert-file", conf.HTTPClientCertFile, "ECSDSA client cert used for authenticating to aggregate-receiver")
	fs.StringVar(&conf.HTTPURL, "http-url", conf.HTTPURL, "Service we will POST aggregates to")

	fs.BoolVar(&conf.Debug, "debug", conf.Debug, "print debug logging during operation")
	fs.StringVar(&conf.DebugDnstapFilename, "debug-dnstap-filename", conf.DebugDnstapFilename, "File for dumping unmodified (sensitive) JSON-formatted dnstap packets we are about to process, for debugging")
	fs.BoolVar(&conf.DebugEnableBlockProfiling, "debug-enable-blockprofiling", conf.DebugEnableBlockProfiling, "Enable profiling of goroutine blocking events")
	fs.BoolVar(&conf.DebugEnableMutexProfiling, "debug-enable-mutexprofiling", conf.DebugEnableMutexProfiling, "Enable profiling of mutex contention events")

	return
}

// envName returns the environment variable that overrides flagName:
// [envPrefix] + "_" + the flag name upper-cased with "-" replaced by "_".
func envName(flagName string) string {
	return envPrefix + "_" + strings.ToUpper(strings.ReplaceAll(flagName, "-", "_"))
}

// applyEnvOverrides applies DNSTAPIR_EDM_* environment variables to fs.
//
// It must run before fs.Parse so flags given on the command line overwrite
// values taken from the environment. An environment value that fails the
// flag's parser is reported as an error naming the variable.
//
// "config-file" is excluded: it selects the config path rather than a config
// value, and the path is resolved from the run/root flags alone (see
// [buildRunProvider]). The environment never selects the config file, so an
// explicit --config-file is never shadowed by an env var.
func applyEnvOverrides(fs *flag.FlagSet) (err error) {
	fs.VisitAll(func(f *flag.Flag) {
		if err != nil || f.Name == "config-file" {
			return
		}
		if val, ok := os.LookupEnv(envName(f.Name)); ok {
			if setErr := fs.Set(f.Name, val); setErr != nil {
				err = fmt.Errorf("invalid value in environment variable %s: %w", envName(f.Name), setErr)
			}
		}
	})
	return
}

// overrideFor returns a [runner.ConfigOverride] copying the named flag's
// value from src.
//
// It returns nil for "config-file" (handled via path resolution, not as an
// overlay) and for names without a mapping; a test walking every registered
// flag guards the switch against drifting from newRunFlagSet.
func overrideFor(name string, src *runner.Config) runner.ConfigOverride {
	switch name {
	case "disable-session-files":
		return func(c *runner.Config) { c.DisableSessionFiles = src.DisableSessionFiles }
	case "disable-histogram-sender":
		return func(c *runner.Config) { c.DisableHistogramSender = src.DisableHistogramSender }
	case "disable-mqtt":
		return func(c *runner.Config) { c.DisableMQTT = src.DisableMQTT }
	case "disable-mqtt-filequeue":
		return func(c *runner.Config) { c.DisableMQTTFilequeue = src.DisableMQTTFilequeue }
	case "enable-manual-parquet-rotation":
		return func(c *runner.Config) { c.EnableManualParquetRotation = src.EnableManualParquetRotation }
	case "pebble-sync":
		return func(c *runner.Config) { c.PebbleSync = src.PebbleSync }
	case "input-unix":
		return func(c *runner.Config) { c.InputUnix = src.InputUnix }
	case "input-tcp":
		return func(c *runner.Config) { c.InputTCP = src.InputTCP }
	case "input-tls":
		return func(c *runner.Config) { c.InputTLS = src.InputTLS }
	case "input-tls-cert-file":
		return func(c *runner.Config) { c.InputTLSCertFile = src.InputTLSCertFile }
	case "input-tls-key-file":
		return func(c *runner.Config) { c.InputTLSKeyFile = src.InputTLSKeyFile }
	case "input-tls-client-ca-file":
		return func(c *runner.Config) { c.InputTLSClientCAFile = src.InputTLSClientCAFile }
	case "cryptopan-key":
		return func(c *runner.Config) { c.CryptopanKey = src.CryptopanKey }
	case "cryptopan-key-salt":
		return func(c *runner.Config) { c.CryptopanKeySalt = src.CryptopanKeySalt }
	case "well-known-domains-file":
		return func(c *runner.Config) { c.WellKnownDomainsFile = src.WellKnownDomainsFile }
	case "ignored-client-ips-file":
		return func(c *runner.Config) { c.IgnoredClientIPsFile = src.IgnoredClientIPsFile }
	case "ignored-question-names-file":
		return func(c *runner.Config) { c.IgnoredQuestionNamesFile = src.IgnoredQuestionNamesFile }
	case "data-dir":
		return func(c *runner.Config) { c.DataDir = src.DataDir }
	case "minimiser-workers":
		return func(c *runner.Config) { c.MinimiserWorkers = src.MinimiserWorkers }
	case "mqtt-signing-key-file":
		return func(c *runner.Config) { c.MQTTSigningKeyFile = src.MQTTSigningKeyFile }
	case "mqtt-client-key-file":
		return func(c *runner.Config) { c.MQTTClientKeyFile = src.MQTTClientKeyFile }
	case "mqtt-client-cert-file":
		return func(c *runner.Config) { c.MQTTClientCertFile = src.MQTTClientCertFile }
	case "mqtt-server":
		return func(c *runner.Config) { c.MQTTServer = src.MQTTServer }
	case "mqtt-ca-file":
		return func(c *runner.Config) { c.MQTTCAFile = src.MQTTCAFile }
	case "mqtt-keepalive":
		return func(c *runner.Config) { c.MQTTKeepalive = src.MQTTKeepalive }
	case "qname-seen-entries":
		return func(c *runner.Config) { c.QnameSeenEntries = src.QnameSeenEntries }
	case "cryptopan-address-entries":
		return func(c *runner.Config) { c.CryptopanAddressEntries = src.CryptopanAddressEntries }
	case "newqname-buffer":
		return func(c *runner.Config) { c.NewQnameBuffer = src.NewQnameBuffer }
	case "histogram-hll-explicit-threshold":
		return func(c *runner.Config) { c.HistogramHLLExplicitThreshold = src.HistogramHLLExplicitThreshold }
	case "http-ca-file":
		return func(c *runner.Config) { c.HTTPCAFile = src.HTTPCAFile }
	case "http-signing-key-file":
		return func(c *runner.Config) { c.HTTPSigningKeyFile = src.HTTPSigningKeyFile }
	case "http-client-key-file":
		return func(c *runner.Config) { c.HTTPClientKeyFile = src.HTTPClientKeyFile }
	case "http-client-cert-file":
		return func(c *runner.Config) { c.HTTPClientCertFile = src.HTTPClientCertFile }
	case "http-url":
		return func(c *runner.Config) { c.HTTPURL = src.HTTPURL }
	case "debug":
		return func(c *runner.Config) { c.Debug = src.Debug }
	case "debug-dnstap-filename":
		return func(c *runner.Config) { c.DebugDnstapFilename = src.DebugDnstapFilename }
	case "debug-enable-blockprofiling":
		return func(c *runner.Config) { c.DebugEnableBlockProfiling = src.DebugEnableBlockProfiling }
	case "debug-enable-mutexprofiling":
		return func(c *runner.Config) { c.DebugEnableMutexProfiling = src.DebugEnableMutexProfiling }
	}
	return nil
}

// buildRunProvider parses the run command's flags plus environment overrides
// and returns the configured provider.
//
// rootCfgFile is a --config-file given before the subcommand; run's own
// --config-file flag wins over it, and neither is settable via the
// environment (see [applyEnvOverrides]). For every other flag the environment
// is applied to the flagset before fs.Parse so the precedence is CLI flag
// over environment variable over config file over flag default. fs.Visit
// afterwards enumerates exactly the union of env-set and CLI-set flags, which
// becomes the immutable override layer re-applied on every config reload.
func buildRunProvider(args []string, rootCfgFile string, errW io.Writer) (provider *runner.FileConfigProvider, err error) {
	// flagConf escapes into the override closures, which outlive this call.
	flagConf := new(runner.Config)
	fs := newRunFlagSet(flagConf)
	fs.SetOutput(errW)

	// fs.Parse reports parse errors (and usage) on errW itself; the other
	// failures below do not, so they are reported at the end to avoid a
	// silent non-zero exit. parseReported tracks the one already-reported
	// path so it is not printed twice.
	parseReported := false
	err = applyEnvOverrides(fs)
	if err == nil {
		if perr := fs.Parse(args); perr != nil {
			err = perr
			parseReported = true
		}
	}
	if err == nil && fs.NArg() > 0 {
		err = fmt.Errorf("unexpected argument(s): %q", fs.Args())
	}

	var path string
	if err == nil {
		explicit := flagConf.ConfigFile
		if explicit == "" {
			explicit = rootCfgFile
		}
		path, err = resolveConfigPath(explicit)
	}

	if err == nil {
		var overrides []runner.ConfigOverride
		fs.Visit(func(f *flag.Flag) {
			if o := overrideFor(f.Name, flagConf); o != nil {
				overrides = append(overrides, o)
			}
		})
		provider = runner.NewFileConfigProvider(path, overrides...)
	}

	if err != nil && !parseReported && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(errW, err)
	}
	return
}

// runRun implements the "run" subcommand.
func runRun(args []string, rootCfgFile string, errW io.Writer) (err error) {
	var provider *runner.FileConfigProvider
	provider, err = buildRunProvider(args, rootCfgFile, errW)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err == nil {
		edmLogger.Info("using config file", "filename", provider.Path())
		err = runMinimiser(provider)
	}
	return
}

// runMinimiser constructs the minimiser from provider and runs it until
// SIGINT or SIGTERM. Errors are logged before being returned.
func runMinimiser(provider runner.ConfigProvider) (err error) {
	var edm *runner.DnstapMinimiser
	edm, err = runner.NewDnstapMinimiser(provider, edmLogger, runner.WithLoggerLevel(edmLoggerLevel))
	if err != nil {
		edmLogger.Error("unable to init", "error", err)
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	err = edm.Run(ctx)
	if err != nil {
		edmLogger.Error("edm: run failed", "error", err)
	}
	return
}
