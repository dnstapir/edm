package runner

import (
	"errors"
	"fmt"

	"github.com/spf13/viper"
)

// Labels 0-9
const defaultLabelLimit = 10

// Config contains all runtime configuration for [DnstapMinimiser].
//
// The struct tags are part of the Viper-backed CLI contract and stay in sync
// with the flags in pkg/cmd. Validation rules are enforced by
// [Config.Validate].
type Config struct {
	ConfigFile                    string `mapstructure:"config-file"`
	DisableSessionFiles           bool   `mapstructure:"disable-session-files"`
	DisableHistogramSender        bool   `mapstructure:"disable-histogram-sender" reload:"true"`
	DisableMQTT                   bool   `mapstructure:"disable-mqtt"`
	DisableMQTTFilequeue          bool   `mapstructure:"disable-mqtt-filequeue"`
	EnableManualParquetRotation   bool   `mapstructure:"enable-manual-parquet-rotation"`
	PebbleSync                    bool   `mapstructure:"pebble-sync" reload:"true"`
	InputUnix                     string `mapstructure:"input-unix"`
	InputTCP                      string `mapstructure:"input-tcp"`
	InputTLS                      string `mapstructure:"input-tls"`
	InputTLSCertFile              string `mapstructure:"input-tls-cert-file"`
	InputTLSKeyFile               string `mapstructure:"input-tls-key-file"`
	InputTLSClientCAFile          string `mapstructure:"input-tls-client-ca-file"`
	CryptopanKey                  string `mapstructure:"cryptopan-key" reload:"true"`
	CryptopanKeySalt              string `mapstructure:"cryptopan-key-salt" reload:"true"`
	WellKnownDomainsFile          string `mapstructure:"well-known-domains-file"`
	HistogramHLLExplicitThreshold int    `mapstructure:"histogram-hll-explicit-threshold"`
	IgnoredClientIPsFile          string `mapstructure:"ignored-client-ips-file" reload:"true"`
	IgnoredQuestionNamesFile      string `mapstructure:"ignored-question-names-file" reload:"true"`
	DataDir                       string `mapstructure:"data-dir"`
	MinimiserWorkers              int    `mapstructure:"minimiser-workers"`
	MQTTSigningKeyFile            string `mapstructure:"mqtt-signing-key-file"`
	MQTTClientKeyFile             string `mapstructure:"mqtt-client-key-file" reload:"true"`
	MQTTClientCertFile            string `mapstructure:"mqtt-client-cert-file" reload:"true"`
	MQTTServer                    string `mapstructure:"mqtt-server"`
	MQTTCAFile                    string `mapstructure:"mqtt-ca-file"`
	MQTTKeepalive                 uint16 `mapstructure:"mqtt-keepalive"`
	MQTTSignWorkers               int    `mapstructure:"mqtt-sign-workers"`
	QnameSeenEntries              int    `mapstructure:"qname-seen-entries"`
	CryptopanAddressEntries       int    `mapstructure:"cryptopan-address-entries"`
	NewQnameBuffer                int    `mapstructure:"newqname-buffer"`
	HTTPCAFile                    string `mapstructure:"http-ca-file"`
	HTTPSigningKeyFile            string `mapstructure:"http-signing-key-file"`
	HTTPClientKeyFile             string `mapstructure:"http-client-key-file" reload:"true"`
	HTTPClientCertFile            string `mapstructure:"http-client-cert-file" reload:"true"`
	HTTPURL                       string `mapstructure:"http-url"`
	Debug                         bool   `mapstructure:"debug"`
	DebugDnstapFilename           string `mapstructure:"debug-dnstap-filename"`
	DebugEnableBlockProfiling     bool   `mapstructure:"debug-enable-blockprofiling"`
	DebugEnableMutexProfiling     bool   `mapstructure:"debug-enable-mutexprofiling"`
}

// Validate checks the configuration rules for Config.
//
// It reports every violation in a single error: individual failures are
// combined with [errors.Join] and the result wraps [ErrInvalidConfig] for
// matching with [errors.Is]. Violations of the exactly-one-input rule
// additionally wrap the same error identities returned by the dnstap input
// setup. Messages use the CLI flag / config key spelling.
func (conf Config) Validate() (err error) {
	var errs []error

	for _, f := range []struct{ key, value string }{
		{"config-file", conf.ConfigFile},
		{"cryptopan-key", conf.CryptopanKey},
		{"cryptopan-key-salt", conf.CryptopanKeySalt},
		{"well-known-domains-file", conf.WellKnownDomainsFile},
		{"data-dir", conf.DataDir},
	} {
		if f.value == "" {
			errs = append(errs, fmt.Errorf("%s must be set", f.key))
		}
	}

	inputs := 0
	for _, in := range []string{conf.InputUnix, conf.InputTCP, conf.InputTLS} {
		if in != "" {
			inputs++
		}
	}
	switch {
	case inputs == 0:
		errs = append(errs, fmt.Errorf("%w: set one of input-unix, input-tcp or input-tls", errNoInputConfigured))
	case inputs > 1:
		errs = append(errs, fmt.Errorf("%w: set only one of input-unix, input-tcp or input-tls", errMultipleInputsConfigured))
	}

	if conf.InputTLS != "" {
		if conf.InputTLSCertFile == "" {
			errs = append(errs, errors.New("input-tls-cert-file must be set when input-tls is used"))
		}
		if conf.InputTLSKeyFile == "" {
			errs = append(errs, errors.New("input-tls-key-file must be set when input-tls is used"))
		}
	}

	if conf.HistogramHLLExplicitThreshold < 1 {
		errs = append(errs, errors.New("histogram-hll-explicit-threshold must be greater than 0"))
	}
	if conf.CryptopanAddressEntries < 0 {
		errs = append(errs, errors.New("cryptopan-address-entries must not be negative"))
	}

	if !conf.DisableMQTT {
		for _, f := range []struct{ key, value string }{
			{"mqtt-signing-key-file", conf.MQTTSigningKeyFile},
			{"mqtt-client-key-file", conf.MQTTClientKeyFile},
			{"mqtt-client-cert-file", conf.MQTTClientCertFile},
			{"mqtt-server", conf.MQTTServer},
		} {
			if f.value == "" {
				errs = append(errs, fmt.Errorf("%s must be set unless disable-mqtt is true", f.key))
			}
		}
		if conf.MQTTKeepalive == 0 {
			errs = append(errs, errors.New("mqtt-keepalive must be set unless disable-mqtt is true"))
		}
	}

	if !conf.DisableHistogramSender {
		for _, f := range []struct{ key, value string }{
			{"http-signing-key-file", conf.HTTPSigningKeyFile},
			{"http-client-key-file", conf.HTTPClientKeyFile},
			{"http-client-cert-file", conf.HTTPClientCertFile},
			{"http-url", conf.HTTPURL},
		} {
			if f.value == "" {
				errs = append(errs, fmt.Errorf("%s must be set unless disable-histogram-sender is true", f.key))
			}
		}
	}

	if len(errs) > 0 {
		err = fmt.Errorf("%w: %w", ErrInvalidConfig, errors.Join(errs...))
	}
	return
}

// ConfigProvider supplies the current runner configuration.
//
// Implementations must be safe to call from the runner's config reload
// goroutine while [DnstapMinimiser.Run] is active.
type ConfigProvider interface {
	GetConfig() (Config, error)
}

// ViperConfigProvider reads [Config] from the package-level Viper instance.
type ViperConfigProvider struct{}

// GetConfig reads and validates Config from Viper.
func (vc ViperConfigProvider) GetConfig() (Config, error) {
	conf := Config{}

	// Re-read the config file on every call so a SIGHUP-triggered reload
	// always observes the current file contents rather than whatever Viper
	// cached at startup.
	err := viper.ReadInConfig()
	if err != nil {
		return Config{}, fmt.Errorf("getViperConfig: unable to read in config: %w", err)
	}

	err = viper.UnmarshalExact(&conf)
	if err != nil {
		return Config{}, fmt.Errorf("getViperConfig: unable to unmarshal config: %w", err)
	}

	err = conf.Validate()
	if err != nil {
		return Config{}, fmt.Errorf("getViperConfig: %w", err)
	}

	return conf, nil
}

func (edm *DnstapMinimiser) updateConfig() error {
	edm.confMutex.Lock()
	defer edm.confMutex.Unlock()

	conf, err := edm.configer.GetConfig()
	if err != nil {
		return err
	}

	edm.conf = conf

	return nil
}

func (edm *DnstapMinimiser) getConfig() Config {
	edm.confMutex.RLock()
	conf := edm.conf
	edm.confMutex.RUnlock()

	return conf
}
