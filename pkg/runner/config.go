package runner

import (
	"fmt"

	"github.com/go-playground/validator/v10"
	"github.com/spf13/viper"
)

// use a single instance of Validate, it caches struct info
var validate = validator.New(validator.WithRequiredStructEnabled())

// Labels 0-9
const defaultLabelLimit = 10

// Config contains all runtime configuration for [DnstapMinimiser].
//
// The struct tags are part of the Viper-backed CLI contract and stay in sync
// with the flags in pkg/cmd.
type Config struct {
	ConfigFile                    string `mapstructure:"config-file" validate:"required"`
	DisableSessionFiles           bool   `mapstructure:"disable-session-files"`
	DisableHistogramSender        bool   `mapstructure:"disable-histogram-sender" reload:"true"`
	DisableMQTT                   bool   `mapstructure:"disable-mqtt"`
	DisableMQTTFilequeue          bool   `mapstructure:"disable-mqtt-filequeue"`
	EnableManualParquetRotation   bool   `mapstructure:"enable-manual-parquet-rotation"`
	PebbleSync                    bool   `mapstructure:"pebble-sync" reload:"true"`
	InputUnix                     string `mapstructure:"input-unix" validate:"required_without_all=InputTCP InputTLS,excluded_with=InputTCP InputTLS"`
	InputTCP                      string `mapstructure:"input-tcp" validate:"required_without_all=InputUnix InputTLS,excluded_with=InputUnix InputTLS"`
	InputTLS                      string `mapstructure:"input-tls" validate:"required_without_all=InputUnix InputTCP,excluded_with=InputUnix InputTCP"`
	InputTLSCertFile              string `mapstructure:"input-tls-cert-file" validate:"required_with=InputTLS"`
	InputTLSKeyFile               string `mapstructure:"input-tls-key-file" validate:"required_with=InputTLS"`
	InputTLSClientCAFile          string `mapstructure:"input-tls-client-ca-file"`
	CryptopanKey                  string `mapstructure:"cryptopan-key" validate:"required" reload:"true"`
	CryptopanKeySalt              string `mapstructure:"cryptopan-key-salt" validate:"required" reload:"true"`
	WellKnownDomainsFile          string `mapstructure:"well-known-domains-file" validate:"required"`
	HistogramHLLExplicitThreshold int    `mapstructure:"histogram-hll-explicit-threshold" validate:"required,gte=0"`
	IgnoredClientIPsFile          string `mapstructure:"ignored-client-ips-file" reload:"true"`
	IgnoredQuestionNamesFile      string `mapstructure:"ignored-question-names-file" reload:"true"`
	DataDir                       string `mapstructure:"data-dir" validate:"required"`
	MinimiserWorkers              int    `mapstructure:"minimiser-workers"`
	MQTTSigningKeyFile            string `mapstructure:"mqtt-signing-key-file" validate:"required_without=DisableMQTT"`
	MQTTClientKeyFile             string `mapstructure:"mqtt-client-key-file" validate:"required_without=DisableMQTT" reload:"true"`
	MQTTClientCertFile            string `mapstructure:"mqtt-client-cert-file" validate:"required_without=DisableMQTT" reload:"true"`
	MQTTServer                    string `mapstructure:"mqtt-server" validate:"required_without=DisableMQTT"`
	MQTTCAFile                    string `mapstructure:"mqtt-ca-file"`
	MQTTKeepalive                 uint16 `mapstructure:"mqtt-keepalive" validate:"required_without=DisableMQTT"`
	MQTTSignWorkers               int    `mapstructure:"mqtt-sign-workers"`
	QnameSeenEntries              int    `mapstructure:"qname-seen-entries"`
	CryptopanAddressEntries       int    `mapstructure:"cryptopan-address-entries" validate:"gte=0"`
	NewQnameBuffer                int    `mapstructure:"newqname-buffer"`
	HTTPCAFile                    string `mapstructure:"http-ca-file"`
	HTTPSigningKeyFile            string `mapstructure:"http-signing-key-file" validate:"required_without=DisableHistogramSender"`
	HTTPClientKeyFile             string `mapstructure:"http-client-key-file" validate:"required_without=DisableHistogramSender" reload:"true"`
	HTTPClientCertFile            string `mapstructure:"http-client-cert-file" validate:"required_without=DisableHistogramSender" reload:"true"`
	HTTPURL                       string `mapstructure:"http-url" validate:"required_without=DisableHistogramSender"`
	Debug                         bool   `mapstructure:"debug"`
	DebugDnstapFilename           string `mapstructure:"debug-dnstap-filename"`
	DebugEnableBlockProfiling     bool   `mapstructure:"debug-enable-blockprofiling"`
	DebugEnableMutexProfiling     bool   `mapstructure:"debug-enable-mutexprofiling"`
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

	err = validate.Struct(conf)
	if err != nil {
		return Config{}, fmt.Errorf("getViperConfig: unable to validate config: %w", err)
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
