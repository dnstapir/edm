package runner

import (
	"bytes"
	"errors"
	"fmt"
	"os"

	"github.com/pelletier/go-toml/v2"
)

// Labels 0-9
const defaultLabelLimit = 10

// Config contains all runtime configuration for [DnstapMinimiser].
//
// The toml struct tags name the config file keys and stay in sync with the
// flags in pkg/cmd. Validation rules are enforced by [Config.Validate].
type Config struct {
	ConfigFile                    string `toml:"config-file"`
	DisableSessionFiles           bool   `toml:"disable-session-files" reload:"true"`
	DisableHistogramSender        bool   `toml:"disable-histogram-sender" reload:"true"`
	DisableMQTT                   bool   `toml:"disable-mqtt"`
	DisableMQTTFilequeue          bool   `toml:"disable-mqtt-filequeue"`
	EnableManualParquetRotation   bool   `toml:"enable-manual-parquet-rotation"`
	PebbleSync                    bool   `toml:"pebble-sync" reload:"true"`
	InputUnix                     string `toml:"input-unix"`
	InputTCP                      string `toml:"input-tcp"`
	InputTLS                      string `toml:"input-tls"`
	InputTLSCertFile              string `toml:"input-tls-cert-file"`
	InputTLSKeyFile               string `toml:"input-tls-key-file"`
	InputTLSClientCAFile          string `toml:"input-tls-client-ca-file"`
	CryptopanKey                  string `toml:"cryptopan-key" reload:"true"`
	CryptopanKeySalt              string `toml:"cryptopan-key-salt" reload:"true"`
	WellKnownDomainsFile          string `toml:"well-known-domains-file" reload:"true"`
	HistogramHLLExplicitThreshold int    `toml:"histogram-hll-explicit-threshold"`
	IgnoredClientIPsFile          string `toml:"ignored-client-ips-file" reload:"true"`
	IgnoredQuestionNamesFile      string `toml:"ignored-question-names-file" reload:"true"`
	DataDir                       string `toml:"data-dir"`
	MinimiserWorkers              int    `toml:"minimiser-workers"`
	MQTTSigningKeyFile            string `toml:"mqtt-signing-key-file"`
	MQTTClientKeyFile             string `toml:"mqtt-client-key-file" reload:"true"`
	MQTTClientCertFile            string `toml:"mqtt-client-cert-file" reload:"true"`
	MQTTServer                    string `toml:"mqtt-server"`
	MQTTCAFile                    string `toml:"mqtt-ca-file"`
	MQTTKeepalive                 uint16 `toml:"mqtt-keepalive"`
	MQTTSignWorkers               int    `toml:"mqtt-sign-workers"`
	QnameSeenEntries              int    `toml:"qname-seen-entries"`
	CryptopanAddressEntries       int    `toml:"cryptopan-address-entries"`
	NewQnameBuffer                int    `toml:"newqname-buffer"`
	HTTPCAFile                    string `toml:"http-ca-file"`
	HTTPSigningKeyFile            string `toml:"http-signing-key-file"`
	HTTPClientKeyFile             string `toml:"http-client-key-file" reload:"true"`
	HTTPClientCertFile            string `toml:"http-client-cert-file" reload:"true"`
	HTTPURL                       string `toml:"http-url"`
	Debug                         bool   `toml:"debug"`
	DebugDnstapFilename           string `toml:"debug-dnstap-filename"`
	DebugEnableBlockProfiling     bool   `toml:"debug-enable-blockprofiling"`
	DebugEnableMutexProfiling     bool   `toml:"debug-enable-mutexprofiling"`
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

// DefaultConfig returns the built-in defaults for [Config].
//
// It is the single source of truth for both the "run" command flag defaults
// and the base layer [FileConfigProvider] starts from before applying the
// config file and startup overrides.
func DefaultConfig() (conf Config) {
	conf = Config{
		CryptopanKeySalt:              "edm-kdf-salt-val",
		WellKnownDomainsFile:          "well-known-domains.dawg",
		DataDir:                       "/var/lib/dnstapir/edm",
		MinimiserWorkers:              1,
		MQTTSigningKeyFile:            "edm-mqtt-signer-key.pem",
		MQTTClientKeyFile:             "edm-mqtt-client-key.pem",
		MQTTClientCertFile:            "edm-mqtt-client.pem",
		MQTTServer:                    "127.0.0.1:8883",
		MQTTKeepalive:                 30,
		QnameSeenEntries:              10_000_000,
		CryptopanAddressEntries:       10_000_000,
		NewQnameBuffer:                1000,
		HistogramHLLExplicitThreshold: 20,
		HTTPSigningKeyFile:            "edm-http-signer-key.pem",
		HTTPClientKeyFile:             "edm-http-client-key.pem",
		HTTPClientCertFile:            "edm-http-client.pem",
		HTTPURL:                       "https://127.0.0.1:8443",
	}
	return
}

// ConfigOverride applies one flag- or environment-derived value to a Config.
//
// Overrides are captured once at startup and re-applied on every config
// reload, so command line and environment values always win over the config
// file even after the file changes.
type ConfigOverride func(*Config)

// FileConfigProvider reads [Config] from a TOML file, layering it as
// [DefaultConfig] values overwritten by file contents overwritten by
// overrides.
//
// It implements [ConfigProvider] and is safe for repeated GetConfig calls
// from the runner's config reload goroutine.
type FileConfigProvider struct {
	path      string
	overrides []ConfigOverride
}

// NewFileConfigProvider returns a provider reading the TOML file at path.
func NewFileConfigProvider(path string, overrides ...ConfigOverride) *FileConfigProvider {
	return &FileConfigProvider{path: path, overrides: overrides}
}

// Path returns the config file path the provider reads from.
func (p *FileConfigProvider) Path() string {
	return p.path
}

// GetConfig reads and validates the configuration.
//
// The file is re-read on every call so a SIGHUP-triggered reload always
// observes the current file contents. Unknown keys in the config file are
// rejected; the returned error wraps [toml.StrictMissingError]. A decode or
// strict-mode failure additionally carries go-toml's human-readable detail
// (the offending key and source line). Validation failures wrap
// [ErrInvalidConfig].
func (p *FileConfigProvider) GetConfig() (conf Config, err error) {
	var data []byte
	data, err = os.ReadFile(p.path)
	if err == nil {
		conf = DefaultConfig()
		dec := toml.NewDecoder(bytes.NewReader(data))
		dec.DisallowUnknownFields()
		err = dec.Decode(&conf)
	}
	if err == nil {
		for _, override := range p.overrides {
			override(&conf)
		}
		conf.ConfigFile = p.path
		err = conf.Validate()
	}
	if err != nil {
		conf = Config{}
		if detail := tomlErrorDetail(err); detail != "" {
			err = fmt.Errorf("GetConfig: %w\n%s", err, detail)
		} else {
			err = fmt.Errorf("GetConfig: %w", err)
		}
	}
	return
}

// tomlErrorDetail returns go-toml's human-readable rendering of a decode or
// strict-mode error, naming the offending key and source location. It
// returns "" for errors that are not go-toml decode errors.
//
// go-toml's Error method is intentionally terse (for example "strict mode:
// fields in the document are missing in the target struct"), so the detailed
// rendering from its String method is surfaced alongside it. [toml.StrictMissingError]
// is checked before [toml.DecodeError] because the former unwraps to the
// latter and its String covers every missing field at once.
func tomlErrorDetail(err error) string {
	var strictErr *toml.StrictMissingError
	if errors.As(err, &strictErr) {
		return strictErr.String()
	}
	var decErr *toml.DecodeError
	if errors.As(err, &decErr) {
		return decErr.String()
	}
	return ""
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
