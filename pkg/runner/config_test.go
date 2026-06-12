package runner

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pelletier/go-toml/v2"
)

// writeConfigFile writes data to a temp TOML file and returns its path.
func writeConfigFile(t *testing.T, data string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "edm.toml")
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// minimalConfigData is the smallest config file content that passes
// Config.Validate when layered over DefaultConfig.
const minimalConfigData = "cryptopan-key = \"secret\"\ninput-unix = \"/tmp/dnstap.sock\"\n"

func TestFileConfigProvider(t *testing.T) {
	t.Run("defaults layered under file", func(t *testing.T) {
		path := writeConfigFile(t, minimalConfigData+"data-dir = \"/srv/edm\"\n")
		conf, err := NewFileConfigProvider(path).GetConfig()
		if err != nil {
			t.Fatalf("GetConfig: %s", err)
		}
		if conf.DataDir != "/srv/edm" {
			t.Fatalf("DataDir = %q, want file value", conf.DataDir)
		}
		def := DefaultConfig()
		if conf.MQTTServer != def.MQTTServer || conf.QnameSeenEntries != def.QnameSeenEntries {
			t.Fatalf("defaults not applied: %#v", conf)
		}
		if conf.ConfigFile != path {
			t.Fatalf("ConfigFile = %q, want %q", conf.ConfigFile, path)
		}
	})

	t.Run("overrides win over file on every read", func(t *testing.T) {
		path := writeConfigFile(t, minimalConfigData+"debug = false\n")
		provider := NewFileConfigProvider(path, func(c *Config) { c.Debug = true })
		for range 2 {
			conf, err := provider.GetConfig()
			if err != nil {
				t.Fatalf("GetConfig: %s", err)
			}
			if !conf.Debug {
				t.Fatal("override did not win over file value")
			}
		}
	})

	t.Run("unknown key is rejected", func(t *testing.T) {
		path := writeConfigFile(t, minimalConfigData+"no-such-key = true\n")
		_, err := NewFileConfigProvider(path).GetConfig()
		var strictErr *toml.StrictMissingError
		if !errors.As(err, &strictErr) {
			t.Fatalf("GetConfig = %v, want *toml.StrictMissingError", err)
		}
	})

	t.Run("missing file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "missing.toml")
		_, err := NewFileConfigProvider(path).GetConfig()
		if !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("GetConfig = %v, want fs.ErrNotExist", err)
		}
	})

	t.Run("validation failure wraps ErrInvalidConfig", func(t *testing.T) {
		path := writeConfigFile(t, "input-unix = \"/tmp/dnstap.sock\"\n")
		_, err := NewFileConfigProvider(path).GetConfig()
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("GetConfig = %v, want ErrInvalidConfig", err)
		}
	})

	t.Run("Path returns the configured path", func(t *testing.T) {
		if got := NewFileConfigProvider("/etc/edm.toml").Path(); got != "/etc/edm.toml" {
			t.Fatalf("Path() = %q", got)
		}
	})
}

// validValidateConfig returns a Config that passes [Config.Validate]. It
// builds on defaultTestConfig, filling in the fields that helper leaves
// empty on purpose.
func validValidateConfig() Config {
	c := defaultTestConfig()
	c.CryptopanKey = "key1"
	c.InputUnix = "/run/edm/dnstap.sock"
	c.MQTTServer = "127.0.0.1:8883"
	c.HTTPURL = "https://127.0.0.1:8443"
	return c
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name string
		// mutate adjusts a valid baseline config into the case under test.
		mutate func(*Config)
		// wantErrs are matched with errors.Is; empty means expect success.
		wantErrs []error
		// wantMsgs are substrings expected in err.Error().
		wantMsgs []string
	}{
		{
			name:   "valid baseline",
			mutate: func(c *Config) {},
		},
		{
			name: "valid with senders disabled and fields cleared",
			mutate: func(c *Config) {
				c.DisableMQTT = true
				c.MQTTSigningKeyFile = ""
				c.MQTTClientKeyFile = ""
				c.MQTTClientCertFile = ""
				c.MQTTServer = ""
				c.MQTTKeepalive = 0
				c.DisableHistogramSender = true
				c.HTTPSigningKeyFile = ""
				c.HTTPClientKeyFile = ""
				c.HTTPClientCertFile = ""
				c.HTTPURL = ""
			},
		},
		{
			name:     "missing config-file",
			mutate:   func(c *Config) { c.ConfigFile = "" },
			wantErrs: []error{ErrInvalidConfig},
			wantMsgs: []string{"config-file must be set"},
		},
		{
			name:     "missing cryptopan-key",
			mutate:   func(c *Config) { c.CryptopanKey = "" },
			wantErrs: []error{ErrInvalidConfig},
			wantMsgs: []string{"cryptopan-key must be set"},
		},
		{
			name:     "missing cryptopan-key-salt",
			mutate:   func(c *Config) { c.CryptopanKeySalt = "" },
			wantErrs: []error{ErrInvalidConfig},
			wantMsgs: []string{"cryptopan-key-salt must be set"},
		},
		{
			name:     "missing well-known-domains-file",
			mutate:   func(c *Config) { c.WellKnownDomainsFile = "" },
			wantErrs: []error{ErrInvalidConfig},
			wantMsgs: []string{"well-known-domains-file must be set"},
		},
		{
			name:     "missing data-dir",
			mutate:   func(c *Config) { c.DataDir = "" },
			wantErrs: []error{ErrInvalidConfig},
			wantMsgs: []string{"data-dir must be set"},
		},
		{
			name:     "no input configured",
			mutate:   func(c *Config) { c.InputUnix = "" },
			wantErrs: []error{ErrInvalidConfig, errNoInputConfigured},
		},
		{
			name:     "multiple inputs configured",
			mutate:   func(c *Config) { c.InputTCP = "127.0.0.1:53535" },
			wantErrs: []error{ErrInvalidConfig, errMultipleInputsConfigured},
		},
		{
			name: "input-tls without cert and key files",
			mutate: func(c *Config) {
				c.InputUnix = ""
				c.InputTLS = "127.0.0.1:53535"
			},
			wantErrs: []error{ErrInvalidConfig},
			wantMsgs: []string{
				"input-tls-cert-file must be set when input-tls is used",
				"input-tls-key-file must be set when input-tls is used",
			},
		},
		{
			name: "input-tls with cert and key files is valid",
			mutate: func(c *Config) {
				c.InputUnix = ""
				c.InputTLS = "127.0.0.1:53535"
				c.InputTLSCertFile = "cert.pem"
				c.InputTLSKeyFile = "key.pem"
			},
		},
		{
			name:     "histogram-hll-explicit-threshold zero",
			mutate:   func(c *Config) { c.HistogramHLLExplicitThreshold = 0 },
			wantErrs: []error{ErrInvalidConfig},
			wantMsgs: []string{"histogram-hll-explicit-threshold must be greater than 0"},
		},
		{
			name:     "histogram-hll-explicit-threshold negative",
			mutate:   func(c *Config) { c.HistogramHLLExplicitThreshold = -1 },
			wantErrs: []error{ErrInvalidConfig},
			wantMsgs: []string{"histogram-hll-explicit-threshold must be greater than 0"},
		},
		{
			name:   "histogram-hll-explicit-threshold one is valid",
			mutate: func(c *Config) { c.HistogramHLLExplicitThreshold = 1 },
		},
		{
			name:     "cryptopan-address-entries negative",
			mutate:   func(c *Config) { c.CryptopanAddressEntries = -1 },
			wantErrs: []error{ErrInvalidConfig},
			wantMsgs: []string{"cryptopan-address-entries must not be negative"},
		},
		{
			name:   "cryptopan-address-entries zero is valid",
			mutate: func(c *Config) { c.CryptopanAddressEntries = 0 },
		},
		{
			name:     "mqtt enabled missing mqtt-signing-key-file",
			mutate:   func(c *Config) { c.MQTTSigningKeyFile = "" },
			wantErrs: []error{ErrInvalidConfig},
			wantMsgs: []string{"mqtt-signing-key-file must be set unless disable-mqtt is true"},
		},
		{
			name:     "mqtt enabled missing mqtt-client-key-file",
			mutate:   func(c *Config) { c.MQTTClientKeyFile = "" },
			wantErrs: []error{ErrInvalidConfig},
			wantMsgs: []string{"mqtt-client-key-file must be set unless disable-mqtt is true"},
		},
		{
			name:     "mqtt enabled missing mqtt-client-cert-file",
			mutate:   func(c *Config) { c.MQTTClientCertFile = "" },
			wantErrs: []error{ErrInvalidConfig},
			wantMsgs: []string{"mqtt-client-cert-file must be set unless disable-mqtt is true"},
		},
		{
			name:     "mqtt enabled missing mqtt-server",
			mutate:   func(c *Config) { c.MQTTServer = "" },
			wantErrs: []error{ErrInvalidConfig},
			wantMsgs: []string{"mqtt-server must be set unless disable-mqtt is true"},
		},
		{
			name:     "mqtt enabled zero mqtt-keepalive",
			mutate:   func(c *Config) { c.MQTTKeepalive = 0 },
			wantErrs: []error{ErrInvalidConfig},
			wantMsgs: []string{"mqtt-keepalive must be set unless disable-mqtt is true"},
		},
		{
			name:     "histogram sender enabled missing http-signing-key-file",
			mutate:   func(c *Config) { c.HTTPSigningKeyFile = "" },
			wantErrs: []error{ErrInvalidConfig},
			wantMsgs: []string{"http-signing-key-file must be set unless disable-histogram-sender is true"},
		},
		{
			name:     "histogram sender enabled missing http-client-key-file",
			mutate:   func(c *Config) { c.HTTPClientKeyFile = "" },
			wantErrs: []error{ErrInvalidConfig},
			wantMsgs: []string{"http-client-key-file must be set unless disable-histogram-sender is true"},
		},
		{
			name:     "histogram sender enabled missing http-client-cert-file",
			mutate:   func(c *Config) { c.HTTPClientCertFile = "" },
			wantErrs: []error{ErrInvalidConfig},
			wantMsgs: []string{"http-client-cert-file must be set unless disable-histogram-sender is true"},
		},
		{
			name:     "histogram sender enabled missing http-url",
			mutate:   func(c *Config) { c.HTTPURL = "" },
			wantErrs: []error{ErrInvalidConfig},
			wantMsgs: []string{"http-url must be set unless disable-histogram-sender is true"},
		},
		{
			name: "multiple failures reported together",
			mutate: func(c *Config) {
				c.CryptopanKey = ""
				c.InputTCP = "127.0.0.1:53535"
				c.HistogramHLLExplicitThreshold = 0
				c.HTTPURL = ""
			},
			wantErrs: []error{ErrInvalidConfig, errMultipleInputsConfigured},
			wantMsgs: []string{
				"cryptopan-key must be set",
				"histogram-hll-explicit-threshold must be greater than 0",
				"http-url must be set unless disable-histogram-sender is true",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conf := validValidateConfig()
			tc.mutate(&conf)

			err := conf.Validate()

			if len(tc.wantErrs) == 0 && len(tc.wantMsgs) == 0 {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() = nil, want error matching %v", tc.wantErrs)
			}
			for _, wantErr := range tc.wantErrs {
				if !errors.Is(err, wantErr) {
					t.Errorf("Validate() = %v, want errors.Is(err, %v)", err, wantErr)
				}
			}
			for _, wantMsg := range tc.wantMsgs {
				if !strings.Contains(err.Error(), wantMsg) {
					t.Errorf("Validate() = %v, want message containing %q", err, wantMsg)
				}
			}
		})
	}
}
