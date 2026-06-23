package runner

import (
	"path/filepath"
	"testing"
)

// configSamplePath locates the annotated reference config shipped in the
// packages as /etc/dnstapir/dnstapir-edm.toml.sample. Tests run with the
// package directory as the working directory, so the repository root is two
// levels up.
const configSamplePath = "../../dnstapir-edm.toml.sample"

// TestConfigSampleParsesAndValidates guards dnstapir-edm.toml.sample against
// drift from Config. The loader rejects unknown keys and runs Config.Validate,
// so a stale or misspelled key, a missing required value, or zero/multiple
// active inputs in the sample fails this test.
func TestConfigSampleParsesAndValidates(t *testing.T) {
	path, err := filepath.Abs(configSamplePath)
	if err != nil {
		t.Fatalf("resolving sample path: %s", err)
	}

	conf, err := NewFileConfigProvider(path).GetConfig()
	if err != nil {
		t.Fatalf("config sample %s must parse and validate: %s", path, err)
	}

	// The two documented edit points must be active out of the box: the
	// Crypto-PAn secret and the input-tcp DNSTAP interface that matches the
	// resolver example in the installation guide.
	if conf.CryptopanKey == "" {
		t.Error("config sample must set cryptopan-key")
	}
	if conf.InputTCP == "" {
		t.Error("config sample must set input-tcp as the active DNSTAP interface")
	}

	// The sample documents the full Core surface, so MQTT and the histogram
	// sender are left enabled rather than disabled.
	if conf.DisableMQTT {
		t.Error("config sample should leave MQTT enabled")
	}
	if conf.DisableHistogramSender {
		t.Error("config sample should leave the histogram sender enabled")
	}
}
