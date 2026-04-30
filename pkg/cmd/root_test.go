package cmd

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/viper"
)

func TestInitConfigIgnoresUnprefixedEnv(t *testing.T) {
	initConfigForTest(t, "debug = false\n")
	t.Setenv("DEBUG", "release")

	initConfig()

	var conf struct {
		Debug bool `mapstructure:"debug"`
	}
	if err := viper.UnmarshalExact(&conf); err != nil {
		t.Fatalf("unprefixed DEBUG should not affect config unmarshalling: %s", err)
	}
	if conf.Debug {
		t.Fatal("unprefixed DEBUG unexpectedly overrode config debug=false")
	}
}

func TestInitConfigUsesPrefixedEnv(t *testing.T) {
	initConfigForTest(t, "debug = false\n")
	t.Setenv("DEBUG", "release")
	debugEnv := envPrefix + "_DEBUG"
	t.Setenv(debugEnv, "true")

	initConfig()

	var conf struct {
		Debug bool `mapstructure:"debug"`
	}
	if err := viper.UnmarshalExact(&conf); err != nil {
		t.Fatalf("prefixed debug env should unmarshal cleanly: %s", err)
	}
	if !conf.Debug {
		t.Fatalf("%s=true did not override config debug=false", debugEnv)
	}
}

func initConfigForTest(t *testing.T, configData string) {
	t.Helper()

	viper.Reset()
	oldCfgFile := cfgFile
	oldLogger := edmLogger
	t.Cleanup(func() {
		cfgFile = oldCfgFile
		edmLogger = oldLogger
		viper.Reset()
	})

	configFile := filepath.Join(t.TempDir(), "dnstapir-edm.toml")
	if err := os.WriteFile(configFile, []byte(configData), 0o600); err != nil {
		t.Fatalf("unable to write test config: %s", err)
	}
	cfgFile = configFile
	edmLogger = slog.New(slog.NewTextHandler(io.Discard, nil))
}
