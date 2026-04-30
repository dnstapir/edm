package cmd

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func testLogger() (*slog.Logger, *slog.LevelVar) {
	level := new(slog.LevelVar)
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: level})), level
}

func restoreCmdGlobals(t *testing.T) {
	t.Helper()

	oldCfgFile := cfgFile
	oldLogger := edmLogger
	oldLoggerLevel := edmLoggerLevel
	oldExitProcess := exitProcess
	oldRunRunner := runRunner
	oldUserHomeDir := userHomeDir
	oldSetConfigFile := viperSetConfigFile
	oldAddConfigPath := viperAddConfigPath
	oldSetConfigType := viperSetConfigType
	oldSetConfigName := viperSetConfigName
	oldSetEnvPrefix := viperSetEnvPrefix
	oldSetEnvKeyReplacer := viperSetEnvKeyReplacer
	oldAutomaticEnv := viperAutomaticEnv
	oldReadInConfig := viperReadInConfig
	oldConfigFileUsed := viperConfigFileUsed
	oldWatchConfig := viperWatchConfig

	t.Cleanup(func() {
		cfgFile = oldCfgFile
		edmLogger = oldLogger
		edmLoggerLevel = oldLoggerLevel
		exitProcess = oldExitProcess
		runRunner = oldRunRunner
		userHomeDir = oldUserHomeDir
		viperSetConfigFile = oldSetConfigFile
		viperAddConfigPath = oldAddConfigPath
		viperSetConfigType = oldSetConfigType
		viperSetConfigName = oldSetConfigName
		viperSetEnvPrefix = oldSetEnvPrefix
		viperSetEnvKeyReplacer = oldSetEnvKeyReplacer
		viperAutomaticEnv = oldAutomaticEnv
		viperReadInConfig = oldReadInConfig
		viperConfigFileUsed = oldConfigFileUsed
		viperWatchConfig = oldWatchConfig
		rootCmd.SetArgs(nil)
		rootCmd.SetOut(nil)
		rootCmd.SetErr(nil)
	})
}

func TestInitConfigExplicitFile(t *testing.T) {
	restoreCmdGlobals(t)
	logger, level := testLogger()
	edmLogger = logger
	edmLoggerLevel = level
	cfgFile = "/tmp/dnstapir-edm-test.toml"

	var setFile string
	var automaticEnvCalled bool
	var watchCalled bool
	viperSetConfigFile = func(file string) { setFile = file }
	viperSetEnvPrefix = func(string) {}
	viperSetEnvKeyReplacer = func(*strings.Replacer) {}
	viperAutomaticEnv = func() { automaticEnvCalled = true }
	viperReadInConfig = func() error { return nil }
	viperConfigFileUsed = func() string { return cfgFile }
	viperWatchConfig = func() { watchCalled = true }

	initConfig()

	if setFile != cfgFile {
		t.Fatalf("SetConfigFile = %q, want %q", setFile, cfgFile)
	}
	if !automaticEnvCalled {
		t.Fatal("AutomaticEnv was not called")
	}
	if !watchCalled {
		t.Fatal("WatchConfig was not called")
	}
}

func TestInitConfigDefaultHomeNoConfig(t *testing.T) {
	restoreCmdGlobals(t)
	logger, level := testLogger()
	edmLogger = logger
	edmLoggerLevel = level
	cfgFile = ""

	var addPath, configType, configName string
	userHomeDir = func() (string, error) { return "/home/tester", nil }
	viperAddConfigPath = func(path string) { addPath = path }
	viperSetConfigType = func(value string) { configType = value }
	viperSetConfigName = func(value string) { configName = value }
	viperSetEnvPrefix = func(string) {}
	viperSetEnvKeyReplacer = func(*strings.Replacer) {}
	viperAutomaticEnv = func() {}
	viperReadInConfig = func() error { return errors.New("not found") }
	viperWatchConfig = func() {}

	initConfig()

	if addPath != "/home/tester" {
		t.Fatalf("AddConfigPath = %q", addPath)
	}
	if configType != "toml" {
		t.Fatalf("SetConfigType = %q", configType)
	}
	if configName != ".dnstapir-edm" {
		t.Fatalf("SetConfigName = %q", configName)
	}
}

func TestExecuteSuccessAndError(t *testing.T) {
	restoreCmdGlobals(t)
	logger, level := testLogger()

	viperSetEnvPrefix = func(string) {}
	viperSetEnvKeyReplacer = func(*strings.Replacer) {}
	viperAutomaticEnv = func() {}
	viperReadInConfig = func() error { return errors.New("not found") }
	viperWatchConfig = func() {}

	rootCmd.SetOut(io.Discard)
	rootCmd.SetErr(io.Discard)
	rootCmd.SetArgs([]string{"--help"})
	Execute(logger, level)
	if edmLogger != logger || edmLoggerLevel != level {
		t.Fatal("Execute did not store logger globals")
	}

	var exitCode int
	exitProcess = func(code int) { exitCode = code }
	rootCmd.SetArgs([]string{"unknown-command"})
	Execute(logger, level)
	if exitCode != 1 {
		t.Fatalf("exit code = %d, want 1", exitCode)
	}
}

func TestRunCommandUsesRunnerSeam(t *testing.T) {
	restoreCmdGlobals(t)
	logger, level := testLogger()
	edmLogger = logger
	edmLoggerLevel = level

	var gotLogger *slog.Logger
	var gotLevel *slog.LevelVar
	runRunner = func(logger *slog.Logger, loggerLevel *slog.LevelVar) {
		gotLogger = logger
		gotLevel = loggerLevel
	}

	runCmd.Run(&cobra.Command{}, nil)
	if gotLogger != logger || gotLevel != level {
		t.Fatalf("runRunner called with logger %p/%p, want %p/%p", gotLogger, gotLevel, logger, level)
	}
}

func TestRunFlagsBoundToViper(t *testing.T) {
	t.Cleanup(func() {
		if err := runCmd.Flags().Set("http-url", "https://127.0.0.1:8443"); err != nil {
			t.Fatalf("reset http-url flag: %s", err)
		}
		if err := runCmd.Flags().Set("debug", "false"); err != nil {
			t.Fatalf("reset debug flag: %s", err)
		}
	})

	for _, name := range []string{"http-url", "debug"} {
		if runCmd.Flags().Lookup(name) == nil {
			t.Fatalf("run flag %q is missing", name)
		}
	}

	if err := runCmd.Flags().Set("http-url", "https://example.test"); err != nil {
		t.Fatal(err)
	}
	if got := viper.GetString("http-url"); got != "https://example.test" {
		t.Fatalf("viper http-url = %q", got)
	}

	if err := runCmd.Flags().Set("debug", "true"); err != nil {
		t.Fatal(err)
	}
	if !viper.GetBool("debug") {
		t.Fatal("viper debug binding did not observe flag value")
	}
}

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

func TestInitConfigUsesPrefixedEnvForHyphenatedKey(t *testing.T) {
	initConfigForTest(t, "well-known-domains-file = \"from-file.dawg\"\n")
	hyphenEnv := envPrefix + "_WELL_KNOWN_DOMAINS_FILE"
	t.Setenv(hyphenEnv, "from-env.dawg")

	initConfig()

	var conf struct {
		WellKnownDomainsFile string `mapstructure:"well-known-domains-file"`
	}
	if err := viper.UnmarshalExact(&conf); err != nil {
		t.Fatalf("prefixed hyphenated env should unmarshal cleanly: %s", err)
	}
	if conf.WellKnownDomainsFile != "from-env.dawg" {
		t.Fatalf("%s did not override well-known-domains-file (hyphen-to-underscore mapping broken)", hyphenEnv)
	}
}

func initConfigForTest(t *testing.T, configData string) {
	t.Helper()

	viper.Reset()
	oldCfgFile := cfgFile
	oldLogger := edmLogger
	oldWatchConfig := viperWatchConfig
	t.Cleanup(func() {
		cfgFile = oldCfgFile
		edmLogger = oldLogger
		viperWatchConfig = oldWatchConfig
		viper.Reset()
	})

	// Stub the watcher so the real fsnotify-backed viper.WatchConfig does not
	// leak a goroutine and inotify watch on the temporary config directory.
	viperWatchConfig = func() {}

	configFile := filepath.Join(t.TempDir(), "dnstapir-edm.toml")
	if err := os.WriteFile(configFile, []byte(configData), 0o600); err != nil {
		t.Fatalf("unable to write test config: %s", err)
	}
	cfgFile = configFile
	edmLogger = slog.New(slog.NewTextHandler(io.Discard, nil))
}
