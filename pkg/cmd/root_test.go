package cmd

import (
	"errors"
	"io"
	"log/slog"
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
		_ = runCmd.Flags().Set("http-url", "https://127.0.0.1:8443")
		_ = runCmd.Flags().Set("debug", "false")
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
