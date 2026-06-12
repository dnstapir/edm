package cmd

import (
	"bytes"
	"errors"
	"flag"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dnstapir/edm/pkg/runner"
)

func testLogger() (*slog.Logger, *slog.LevelVar) {
	level := new(slog.LevelVar)
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: level})), level
}

// restoreCmdGlobals snapshots the package-level seams mutated by tests and
// restores them on cleanup.
func restoreCmdGlobals(t *testing.T) {
	t.Helper()

	oldLogger := edmLogger
	oldLoggerLevel := edmLoggerLevel
	oldExitProcess := exitProcess
	oldUserHomeDir := userHomeDir
	oldOSArgs := osArgs

	t.Cleanup(func() {
		edmLogger = oldLogger
		edmLoggerLevel = oldLoggerLevel
		exitProcess = oldExitProcess
		userHomeDir = oldUserHomeDir
		osArgs = oldOSArgs
	})
}

// writeTestConfig writes a minimal valid config file and returns its path.
func writeTestConfig(t *testing.T, extra string) string {
	t.Helper()

	configFile := filepath.Join(t.TempDir(), "dnstapir-edm.toml")
	configData := "cryptopan-key = \"test-secret\"\ninput-unix = \"/tmp/dnstap.sock\"\n" + extra
	if err := os.WriteFile(configFile, []byte(configData), 0o600); err != nil {
		t.Fatalf("unable to write test config: %s", err)
	}
	return configFile
}

func TestDispatch(t *testing.T) {
	restoreCmdGlobals(t)
	edmLogger, edmLoggerLevel = testLogger()

	tests := []struct {
		name       string
		args       []string
		wantErr    error
		wantOutSub string
		wantErrSub string
	}{
		{
			name:       "no args prints usage",
			args:       nil,
			wantOutSub: "Usage:",
		},
		{
			name:       "help prints usage",
			args:       []string{"help"},
			wantOutSub: "Usage:",
		},
		{
			name:       "root --help prints usage on stderr",
			args:       []string{"--help"},
			wantErrSub: "Usage:",
		},
		{
			name: "run --help succeeds",
			args: []string{"run", "--help"},
		},
		{
			name:       "unknown command errors",
			args:       []string{"frobnicate"},
			wantErr:    errUnknownCommand,
			wantErrSub: `unknown command "frobnicate"`,
		},
		{
			name:    "bad root flag errors",
			args:    []string{"--no-such-flag"},
			wantErr: nil, // flag package synthesizes its own error
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			outW := &bytes.Buffer{}
			errW := &bytes.Buffer{}

			err := dispatch(tc.args, outW, errW)

			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("dispatch() = %v, want errors.Is(err, %v)", err, tc.wantErr)
			}
			if tc.wantErr == nil && tc.name != "bad root flag errors" && err != nil {
				t.Fatalf("dispatch() = %v, want nil", err)
			}
			if tc.wantOutSub != "" && !strings.Contains(outW.String(), tc.wantOutSub) {
				t.Fatalf("stdout %q does not contain %q", outW.String(), tc.wantOutSub)
			}
			if tc.wantErrSub != "" && !strings.Contains(errW.String(), tc.wantErrSub) {
				t.Fatalf("stderr %q does not contain %q", errW.String(), tc.wantErrSub)
			}
		})
	}

	t.Run("bad root flag returns error", func(t *testing.T) {
		errW := &bytes.Buffer{}
		if err := dispatch([]string{"--no-such-flag"}, io.Discard, errW); err == nil {
			t.Fatal("dispatch with unknown root flag succeeded")
		}
	})
}

func TestDispatchRunReportsMissingConfigFile(t *testing.T) {
	restoreCmdGlobals(t)
	edmLogger, edmLoggerLevel = testLogger()

	missing := filepath.Join(t.TempDir(), "missing.toml")

	// The rpm systemd unit form: --config-file before the subcommand.
	err := dispatch([]string{"--config-file", missing, "run"}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), missing) {
		t.Fatalf("dispatch() = %v, want error mentioning %q", err, missing)
	}

	// run's own --config-file flag wins over the root flag.
	missingRun := filepath.Join(t.TempDir(), "missing-run.toml")
	err = dispatch([]string{"--config-file", missing, "run", "--config-file", missingRun}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), missingRun) {
		t.Fatalf("dispatch() = %v, want error mentioning %q", err, missingRun)
	}
}

func TestDispatchRunUnexpectedArgument(t *testing.T) {
	restoreCmdGlobals(t)
	edmLogger, edmLoggerLevel = testLogger()

	err := dispatch([]string{"run", "surprise"}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "unexpected argument") {
		t.Fatalf("dispatch() = %v, want unexpected argument error", err)
	}
}

func TestResolveConfigPath(t *testing.T) {
	restoreCmdGlobals(t)

	path, err := resolveConfigPath("/etc/explicit.toml")
	if err != nil || path != "/etc/explicit.toml" {
		t.Fatalf("resolveConfigPath(explicit) = %q, %v", path, err)
	}

	userHomeDir = func() (string, error) { return "/home/tester", nil }
	path, err = resolveConfigPath("")
	if err != nil || path != "/home/tester/.dnstapir-edm.toml" {
		t.Fatalf("resolveConfigPath(\"\") = %q, %v", path, err)
	}

	homeErr := errors.New("no home")
	userHomeDir = func() (string, error) { return "", homeErr }
	if _, err := resolveConfigPath(""); !errors.Is(err, homeErr) {
		t.Fatalf("resolveConfigPath(\"\") err = %v, want %v", err, homeErr)
	}
}

func TestBuildRunProviderPrecedence(t *testing.T) {
	tests := []struct {
		name  string
		extra string            // appended to the base config file
		env   map[string]string // set via t.Setenv
		args  []string
		check func(t *testing.T, conf interface {
			GetDebug() bool
		})
		// verify receives the resolved config.
		verify func(t *testing.T, debug bool, wkdFile string, dataDir string, keepalive uint16, httpURL string)
	}{
		{
			name: "defaults only",
			verify: func(t *testing.T, debug bool, wkdFile, dataDir string, keepalive uint16, httpURL string) {
				if debug || wkdFile != "well-known-domains.dawg" || dataDir != "/var/lib/dnstapir/edm" || keepalive != 30 || httpURL != "https://127.0.0.1:8443" {
					t.Fatalf("unexpected defaults: debug=%v wkd=%q dataDir=%q keepalive=%d httpURL=%q", debug, wkdFile, dataDir, keepalive, httpURL)
				}
			},
		},
		{
			name:  "file overrides default",
			extra: "data-dir = \"/srv/edm\"\nmqtt-keepalive = 60\n",
			verify: func(t *testing.T, debug bool, wkdFile, dataDir string, keepalive uint16, httpURL string) {
				if dataDir != "/srv/edm" || keepalive != 60 {
					t.Fatalf("file values not applied: dataDir=%q keepalive=%d", dataDir, keepalive)
				}
			},
		},
		{
			name:  "env overrides file",
			extra: "debug = false\nwell-known-domains-file = \"from-file.dawg\"\n",
			env: map[string]string{
				"DNSTAPIR_EDM_DEBUG":                   "true",
				"DNSTAPIR_EDM_WELL_KNOWN_DOMAINS_FILE": "from-env.dawg",
				"DNSTAPIR_EDM_MQTT_KEEPALIVE":          "45",
			},
			verify: func(t *testing.T, debug bool, wkdFile, dataDir string, keepalive uint16, httpURL string) {
				if !debug || wkdFile != "from-env.dawg" || keepalive != 45 {
					t.Fatalf("env values not applied: debug=%v wkd=%q keepalive=%d", debug, wkdFile, keepalive)
				}
			},
		},
		{
			name:  "CLI overrides env",
			extra: "debug = true\n",
			env: map[string]string{
				"DNSTAPIR_EDM_DEBUG":          "true",
				"DNSTAPIR_EDM_MQTT_KEEPALIVE": "45",
			},
			args: []string{"--debug=false", "--mqtt-keepalive", "99"},
			verify: func(t *testing.T, debug bool, wkdFile, dataDir string, keepalive uint16, httpURL string) {
				if debug || keepalive != 99 {
					t.Fatalf("CLI values not applied: debug=%v keepalive=%d", debug, keepalive)
				}
			},
		},
		{
			name:  "unprefixed env is ignored",
			extra: "debug = false\n",
			env: map[string]string{
				"DEBUG": "true",
			},
			verify: func(t *testing.T, debug bool, wkdFile, dataDir string, keepalive uint16, httpURL string) {
				if debug {
					t.Fatal("unprefixed DEBUG unexpectedly overrode config debug=false")
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			configFile := writeTestConfig(t, tc.extra)

			provider, err := buildRunProvider(append([]string{"--config-file", configFile}, tc.args...), "", io.Discard)
			if err != nil {
				t.Fatalf("buildRunProvider: %s", err)
			}
			conf, err := provider.GetConfig()
			if err != nil {
				t.Fatalf("GetConfig: %s", err)
			}
			if conf.ConfigFile != configFile {
				t.Fatalf("ConfigFile = %q, want %q", conf.ConfigFile, configFile)
			}
			tc.verify(t, conf.Debug, conf.WellKnownDomainsFile, conf.DataDir, conf.MQTTKeepalive, conf.HTTPURL)
		})
	}
}

func TestBuildRunProviderOverridesSurviveReload(t *testing.T) {
	configFile := writeTestConfig(t, "debug = false\ndata-dir = \"/srv/one\"\n")
	t.Setenv("DNSTAPIR_EDM_DEBUG", "true")

	provider, err := buildRunProvider([]string{"--config-file", configFile, "--data-dir", "/srv/cli"}, "", io.Discard)
	if err != nil {
		t.Fatalf("buildRunProvider: %s", err)
	}

	conf, err := provider.GetConfig()
	if err != nil {
		t.Fatalf("GetConfig: %s", err)
	}
	if !conf.Debug || conf.DataDir != "/srv/cli" {
		t.Fatalf("first read: debug=%v dataDir=%q", conf.Debug, conf.DataDir)
	}

	// Rewrite the file changing both an overridden key and a free key; the
	// startup overrides must still win on re-read while the free key change
	// lands.
	newData := "cryptopan-key = \"test-secret\"\ninput-unix = \"/tmp/dnstap.sock\"\ndebug = false\ndata-dir = \"/srv/two\"\nqname-seen-entries = 42\n"
	if err := os.WriteFile(configFile, []byte(newData), 0o600); err != nil {
		t.Fatalf("rewrite config: %s", err)
	}

	conf, err = provider.GetConfig()
	if err != nil {
		t.Fatalf("GetConfig after rewrite: %s", err)
	}
	if !conf.Debug || conf.DataDir != "/srv/cli" {
		t.Fatalf("overrides lost on reload: debug=%v dataDir=%q", conf.Debug, conf.DataDir)
	}
	if conf.QnameSeenEntries != 42 {
		t.Fatalf("file change not picked up on reload: qnameSeenEntries=%d", conf.QnameSeenEntries)
	}
}

func TestApplyEnvOverridesInvalidValue(t *testing.T) {
	t.Setenv("DNSTAPIR_EDM_DEBUG", "release")

	_, err := buildRunProvider(nil, "", io.Discard)
	if err == nil || !strings.Contains(err.Error(), "DNSTAPIR_EDM_DEBUG") {
		t.Fatalf("buildRunProvider = %v, want error naming DNSTAPIR_EDM_DEBUG", err)
	}
}

// TestOverrideForCoversAllFlags guards the overrideFor switch against
// drifting from the flags registered in newRunFlagSet.
func TestOverrideForCoversAllFlags(t *testing.T) {
	flagConf := new(runner.Config)
	fs := newRunFlagSet(flagConf)
	fs.VisitAll(func(f *flag.Flag) {
		if f.Name == "config-file" {
			if overrideFor(f.Name, flagConf) != nil {
				t.Error("config-file should not produce an override")
			}
			return
		}
		if overrideFor(f.Name, flagConf) == nil {
			t.Errorf("flag %q has no overrideFor mapping", f.Name)
		}
	})
}

func TestExecuteSuccessAndError(t *testing.T) {
	restoreCmdGlobals(t)
	logger, level := testLogger()

	exitCalled := false
	exitProcess = func(code int) { exitCalled = true }

	osArgs = func() []string { return []string{"dnstapir-edm", "help"} }
	Execute(logger, level)
	if edmLogger != logger || edmLoggerLevel != level {
		t.Fatal("Execute did not store logger globals")
	}
	if exitCalled {
		t.Fatal("Execute exited on a successful command")
	}

	var exitCode int
	exitProcess = func(code int) { exitCode = code }
	osArgs = func() []string { return []string{"dnstapir-edm", "unknown-command"} }
	Execute(logger, level)
	if exitCode != 1 {
		t.Fatalf("exit code = %d, want 1", exitCode)
	}
}

func TestRunMinimiserInitError(t *testing.T) {
	restoreCmdGlobals(t)
	edmLogger, edmLoggerLevel = testLogger()

	if err := runMinimiser(nil); err == nil {
		t.Fatal("runMinimiser with nil provider succeeded")
	}
}

// TestPrintUsageMentionsRunCommand sanity-checks the usage text mentions
// the pieces users rely on.
func TestPrintUsageMentionsRunCommand(t *testing.T) {
	out := &bytes.Buffer{}
	rootFS := flag.NewFlagSet("dnstapir-edm", flag.ContinueOnError)
	rootFS.String("config-file", "", "config file")
	printUsage(out, rootFS)
	for _, want := range []string{"run", "help", "config-file"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("usage output missing %q:\n%s", want, out.String())
		}
	}
}
