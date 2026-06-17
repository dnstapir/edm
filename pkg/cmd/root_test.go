package cmd

import (
	"bytes"
	"errors"
	"flag"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
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
		name         string
		args         []string
		wantErr      error
		wantOutSub   []string
		wantErrSub   []string
		wantErrEmpty bool
	}{
		{
			name:       "no args prints usage with run flags",
			args:       nil,
			wantOutSub: []string{"Usage:", "cryptopan-key"},
		},
		{
			name:       "help prints usage with run flags",
			args:       []string{"help"},
			wantOutSub: []string{"Usage:", "cryptopan-key"},
		},
		{
			name:         "root -help prints usage with run flags on stdout",
			args:         []string{"-help"},
			wantOutSub:   []string{"Usage:", "cryptopan-key"},
			wantErrEmpty: true,
		},
		{
			name:         "root --help prints usage with run flags on stdout",
			args:         []string{"--help"},
			wantOutSub:   []string{"Usage:", "cryptopan-key"},
			wantErrEmpty: true,
		},
		{
			name:         "run --help prints run flags on stdout",
			args:         []string{"run", "--help"},
			wantOutSub:   []string{"cryptopan-key"},
			wantErrEmpty: true,
		},
		{
			name:       "unknown command errors",
			args:       []string{"frobnicate"},
			wantErr:    errUnknownCommand,
			wantErrSub: []string{`unknown command "frobnicate"`},
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
			if tc.wantErr == nil && err != nil {
				t.Fatalf("dispatch() = %v, want nil", err)
			}
			for _, want := range tc.wantOutSub {
				if !strings.Contains(outW.String(), want) {
					t.Fatalf("stdout %q does not contain %q", outW.String(), want)
				}
			}
			for _, want := range tc.wantErrSub {
				if !strings.Contains(errW.String(), want) {
					t.Fatalf("stderr %q does not contain %q", errW.String(), want)
				}
			}
			if tc.wantErrEmpty && errW.Len() != 0 {
				t.Fatalf("stderr = %q, want empty", errW.String())
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

// TestConfigFileNotSettableViaEnv pins that DNSTAPIR_EDM_CONFIG_FILE does not
// select the config path: an explicit --config-file must win over it, and with
// no flag the environment value is ignored entirely (the path falls back to
// the home default).
func TestConfigFileNotSettableViaEnv(t *testing.T) {
	restoreCmdGlobals(t)
	edmLogger, edmLoggerLevel = testLogger()

	configFile := writeTestConfig(t, "")
	bogus := filepath.Join(t.TempDir(), "from-env.toml")
	t.Setenv("DNSTAPIR_EDM_CONFIG_FILE", bogus)

	// A root-level --config-file must win over the env var, not be shadowed.
	provider, err := buildRunProvider(nil, configFile, io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("buildRunProvider: %s", err)
	}
	if provider.Path() != configFile {
		t.Fatalf("Path() = %q, want %q (env must not override --config-file)", provider.Path(), configFile)
	}

	// With no --config-file at all, the env var is ignored and the path falls
	// back to the home default rather than the env value.
	userHomeDir = func() (string, error) { return "/home/tester", nil }
	provider, err = buildRunProvider(nil, "", io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("buildRunProvider: %s", err)
	}
	if want := "/home/tester/.dnstapir-edm.toml"; provider.Path() != want {
		t.Fatalf("Path() = %q, want %q (env must be ignored)", provider.Path(), want)
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
		// verify receives the fully resolved config.
		verify func(t *testing.T, conf runner.Config)
	}{
		{
			name: "defaults only",
			verify: func(t *testing.T, conf runner.Config) {
				if conf.Debug || conf.WellKnownDomainsFile != "well-known-domains.dawg" || conf.DataDir != "/var/lib/dnstapir/edm" || conf.MQTTKeepalive != 30 || conf.HTTPURL != "https://127.0.0.1:8443" {
					t.Fatalf("unexpected defaults: %+v", conf)
				}
			},
		},
		{
			name:  "file overrides default",
			extra: "data-dir = \"/srv/edm\"\nmqtt-keepalive = 60\n",
			verify: func(t *testing.T, conf runner.Config) {
				if conf.DataDir != "/srv/edm" || conf.MQTTKeepalive != 60 {
					t.Fatalf("file values not applied: dataDir=%q keepalive=%d", conf.DataDir, conf.MQTTKeepalive)
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
			verify: func(t *testing.T, conf runner.Config) {
				if !conf.Debug || conf.WellKnownDomainsFile != "from-env.dawg" || conf.MQTTKeepalive != 45 {
					t.Fatalf("env values not applied: debug=%v wkd=%q keepalive=%d", conf.Debug, conf.WellKnownDomainsFile, conf.MQTTKeepalive)
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
			verify: func(t *testing.T, conf runner.Config) {
				if conf.Debug || conf.MQTTKeepalive != 99 {
					t.Fatalf("CLI values not applied: debug=%v keepalive=%d", conf.Debug, conf.MQTTKeepalive)
				}
			},
		},
		{
			name:  "unprefixed env is ignored",
			extra: "debug = false\n",
			env: map[string]string{
				"DEBUG": "true",
			},
			verify: func(t *testing.T, conf runner.Config) {
				if conf.Debug {
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

			provider, err := buildRunProvider(append([]string{"--config-file", configFile}, tc.args...), "", io.Discard, io.Discard)
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
			tc.verify(t, conf)
		})
	}
}

func TestBuildRunProviderOverridesSurviveReload(t *testing.T) {
	configFile := writeTestConfig(t, "debug = false\ndata-dir = \"/srv/one\"\n")
	t.Setenv("DNSTAPIR_EDM_DEBUG", "true")

	provider, err := buildRunProvider([]string{"--config-file", configFile, "--data-dir", "/srv/cli"}, "", io.Discard, io.Discard)
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

	_, err := buildRunProvider(nil, "", io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "DNSTAPIR_EDM_DEBUG") {
		t.Fatalf("buildRunProvider = %v, want error naming DNSTAPIR_EDM_DEBUG", err)
	}
}

// TestBuildRunProviderReportsErrors pins that the failures the flag package
// does not print itself are written to errW, so Execute never exits silently.
func TestBuildRunProviderReportsErrors(t *testing.T) {
	t.Run("invalid environment value", func(t *testing.T) {
		t.Setenv("DNSTAPIR_EDM_DEBUG", "release")
		var errW bytes.Buffer
		if _, err := buildRunProvider(nil, "", io.Discard, &errW); err == nil {
			t.Fatal("buildRunProvider succeeded, want error")
		}
		if !strings.Contains(errW.String(), "DNSTAPIR_EDM_DEBUG") {
			t.Fatalf("error not reported to writer: %q", errW.String())
		}
	})

	t.Run("unexpected argument", func(t *testing.T) {
		var errW bytes.Buffer
		if _, err := buildRunProvider([]string{"surprise"}, "", io.Discard, &errW); err == nil {
			t.Fatal("buildRunProvider succeeded, want error")
		}
		if !strings.Contains(errW.String(), "unexpected argument") {
			t.Fatalf("error not reported to writer: %q", errW.String())
		}
	})

	// mqtt-keepalive is the only flag with a hand-written parser (fs.Func),
	// so it is the only CLI value that can fail to parse; both the
	// non-numeric and the uint16-overflow paths must error and name the flag.
	t.Run("invalid mqtt-keepalive value", func(t *testing.T) {
		var errW bytes.Buffer
		if _, err := buildRunProvider([]string{"--mqtt-keepalive", "not-a-number"}, "", io.Discard, &errW); err == nil {
			t.Fatal("buildRunProvider succeeded, want error")
		}
		if !strings.Contains(errW.String(), "mqtt-keepalive") {
			t.Fatalf("error not reported to writer: %q", errW.String())
		}
	})

	t.Run("out-of-range mqtt-keepalive value", func(t *testing.T) {
		var errW bytes.Buffer
		if _, err := buildRunProvider([]string{"--mqtt-keepalive", "70000"}, "", io.Discard, &errW); err == nil {
			t.Fatal("buildRunProvider succeeded with overflowing keepalive, want error")
		}
		if !strings.Contains(errW.String(), "mqtt-keepalive") {
			t.Fatalf("error not reported to writer: %q", errW.String())
		}
	})
}

// TestOverrideForCoversAllFlags guards the overrideFor switch against
// drifting from the flags registered in newRunFlagSet and verifies each
// override copies the matching Config field and only that field.
//
// A flag name equals its Config field's toml tag, so the tag is an
// independent source of truth: setting only the tagged field of src and
// applying the override to a zero Config must leave exactly that field set.
// A wrong source or destination field makes the result diverge from want.
func TestOverrideForCoversAllFlags(t *testing.T) {
	flagConf := new(runner.Config)
	fs := newRunFlagSet(flagConf)

	confType := reflect.TypeOf(runner.Config{})
	fieldByTag := make(map[string]int, confType.NumField())
	for i := range confType.NumField() {
		if tag, _, _ := strings.Cut(confType.Field(i).Tag.Get("toml"), ","); tag != "" {
			fieldByTag[tag] = i
		}
	}

	fs.VisitAll(func(f *flag.Flag) {
		if f.Name == "config-file" {
			if overrideFor(f.Name, flagConf) != nil {
				t.Error("config-file should not produce an override")
			}
			return
		}
		idx, ok := fieldByTag[f.Name]
		if !ok {
			t.Errorf("flag %q has no Config field with a matching toml tag", f.Name)
			return
		}

		var src runner.Config
		setSentinel(reflect.ValueOf(&src).Elem().Field(idx))
		o := overrideFor(f.Name, &src)
		if o == nil {
			t.Errorf("flag %q has no overrideFor mapping", f.Name)
			return
		}
		var dst, want runner.Config
		o(&dst)
		setSentinel(reflect.ValueOf(&want).Elem().Field(idx))
		if dst != want {
			t.Errorf("overrideFor(%q) copied the wrong field: got %+v, want only %s set", f.Name, dst, confType.Field(idx).Name)
		}
	})
}

// setSentinel sets v to a non-zero value appropriate to its kind so a copy
// of it can be detected.
func setSentinel(v reflect.Value) {
	switch v.Kind() {
	case reflect.Bool:
		v.SetBool(true)
	case reflect.String:
		v.SetString("sentinel")
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(1)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(1)
	}
}

func TestExecuteSuccessAndError(t *testing.T) {
	restoreCmdGlobals(t)
	logger, level := testLogger()

	exitCalled := false
	exitProcess = func(int) { exitCalled = true }

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

// TestPrintUsageMentionsRunCommand sanity-checks the usage text mentions the
// pieces users rely on, including the operational "run" flags so the top-level
// help documents the full flag set rather than only the root flags.
func TestPrintUsageMentionsRunCommand(t *testing.T) {
	out := &bytes.Buffer{}
	rootFS := flag.NewFlagSet("dnstapir-edm", flag.ContinueOnError)
	rootFS.String("config-file", "", "config file")
	printUsage(out, rootFS)
	for _, want := range []string{"run", "help", "config-file", "cryptopan-key", "data-dir", "mqtt-server", "debug"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("usage output missing %q:\n%s", want, out.String())
		}
	}
	if got := strings.Count(out.String(), "config-file"); got != 1 {
		t.Fatalf("usage output contains config-file %d times, want 1:\n%s", got, out.String())
	}
}
