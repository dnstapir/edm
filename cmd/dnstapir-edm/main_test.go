package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

func TestBuildLogger(t *testing.T) {
	var buf bytes.Buffer
	level := new(slog.LevelVar)

	logger := buildLogger(&buf, level, "v1.2.3", "test-host")
	if logger == nil {
		t.Fatal("buildLogger returned nil logger")
	}

	logger.Info("hello")

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("log output is not valid JSON: %v (%q)", err, buf.String())
	}

	wantFields := map[string]string{
		"service":  "dnstapir-edm",
		"hostname": "test-host",
		"version":  "v1.2.3",
		"msg":      "hello",
	}
	for k, want := range wantFields {
		if got, _ := rec[k].(string); got != want {
			t.Errorf("log field %q = %q, want %q", k, got, want)
		}
	}
	if _, ok := rec["go_version"]; !ok {
		t.Error("log record missing go_version field")
	}
}

func TestBuildLoggerRespectsLevel(t *testing.T) {
	var buf bytes.Buffer
	level := new(slog.LevelVar) // Info by default

	logger := buildLogger(&buf, level, "v1", "host")

	logger.Debug("suppressed")
	if buf.Len() != 0 {
		t.Fatalf("debug record should be suppressed at info level, got %q", buf.String())
	}

	level.Set(slog.LevelDebug)
	logger.Debug("emitted")
	if !strings.Contains(buf.String(), "emitted") {
		t.Fatalf("debug record should be emitted after raising level, got %q", buf.String())
	}
}

func TestResolveHostname(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		orig := osHostname
		t.Cleanup(func() { osHostname = orig })
		osHostname = func() (string, error) { return "real-host", nil }

		var warn bytes.Buffer
		if got := resolveHostname(&warn); got != "real-host" {
			t.Errorf("resolveHostname = %q, want %q", got, "real-host")
		}
		if warn.Len() != 0 {
			t.Errorf("unexpected warning on success: %q", warn.String())
		}
	})

	t.Run("fallback", func(t *testing.T) {
		orig := osHostname
		t.Cleanup(func() { osHostname = orig })
		osHostname = func() (string, error) { return "", errors.New("boom") }

		var warn bytes.Buffer
		if got := resolveHostname(&warn); got != defaultHostname {
			t.Errorf("resolveHostname = %q, want %q", got, defaultHostname)
		}
		if !strings.Contains(warn.String(), defaultHostname) {
			t.Errorf("warning should mention fallback hostname, got %q", warn.String())
		}
	})
}
