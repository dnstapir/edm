package runner

import (
	"errors"
	"io"
	"log/slog"
	"testing"
)

func TestNewDnstapMinimiserAPI(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	if _, err := NewDnstapMinimiser(nil, logger); !errors.Is(err, ErrNilConfigProvider) {
		t.Fatalf("nil provider err = %v, want %v", err, ErrNilConfigProvider)
	}
	if _, err := NewDnstapMinimiser(defaultTC, nil); !errors.Is(err, ErrNilLogger) {
		t.Fatalf("nil logger err = %v, want %v", err, ErrNilLogger)
	}

	loggerLevel := new(slog.LevelVar)
	edm, err := NewDnstapMinimiser(defaultTC, logger,
		WithLoggerLevel(loggerLevel),
		WithDependencies(Dependencies{PprofListenAddr: "127.0.0.1:0"}),
	)
	if err != nil {
		t.Fatalf("NewDnstapMinimiser: %s", err)
	}
	t.Cleanup(func() { cleanupTestMinimiser(edm) })

	if edm.loggerLevel != loggerLevel {
		t.Fatal("WithLoggerLevel did not install the supplied level var")
	}
	if edm.deps.PprofListenAddr != "127.0.0.1:0" {
		t.Fatalf("PprofListenAddr = %q, want custom value", edm.deps.PprofListenAddr)
	}
	if edm.deps.FileSystem == nil || edm.deps.Clock == nil || edm.deps.HTTPServerRunner == nil {
		t.Fatal("WithDependencies did not fill nil dependency fields")
	}
}

func TestDnstapMinimiserRunGuards(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)

	if err := edm.Run(nil); !errors.Is(err, ErrNilRunContext) {
		t.Fatalf("Run(nil) err = %v, want %v", err, ErrNilRunContext)
	}

	edm.running.Store(true)
	t.Cleanup(func() { edm.running.Store(false) })

	if err := edm.Run(t.Context()); !errors.Is(err, ErrDnstapMinimiserRunning) {
		t.Fatalf("concurrent Run err = %v, want %v", err, ErrDnstapMinimiserRunning)
	}
}
