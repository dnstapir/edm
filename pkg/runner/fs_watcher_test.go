package runner

import (
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/fsnotify/fsnotify"
)

func TestCleanupFSWatchersReleasesLockOnError(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)

	// Watch a directory that is not referenced by any callback so
	// cleanupFSWatchers tries to remove it.
	removeErr := errors.New("remove failed")
	edm.fsWatcher = &testFileWatcher{
		events:    make(chan fsnotify.Event),
		errors:    make(chan error),
		done:      make(chan struct{}),
		watchList: []string{t.TempDir()},
		removeErr: removeErr,
	}
	edm.fsWatcherFuncs = make(map[string][]func() error)

	err := edm.cleanupFSWatchers()
	if !errors.Is(err, removeErr) {
		t.Fatalf("cleanupFSWatchers error have: %v, want: %v", err, removeErr)
	}

	// The RLock must have been released even though removal failed.
	if !edm.fsWatcherMutex.TryLock() {
		t.Fatal("fsWatcherMutex.TryLock() failed - mutex is still locked after cleanupFSWatchers")
	}
	edm.fsWatcherMutex.Unlock()
}

func TestFSWatchersAndEventWatcher(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		edm := newSynctestDnstapMinimiser(t, defaultTC)
		watcher := newTestFileWatcher()
		edm.fsWatcher = watcher
		dir := t.TempDir()
		watched := filepath.Join(dir, "watched.txt")
		var calls atomic.Int32
		callbackDone := make(chan struct{}, 1)
		edm.fsWatcherFuncs = map[string][]func() error{
			watched: {
				func() error {
					calls.Add(1)
					select {
					case callbackDone <- struct{}{}:
					default:
					}
					return errors.New("logged")
				},
			},
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go edm.fsEventWatcher(&wg)
		defer func() {
			if err := edm.fsWatcher.Close(); err != nil {
				t.Fatal(err)
			}
			wg.Wait()
		}()

		watcher.events <- fsnotifyEvent(watched)
		time.Sleep(edm.deps.FSEventDebounce)
		synctest.Wait()
		select {
		case <-callbackDone:
		default:
			t.Fatal("watcher callback did not run")
		}
		if calls.Load() != 1 {
			t.Fatalf("watcher callbacks = %d, want 1", calls.Load())
		}
	})
}

type testFileWatcher struct {
	events    chan fsnotify.Event
	errors    chan error
	done      chan struct{}
	watchList []string
	removeErr error
}

func newTestFileWatcher() *testFileWatcher {
	return &testFileWatcher{
		events: make(chan fsnotify.Event, 1),
		errors: make(chan error, 1),
		done:   make(chan struct{}),
	}
}

func (tfw *testFileWatcher) Add(string) error {
	return nil
}

func (tfw *testFileWatcher) Remove(string) error {
	return tfw.removeErr
}

func (tfw *testFileWatcher) Close() error {
	select {
	case <-tfw.done:
	default:
		close(tfw.done)
		close(tfw.events)
		close(tfw.errors)
	}
	return nil
}

func (tfw *testFileWatcher) WatchList() []string {
	return tfw.watchList
}

func (tfw *testFileWatcher) Events() <-chan fsnotify.Event {
	return tfw.events
}

func (tfw *testFileWatcher) Errors() <-chan error {
	return tfw.errors
}

func fsnotifyEvent(name string) fsnotify.Event {
	return fsnotify.Event{Name: name, Op: fsnotify.Write}
}

func TestConfigureFSWatchers(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	dir := t.TempDir()
	edm.conf.IgnoredClientIPsFile = filepath.Join(dir, "ignored-ips")
	edm.conf.IgnoredQuestionNamesFile = filepath.Join(dir, "ignored-names")
	edm.conf.HTTPClientCertFile = filepath.Join(dir, "http-cert")
	edm.conf.MQTTClientCertFile = filepath.Join(dir, "mqtt-cert")
	edm.conf.DisableHistogramSender = false
	startConf := edm.conf
	startConf.DisableMQTT = false

	if err := edm.configureFSWatchers(startConf); err != nil {
		t.Fatal(err)
	}
	if len(edm.fsWatcherFuncs) != 4 {
		t.Fatalf("fsWatcherFuncs = %d, want 4", len(edm.fsWatcherFuncs))
	}

	edm.conf.IgnoredClientIPsFile = ""
	edm.conf.IgnoredQuestionNamesFile = ""
	edm.conf.HTTPClientCertFile = ""
	edm.conf.MQTTClientCertFile = ""
	if err := edm.configureFSWatchers(startConf); err != nil {
		t.Fatal(err)
	}
	if len(edm.fsWatcherFuncs) != 0 {
		t.Fatalf("fsWatcherFuncs after cleanup = %d", len(edm.fsWatcherFuncs))
	}
}

// TestAddFSWatchersErrorOnBadPath covers the addFSWatchers error branch:
// asking fsnotify to watch a non-existent directory fails with
// ENOENT, which addFSWatchers wraps and returns.
func TestAddFSWatchersErrorOnBadPath(t *testing.T) {
	edm := newTestDnstapMinimiser(t, defaultTC)
	bogus := filepath.Join(t.TempDir(), "missing-dir", "watched")
	err := edm.addFSWatchers(map[string][]func() error{bogus: {func() error { return nil }}})
	if err == nil || !strings.Contains(err.Error(), "addFSWatchers") {
		t.Fatalf("err = %v, want addFSWatchers wrap", err)
	}
}
