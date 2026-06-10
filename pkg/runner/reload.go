package runner

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

func configUpdater(ctx context.Context, viperNotifyCh chan fsnotify.Event, edm *DnstapMinimiser) {
	// Since not all config changes are dynamically picked up and requires
	// a restart, keep a reference to what we started with.
	startConf := edm.getConfig()

	// The notifications from viper are based on
	// https://github.com/fsnotify/fsnotify which means we can receive
	// multiple events for the same file when someone modifies it. E.g. an
	// editor like vim writing to the file can result in three events
	// (CREATE, WRITE, WRITE) because of how the editor juggles the file
	// during a write.
	//
	// To not let this translate to us updating settings three times when
	// one is enough we wait a short duration for more events to occur
	// before telling things to update.
	//
	// The code below is inspired by the example at:
	// https://github.com/fsnotify/fsnotify/blob/main/cmd/fsnotify/dedup.go

	applyUpdate := func(eventName string) {
		edm.log.Info("configUpdater: config file was modified", "filename", eventName)

		oldConf := edm.getConfig()

		err := edm.updateConfig()
		if err != nil {
			edm.log.Error("configUpdater: unable to update edm config", "error", err)
			return
		}

		conf := edm.getConfig()

		// Log a warning if we detect a changed config key that
		// does not have the 'reload:"true"' struct tag. If you
		// implement new functionality below to handle dynamically
		// reloading a new config key remember to also add the "reload"
		// struct tag to it.
		vOld := reflect.ValueOf(oldConf)
		vNew := reflect.ValueOf(conf)
		typ := vOld.Type()

		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)

			oldVal := vOld.Field(i).Interface()
			newVal := vNew.Field(i).Interface()

			if !reflect.DeepEqual(oldVal, newVal) {
				if field.Tag.Get("reload") != "true" {
					edm.log.Warn("configUpdater: modified configuration requires restart", "struct_field", field.Name, "config_key", field.Tag.Get("mapstructure"))
				}
			}
		}

		if oldConf.CryptopanKey != conf.CryptopanKey ||
			oldConf.CryptopanKeySalt != conf.CryptopanKeySalt {
			edm.log.Info("configUpdater: updating Crypto-PAn instance")
			err = edm.setCryptopan(conf.CryptopanKey, conf.CryptopanKeySalt, conf.CryptopanAddressEntries)
			if err != nil {
				edm.log.Error("configUpdater: unable to update Crypto-PAn instance", "error", err)
			}
		}

		if oldConf.IgnoredClientIPsFile != conf.IgnoredClientIPsFile {
			err := edm.setIgnoredClientIPs()
			if err != nil {
				edm.log.Error("configUpdater: unable to run edm.setIgnoredClientIPs", "error", err)
			}
		}

		if oldConf.IgnoredQuestionNamesFile != conf.IgnoredQuestionNamesFile {
			err := edm.setIgnoredQuestionNames()
			if err != nil {
				edm.log.Error("configUpdater: unable to run edm.setIgnoredQuestionNames", "error", err)
			}
		}

		if !conf.DisableHistogramSender {
			if oldConf.HTTPClientCertFile != conf.HTTPClientCertFile ||
				oldConf.HTTPClientKeyFile != conf.HTTPClientKeyFile {
				err := edm.loadHTTPClientCert()
				if err != nil {
					edm.log.Error("configUpdater: unable to run edm.loadHTTPClientCert", "error", err)
				}
			}

			// If the histogram sender was not enabled at startup
			// we also need to initialize it.
			edm.aggregSenderMutex.RLock()
			isUninitialized := edm.aggregSender == nil
			edm.aggregSenderMutex.RUnlock()
			if isUninitialized {
				// Also make sure a client certificate is loaded
				_, err := edm.httpClientCertStore.getClientCertificate(nil)
				if err != nil {
					if errors.Is(err, errNoClientCertificate) {
						err := edm.loadHTTPClientCert()
						if err != nil {
							edm.log.Error("configUpdater: unable to init certs when setting up histogram sender", "error", err)
						}
					} else {
						edm.log.Error("configUpdater: unable to call getClientCertificate", "error", err)
					}
				}
				err = edm.setupHistogramSender()
				if err != nil {
					edm.log.Error("configUpdater: unable to setup histogram sender", "error", err)
				}
			}
		}

		if !conf.DisableMQTT {
			if !startConf.DisableMQTT {
				if oldConf.MQTTClientCertFile != conf.MQTTClientCertFile ||
					oldConf.MQTTClientKeyFile != conf.MQTTClientKeyFile {
					err := edm.loadMQTTClientCert()
					if err != nil {
						edm.log.Error("configUpdater: unable to run edm.loadMQTTClientCert", "error", err)
					}
				}
			}
		}

		err = edm.configureFSWatchers(startConf)
		if err != nil {
			edm.log.Error("unable to update fs watchers", "error", err)
		}

		if oldConf != conf {
			edm.reloadMinimiserMutex.RLock()
			for minimiserID := range edm.reloadMinimiserConfigCh {
				select {
				case edm.reloadMinimiserConfigCh[minimiserID] <- struct{}{}:
				default: // notify already queued
				}
			}
			edm.reloadMinimiserMutex.RUnlock()

			select {
			case edm.reloadHistogramSenderConfigCh <- struct{}{}:
			default: // notify already queued
			}
		}
	}

	var debounce <-chan time.Time
	var eventName string
	for {
		select {
		case ev, ok := <-viperNotifyCh:
			if !ok {
				return
			}
			// If an event has been received this means we now want to
			// wait "soon" before updating, but if more events occur we
			// replace the pending debounce channel. This allows us to wait
			// until events on the file settle down before actually calling
			// the update function.
			eventName = ev.Name
			debounce = edm.deps.Clock.After(edm.deps.ConfigUpdateDebounce)
		case <-debounce:
			name := eventName
			debounce = nil
			applyUpdate(name)
		case <-ctx.Done():
			return
		}
	}
}

func (edm *DnstapMinimiser) fsEventWatcher(wg *sync.WaitGroup) {
	defer wg.Done()
	// Like in
	// https://github.com/fsnotify/fsnotify/blob/main/cmd/fsnotify/dedup.go
	// we keep a timer per registered filename
	timers := map[string]Timer{}
	timersMutex := new(sync.Mutex)
	defer func() {
		timersMutex.Lock()
		defer timersMutex.Unlock()
		for _, t := range timers {
			t.Stop()
		}
	}()

	callbackHandler := func(callbacks []func() error, name string) func() {
		return func() {
			for _, callback := range callbacks {
				err := callback()
				if err != nil {
					edm.log.Error("fsEventWatcher: callback error", "filename", name, "error", err)
				}
			}

			// Cleanup expired timer
			timersMutex.Lock()
			delete(timers, name)
			timersMutex.Unlock()
		}
	}

	for {
		select {
		case event, ok := <-edm.fsWatcher.Events():
			if !ok {
				// watcher is closed
				return
			}

			if !event.Has(fsnotify.Write) && !event.Has(fsnotify.Create) {
				continue
			}

			cleanName := filepath.Clean(event.Name)

			edm.fsWatcherMutex.RLock()
			callbacks, ok := edm.fsWatcherFuncs[cleanName]
			edm.fsWatcherMutex.RUnlock()
			if !ok {
				if edm.debug {
					edm.log.Info("skipping event for unregistered file", "op", event.Op.String(), "filename", cleanName)
				}
				continue
			}

			timersMutex.Lock()
			t, ok := timers[cleanName]
			timersMutex.Unlock()
			if !ok {
				t = edm.deps.Clock.AfterFunc(math.MaxInt64, callbackHandler(callbacks, cleanName))
				t.Stop()

				timersMutex.Lock()
				timers[cleanName] = t
				timersMutex.Unlock()
			}

			t.Reset(edm.deps.FSEventDebounce)
		case err, ok := <-edm.fsWatcher.Errors():
			if !ok {
				// watcher is closed
				return
			}
			edm.log.Error("fsEventWatcher: error received", "error", err)
		}
	}
}

func (edm *DnstapMinimiser) configureFSWatchers(startConf Config) error {
	conf := edm.getConfig()

	// Build fresh file -> []func mapping
	fileToFuncs := map[string][]func() error{}

	if conf.IgnoredClientIPsFile != "" {
		fname, err := filepath.Abs(conf.IgnoredClientIPsFile)
		if err != nil {
			edm.log.Error("unable to create absolute filepath for conf.IgnoredClientsIPsFile", "error", err)
		} else {
			fileToFuncs[fname] = append(fileToFuncs[fname], edm.setIgnoredClientIPs)
		}
	}

	if conf.IgnoredQuestionNamesFile != "" {
		fname, err := filepath.Abs(conf.IgnoredQuestionNamesFile)
		if err != nil {
			edm.log.Error("unable to create absolute filepath for conf.IgnoredQuestionNamesFile", "error", err)
		} else {
			fileToFuncs[fname] = append(fileToFuncs[fname], edm.setIgnoredQuestionNames)
		}
	}

	if !conf.DisableHistogramSender {
		if conf.HTTPClientCertFile != "" {
			fname, err := filepath.Abs(conf.HTTPClientCertFile)
			if err != nil {
				edm.log.Error("unable to create absolute filepath for conf.HTTPClientCertFile", "error", err)
			} else {
				fileToFuncs[fname] = append(fileToFuncs[fname], edm.loadHTTPClientCert)
			}
		}
	}

	// MQTT can only be enabled/disabled at startup
	if !startConf.DisableMQTT {
		if conf.MQTTClientCertFile != "" {
			fname, err := filepath.Abs(conf.MQTTClientCertFile)
			if err != nil {
				edm.log.Error("unable to create absolute filepath for conf.MQTTClientCertFile", "error", err)
			} else {
				fileToFuncs[fname] = append(fileToFuncs[fname], edm.loadMQTTClientCert)
			}
		}
	}

	edm.fsWatcherMutex.Lock()
	edm.fsWatcherFuncs = fileToFuncs
	edm.fsWatcherMutex.Unlock()

	err := edm.addFSWatchers(fileToFuncs)
	if err != nil {
		return fmt.Errorf("configureFSWatchers: addFSWatchers failed: %w", err)
	}

	err = edm.cleanupFSWatchers()
	if err != nil {
		return fmt.Errorf("configureFSWatchers: cleanupFSWatchers failed: %w", err)
	}

	return nil
}

func (edm *DnstapMinimiser) addFSWatchers(fileToFuncs map[string][]func() error) error {
	// Adding the same dir multiple times is a no-op, so it is OK to
	// add multiple files from the same directory.
	for filename := range fileToFuncs {
		err := edm.fsWatcher.Add(filepath.Dir(filename))
		if err != nil {
			return fmt.Errorf("addFSWatchers: unable to add directory '%s': %w", filepath.Dir(filename), err)
		}
	}

	return nil
}

func (edm *DnstapMinimiser) cleanupFSWatchers() error {
	edm.fsWatcherMutex.RLock()
	defer edm.fsWatcherMutex.RUnlock()
	for _, watchPath := range edm.fsWatcher.WatchList() {
		watchPathInUse := false
		for fsWatcherFuncFilename := range edm.fsWatcherFuncs {
			if filepath.Dir(fsWatcherFuncFilename) == watchPath {
				watchPathInUse = true
			}
		}

		if !watchPathInUse {
			edm.log.Info("cleanupFSWatchers: cleaning up path watcher", "watch_path", watchPath)
			err := edm.fsWatcher.Remove(watchPath)
			if err != nil {
				return fmt.Errorf("cleanupFSWatchers: unable to remove path watcher '%s': %w", watchPath, err)
			}
		}
	}

	return nil
}
