package runner

import (
	"context"
	"errors"
	"os"
	"reflect"
)

// configUpdater reloads the runner configuration each time a value is
// received on reloadCh, which [DnstapMinimiser.Run] wires to SIGHUP.
//
// The goroutine exits when ctx is cancelled or reloadCh is closed. Reload
// errors are logged and leave the previous state in place, so a SIGHUP with
// a broken config file or auxiliary file never tears down a running service.
func configUpdater(ctx context.Context, reloadCh <-chan os.Signal, edm *DnstapMinimiser) {
	// Since not all config changes are dynamically picked up and requires
	// a restart, keep a reference to what we started with.
	startConf := edm.getConfig()

	for {
		select {
		case _, ok := <-reloadCh:
			if !ok {
				return
			}
			edm.applyUpdate(startConf)
		case <-ctx.Done():
			return
		}
	}
}

// applyUpdate re-reads the configuration and re-applies every reloadable
// piece of state derived from it or from files it points at.
//
// The file-backed loaders (ignore lists and client certificates) are re-run
// unconditionally: a reload request typically means file contents changed,
// which is invisible in the Config value itself. Each loader validates its
// input and atomically swaps the active state, so a failing loader logs an
// error and keeps the previous state.
func (edm *DnstapMinimiser) applyUpdate(startConf Config) {
	edm.log.Info("configUpdater: reload requested, updating config")

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

	// The well-known-domains DAWG swap must coincide with a histogram
	// rotation (the histogram map is keyed by DAWG index), so only flag
	// the request here; the data collector applies it at the next
	// rotation, within a minute.
	edm.dawgReloadRequested.Store(true)

	if err := edm.setIgnoredClientIPs(); err != nil {
		edm.log.Error("configUpdater: unable to run edm.setIgnoredClientIPs", "error", err)
	}

	if err := edm.setIgnoredQuestionNames(); err != nil {
		edm.log.Error("configUpdater: unable to run edm.setIgnoredQuestionNames", "error", err)
	}

	if !conf.DisableHistogramSender {
		if err := edm.loadHTTPClientCert(); err != nil {
			edm.log.Error("configUpdater: unable to run edm.loadHTTPClientCert", "error", err)
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
		// MQTT can only be enabled/disabled at startup.
		if !startConf.DisableMQTT {
			if err := edm.loadMQTTClientCert(); err != nil {
				edm.log.Error("configUpdater: unable to run edm.loadMQTTClientCert", "error", err)
			}
		}
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
