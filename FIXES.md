# Fixes

## fsEventWatcher Goroutine Not Tracked

- **Bug:** The `fsEventWatcher` goroutine was started at line 1204 with `go edm.fsEventWatcher()` and was not tracked in any WaitGroup. When `Run()` exited, the goroutine was forced to terminate abruptly via `fsWatcher.Close()` on the deferred path, with no way to verify it had completed.
- **Impact:** Inconsistent shutdown semantics; potential for fsEventWatcher to be processing an event while the rest of the program shuts down; in the worst case, a callback could run while shared state was being torn down.
- **Fix:** (1) Added a `*sync.WaitGroup` parameter to `fsEventWatcher` with `defer wg.Done()`. (2) Tracked it in the main `wg` via `wg.Add(1); go edm.fsEventWatcher(&wg)`. (3) Moved fsEventWatcher start to after `wg` is declared. (4) Added explicit `fsWatcher.Close()` before `wg.Wait()` so the goroutine has a signal to exit. (5) Made the deferred fsWatcher.Close into a safety net that checks for nil.
- **Reasoning:** All long-lived goroutines should be tracked by a WaitGroup for proper shutdown sequencing, matching the pattern used by other goroutines in the program.
- **Tests:** Verified by running the full test suite; no regressions.

## NewQnameEvent Empty Question Section Panic

- **Bug:** In `pkg/protocols/protocols.go`, `NewQnameEvent()` accessed `msg.Question[0]` without first checking whether the Question section had any entries. While all current callers in the runner package validate this before calling, the function itself was unsafe and would panic on an empty Question section.
- **Impact:** A future caller (or test) that called `NewQnameEvent()` with an empty Question section would panic. As a public function in a published package, this is a defensive programming violation.
- **Fix:** Added a length check on `msg.Question`. If empty, the returned event has an empty `Qname` and nil `Qtype`/`Qclass` pointers, while header-derived fields (Flags, Type, Version) are still populated.
- **Reasoning:** Public functions should validate their inputs or be safe against edge cases. The fix provides graceful degradation rather than panicking.
- **Tests:** Added `TestNewQnameEventValid` and `TestNewQnameEventEmptyQuestion` in `pkg/protocols/protocols_test.go` to verify both the happy path and the empty-question case.

## Input Channel Not Closed and DNStap Reader Not Synchronized

- **Bug:** The dnstap reader goroutine (`dti.ReadInto(edm.inputChannel)`) was started at line 1383 without being tracked in a WaitGroup. The `inputChannel` was never explicitly closed. When `minimiserWg.Wait()` completed, minimisers stopped reading from the channel, but `dti.ReadInto` could still be blocked trying to send.
- **Impact:** Goroutine leak; unclean shutdown; potential panic if channel is closed while dti.ReadInto is sending; no guarantee of orderly shutdown.
- **Fix:** Wrapped `dti.ReadInto` in a tracked goroutine with `wg.Add(1)` and `defer wg.Done()`, and added `close(edm.inputChannel)` after `minimiserWg.Wait()` to signal the reader to exit.
- **Reasoning:** All long-lived goroutines must be tracked in the main WaitGroup for proper shutdown sequencing; channels should be explicitly closed by the sender when done.
- **Tests:** Verified by running full test suite; no new test needed as the fix ensures orderly shutdown already tested by existing tests.

## HTTP Servers (pprof and metrics) Not Synchronized or Gracefully Shut Down

- **Bug:** The pprof and metrics HTTP servers (lines 1269-1272 and 1285-1288) were started as goroutines without WaitGroup tracking. Neither server received graceful shutdown signals; both `ListenAndServe()` blocks indefinitely. When `wg.Wait()` completed, they were still running.
- **Impact:** Unclean HTTP server shutdown; potential connection leaks; incomplete request handling; goroutines forced to terminate abruptly on process exit.
- **Fix:** (1) Wrapped both servers in tracked goroutines with `wg.Add(1)` and `defer wg.Done()`, handling `http.ErrServerClosed` as expected error. (2) Added graceful `Shutdown(ctx)` calls with 5-second timeout before `wg.Wait()`.
- **Reasoning:** HTTP servers need explicit graceful shutdown to drain pending requests and close connections cleanly; all goroutines should be tracked for proper ordering.
- **Tests:** Verified by running full test suite and checking that shutdown completes without errors.

## HTTP Client Transport Resource Leak on Reload

- **Bug:** In `setupHistogramSender()`, when the aggregateSender was replaced (e.g., during config reload), the old aggregateSender's HTTP transport's connection pool was never closed. The transport holds idle connections that accumulate over time.
- **Impact:** Over multiple config reloads, idle HTTP connections accumulate and are not returned to the OS, exhausting per-process connection limits and file descriptors.
- **Fix:** (1) Added `httpTransport` field to `aggregateSender` struct to track the transport. (2) In `setupHistogramSender()`, call `CloseIdleConnections()` on the old transport before replacing it.
- **Reasoning:** HTTP transports maintain connection pools that must be explicitly cleaned up when the client is replaced, or connections will remain open until GC runs or the process exits.
- **Tests:** Verified by running full test suite with no regressions.

## Disk Cleaner Retention Period

- **Bug:** The `diskCleaner` used a variable named `oneDay` but set it to `time.Hour * 12` (12 hours), not 24 hours. Sent histogram files were deleted after 12 hours instead of the intended 24 hours.
- **Impact:** Histogram files were retained for only half the intended duration, potentially causing data loss if the aggregate receiver was unavailable for more than 12 hours.
- **Fix:** Changed `oneDay := time.Hour * 12` to `oneDay := time.Hour * 24`.
- **Reasoning:** The retention period should match the intended 24-hour window for aggregate data recovery.
- **Tests:** Added `TestDiskCleanerRetentionThreshold` to verify the retention threshold is correctly set to 24 hours.

## FS Watcher Mutex Leak

- **Bug:** In `cleanupFSWatchers()`, an early `return` statement inside the `RLock` scope at lines 1003–1005 exited the function without calling `RUnlock()`, leaking the read lock.
- **Impact:** Any subsequent attempt to acquire the write lock on `fsWatcherMutex` (e.g., in `configureFSWatchers`) would block indefinitely because the read lock was never released.
- **Fix:** Added `defer edm.fsWatcherMutex.RUnlock()` immediately after `RLock()` and removed the explicit `RUnlock()` call that was only reachable on the success path.
- **Reasoning:** Using `defer` ensures the lock is always released, even on error return paths, preventing mutex deadlocks.
- **Tests:** Added `TestCleanupFSWatchersReleasesLockOnError` which calls `cleanupFSWatchers()` and then verifies `fsWatcherMutex.TryLock()` succeeds, confirming the read lock was released.

## Config Updater Goroutine Leak

- **Bug:** In `Run()`, `configUpdater` looped over an unbuffered `viperNotifyCh` that was never closed. The `for e = range viperNotifyCh` loop at line 601 could only exit when the channel was closed, which never happened, causing the goroutine to leak on shutdown. Additionally, the `viper.OnConfigChange` callback did a blocking send on the unbuffered channel, which could deadlock the viper watcher goroutine if `configUpdater` was busy.
- **Impact:** The `configUpdater` goroutine leaked on every `Run()` call, accumulating unreleased resources in long-lived processes or test suites.
- **Fix:** Three changes: (1) Made `viperNotifyCh` buffered with capacity 1 (line 1134), (2) Changed the `viper.OnConfigChange` callback to use a non-blocking send with `select` and `default` (lines 1139–1141), (3) Changed `configUpdater`'s loop from `for e = range` to a `select` that also listens to `edm.ctx.Done()` (lines 601–617), (4) Added `close(viperNotifyCh)` at the end of `Run()` after `wg.Wait()` (line 1405).
- **Reasoning:** Config-change events are idempotent (the dedup timer already coalesces multiple events), so dropping duplicates is correct. The goroutine must respond to context cancellation for clean shutdown. Closing the channel signals the end of events and allows the `range` loop to exit.
- **Tests:** Added `TestConfigUpdaterExitsOnContextCancel` which starts `configUpdater` with a buffered channel and verifies it exits promptly (within 2 seconds) when the context is cancelled.

## Minimiser Session Send Backpressure

- **Bug:** In `runMinimiser()`, the send to `sessionCollectorCh` at line 2121 was a plain blocking send. If the channel filled up (capacity 100), the minimiser goroutine would block inside the inputFrame processing case and could never reach the outer `case <-edm.ctx.Done()` to exit.
- **Impact:** A slow or stalled `dataCollector` could prevent minimiser workers from shutting down gracefully, blocking `minimiserWg.Wait()` and stalling the entire shutdown sequence including the histogram flush.
- **Fix:** Wrapped the channel send with a `select` that includes `case <-edm.ctx.Done()`, allowing the minimiser to exit when context is cancelled even if the channel is full.
- **Reasoning:** All sends inside the main minimiser select must have a context escape hatch to honour graceful shutdown.
- **Tests:** Added `TestRunMinimiserSessionSendUnblocksOnContextCancel` which fills `sessionCollectorCh` to capacity and verifies the minimiser exits promptly (within 2 seconds) when context is cancelled.

## Histogram Sender Backoff Interruptibility

- **Bug:** In `histogramSender()`, when `as.send()` failed, the code called `time.Sleep(backoffDuration)` unconditionally (15 seconds). The sleep had no way to be interrupted by context cancellation, delaying shutdown by up to 15 seconds.
- **Impact:** Graceful shutdown could be delayed by up to 15 seconds for each failed histogram send attempt, preventing timely process termination.
- **Fix:** Replaced `time.Sleep(backoffDuration)` with a `select` that listens to both the timer and `edm.ctx.Done()`, allowing immediate exit on context cancellation.
- **Reasoning:** Long-running operations inside goroutines must be interruptible by context cancellation for clean shutdown.
- **Tests:** Verified by running the full test suite; no new test added as the fix is an internal optimization that doesn't change externally observable behavior (existing tests confirm the backoff still fires when intended).

## MQTT Publish Worker Context Cancellation

- **Bug:** In `mqttPublishWorker()`, the channel read from `mqttSignedCh` at line 198 was a plain blocking receive with no `select` on `autopahoCtx.Done()`. When the context was cancelled while the channel was empty, the goroutine blocked indefinitely and could not exit.
- **Impact:** Context cancellation alone could not stop the publish worker; shutdown depended entirely on the channel being closed, which only happens after sign workers finish. This delayed shutdown by requiring the channel closure path instead of honoring immediate cancellation.
- **Fix:** Replaced the plain blocking read with a `select` that listens to both `mqttSignedCh` and `autopahoCtx.Done()`.
- **Reasoning:** All goroutines participating in graceful shutdown must be interruptible via context cancellation.
- **Tests:** Added `TestMqttPublishWorkerExitsOnContextCancel` which starts the worker with an empty channel and verifies it exits promptly (within 2 seconds) when the context is cancelled.

## HTTP Response Body Leak on Read Error

- **Bug:** In `aggregateSender.send()`, if `io.ReadAll(res.Body)` returned an error, the function returned early without closing `res.Body`. This leaked the HTTP connection from the transport pool.
- **Impact:** Transient network errors reading the aggregate receiver's response body permanently removed one HTTP connection from the pool, gradually exhausting the pool over time.
- **Fix:** Added `defer res.Body.Close()` immediately after the successful `Do()` call and removed the explicit `res.Body.Close()` call that was only reachable on the success path.
- **Reasoning:** Using `defer` ensures the body is closed on all return paths, including error paths, which is the idiomatic Go pattern for HTTP response bodies.
- **Tests:** Added `TestAggregateSenderClosesBodyOnReadError` which uses an HTTP connection hijacker to truncate the response body mid-delivery, verifying that `send()` returns an error without leaking the connection.

## Config Env Isolation

- **Bug:** Viper accepted unprefixed environment variables, so unrelated ambient variables such as `DEBUG=release` were treated as EDM config.
- **Impact:** Startup could fail during config unmarshalling before any runtime work began, even when the config file and flags were valid.
- **Fix:** Environment overrides now require the `DNSTAPIR_EDM_` prefix and map hyphenated config keys to underscore-separated environment keys.
- **Reasoning:** Prefixed environment variables preserve intentional env-based config while preventing collisions with common shell or build environment variables.
- **Tests:** Added command-package tests proving `DEBUG=release` is ignored and `DNSTAPIR_EDM_DEBUG=true` still overrides `debug = false`.

## Optional TLS Client CA

- **Bug:** `input-tls-client-ca-file` was validated as required whenever `input-tls` was set, even though the runtime only enables client certificate authentication when the CA file is configured.
- **Impact:** Operators could not run a TLS dnstap listener without requiring client certificates.
- **Fix:** Removed the required validation from `input-tls-client-ca-file` while keeping the existing conditional mTLS setup in the listener.
- **Reasoning:** Server TLS and mTLS are separate choices; an omitted client CA should mean encrypted transport without client cert verification.
- **Tests:** Added config validation coverage showing `input-tls` with server cert/key and no client CA is valid.

## MQTT Server URL Normalization

- **Bug:** The default MQTT server value was documented and configured as a bare `host:port`, but the MQTT client expects URL-shaped values.
- **Impact:** A default or operator-provided bare address could be parsed with the host portion as a scheme and fail later during connection setup.
- **Fix:** Bare MQTT server values are normalized to `tls://host:port`; explicit schemes are parsed and preserved.
- **Reasoning:** The default port is the TLS MQTT port, so the secure scheme is the least surprising default while still allowing explicit plaintext or websocket schemes.
- **Tests:** Added MQTT URL parsing coverage for bare IPv4/IPv6 addresses, explicit TLS/MQTT/TCP schemes, missing hosts, and unsupported schemes.

## Malformed Dnstap Frame Handling

- **Bug:** A single protobuf unmarshal error caused a minimiser worker to exit, and missing message/type fields could panic before the frame was dropped.
- **Impact:** One malformed frame could permanently reduce processing capacity or crash the process.
- **Fix:** Malformed frames and structurally incomplete dnstap messages are logged and skipped while the worker continues.
- **Reasoning:** Capture streams can contain bad frames; one invalid payload should not terminate long-lived processing.
- **Tests:** Added a worker-level regression that sends malformed bytes, a dnstap message without payload, a payload without message type, and then a valid response that must still be processed.

## Partial Dnstap Message Nil-Safety

- **Bug:** Some optional dnstap protobuf fields were dereferenced as required fields, and query endpoint formatting checked `ResponsePort` while dereferencing `QueryPort`.
- **Impact:** Decodable but partially populated dnstap messages could panic processing; endpoint logs could also crash when only a query port was present.
- **Fix:** Packet parsing now uses safe timestamp fallback and shared endpoint formatting, while session creation tolerates missing socket family/protocol metadata.
- **Reasoning:** Optional protobuf fields should produce dropped packets or partial output, not process panics.
- **Tests:** Added coverage for missing query/response timestamps, missing socket family/protocol in session creation, and query-port-without-address endpoint formatting.

## Shutdown Flush

- **Bug:** The collector only wrote sessions and histograms on the minute ticker; pending data could be left in memory when shutdown happened before the next tick.
- **Impact:** The final partial interval of session rows and well-known-domain histogram updates could be silently lost during graceful shutdown.
- **Fix:** After minimiser workers stop, the collector now drains queued session/update work and flushes any accumulated session and histogram data before closing writer channels.
- **Reasoning:** Graceful shutdown should preserve already accepted data even if the current collection interval is incomplete.
- **Tests:** Added collector shutdown coverage that queues a session and histogram update, stops the collector, and verifies both writer channels receive flushed data.

## Minute-Boundary Scheduling

- **Bug:** The delay until the next minute ignored sub-second time, so a start at `12:30:00.500` waited a full 60 seconds instead of 59.5 seconds.
- **Impact:** Periodic rotation could drift nearly one second late and keep doing so after every reset.
- **Fix:** The timer now computes the duration from the current instant to `now.Truncate(time.Minute).Add(time.Minute)`.
- **Reasoning:** Computing against the actual next minute boundary preserves sub-second precision without changing the intended one-minute cadence.
- **Tests:** Added fixed-time coverage for exact minute, half-second-after-minute, and one-millisecond-before-minute cases.

## Exact Aggregate Intervals

- **Bug:** Histogram upload metadata truncated the interval start to the minute and rounded the duration to whole minutes.
- **Impact:** Partial histogram files, including shutdown flushes or any second-precision filename interval, could be reported to aggrec with the wrong `Aggregate-Interval`, such as a 45-second interval becoming `PT1M` or `PT0M`.
- **Fix:** The sender now preserves the exact start timestamp and formats the duration as an ISO 8601 duration with second precision when needed.
- **Reasoning:** The filename parser already preserves start/stop seconds, and aggrec accepts ISO 8601 start/duration intervals, so the upload metadata should reflect the file's actual interval rather than a rounded minute bucket.
- **Tests:** Added duration formatting coverage and an aggregate sender test that posts to a local HTTP server and verifies the exact `Aggregate-Interval` header.

## Concurrent New-Qname Deduplication

- **Bug:** The first-seen qname check used separate LRU and Pebble operations without an atomic per-qname critical section.
- **Impact:** Two minimiser workers processing the same previously unseen qname at the same time could both decide it was new and enqueue duplicate `new_qname` events.
- **Fix:** Added sharded per-qname locking around the LRU/Pebble check-add-set sequence.
- **Reasoning:** Identical qnames must be deduplicated as a single logical operation, while unrelated qnames should still proceed concurrently without a global lock.
- **Tests:** Added concurrent `qnameSeen` coverage proving exactly one worker reports a qname as first-seen.

## Mismatched IPv4 Session Address Safety

- **Bug:** Session creation trusted `SocketFamily_INET` enough to call `netip.Addr.As4()` on the raw address bytes without first proving the parsed address was IPv4.
- **Impact:** A malformed dnstap message with IPv4 socket family metadata but IPv6-sized address bytes could panic the minimiser while building session output.
- **Fix:** IPv4 address conversion now unmapps IPv4-mapped addresses and returns an error for non-IPv4 parsed addresses instead of panicking.
- **Reasoning:** Socket family metadata is external input; mismatches should omit the affected session IP field and log the conversion error, not crash processing.
- **Tests:** Added session coverage for `SocketFamily_INET` paired with IPv6 query address bytes.

## Shutdown Partial Interval Metadata

- **Bug:** Shutdown-flushed session and histogram files still derived their start time as `rotationTime - 60s`, even when shutdown happened partway through the current interval.
- **Impact:** The final partial interval could be written with filenames and aggregate metadata that overlapped the previous minute or claimed a longer collection window than the data actually covered.
- **Fix:** The collector now tracks session and histogram interval starts explicitly and carries those starts into flushed writer payloads; legacy callers still fall back to the previous one-minute calculation.
- **Reasoning:** Once shutdown can flush partial intervals, the writer contract needs both interval boundaries instead of reconstructing the start from the stop time.
- **Tests:** Extended collector shutdown coverage to assert flushed session and histogram payloads carry non-zero, non-inverted interval boundaries.

## Serial MQTT Publishing

- **Bug:** The MQTT publish worker was documented as the single paho writer but spawned a new goroutine for every non-filequeue publish.
- **Impact:** Slow publishes could overlap, creating concurrent calls into the connection manager and letting the worker appear drained while publish goroutines were still running.
- **Fix:** The worker now calls `Publish` directly in the single publisher goroutine; signing remains parallel upstream.
- **Reasoning:** Back-pressure belongs at the publish boundary, and the lifecycle wait group should represent all publish work accepted by the worker.
- **Tests:** Added a fake blocking MQTT connection manager that fails if a second publish starts before the first publish is released.
