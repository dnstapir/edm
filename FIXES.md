# Fixes

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
