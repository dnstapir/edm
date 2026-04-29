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
