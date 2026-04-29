# Fixes

## Config Env Isolation

- **Bug:** Viper accepted unprefixed environment variables, so unrelated ambient variables such as `DEBUG=release` were treated as EDM config.
- **Impact:** Startup could fail during config unmarshalling before any runtime work began, even when the config file and flags were valid.
- **Fix:** Environment overrides now require the `DNSTAPIR_EDM_` prefix and map hyphenated config keys to underscore-separated environment keys.
- **Reasoning:** Prefixed environment variables preserve intentional env-based config while preventing collisions with common shell or build environment variables.
- **Tests:** Added command-package tests proving `DEBUG=release` is ignored and `DNSTAPIR_EDM_DEBUG=true` still overrides `debug = false`.
