package runner

import "testing"

func TestConfigValidationAllowsTLSWithoutClientCA(t *testing.T) {
	conf := validConfigForValidation()
	conf.InputUnix = ""
	conf.InputTLS = "127.0.0.1:53535"
	conf.InputTLSCertFile = "server-cert.pem"
	conf.InputTLSKeyFile = "server-key.pem"

	if err := validate.Struct(conf); err != nil {
		t.Fatalf("TLS input with server cert/key and no client CA should be valid: %s", err)
	}
}

func validConfigForValidation() config {
	return config{
		ConfigFile:                    "dnstapir-edm.toml",
		DisableHistogramSender:        true,
		DisableMQTT:                   true,
		InputUnix:                     "/tmp/dnstapir-edm.sock",
		CryptopanKey:                  "mysecret",
		CryptopanKeySalt:              "edm-kdf-salt-val",
		WellKnownDomainsFile:          "well-known-domains.dawg",
		HistogramHLLExplicitThreshold: 20,
		DataDir:                       "/var/lib/dnstapir/edm",
	}
}
