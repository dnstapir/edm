package runner

import "testing"

func TestParseMQTTServerURL(t *testing.T) {
	tests := []struct {
		name       string
		in         string
		wantScheme string
		wantHost   string
		wantErr    bool
	}{
		{
			name:       "bare host port defaults to tls",
			in:         "127.0.0.1:8883",
			wantScheme: "tls",
			wantHost:   "127.0.0.1:8883",
		},
		{
			name:       "bare IPv6 host port defaults to tls",
			in:         "[2001:db8::1]:8883",
			wantScheme: "tls",
			wantHost:   "[2001:db8::1]:8883",
		},
		{
			name:       "explicit tls is preserved",
			in:         "tls://mqtt.example:8883",
			wantScheme: "tls",
			wantHost:   "mqtt.example:8883",
		},
		{
			name:       "explicit mqtt is preserved",
			in:         "mqtt://mqtt.example:1883",
			wantScheme: "mqtt",
			wantHost:   "mqtt.example:1883",
		},
		{
			name:       "explicit tcp is preserved",
			in:         "tcp://mqtt.example:1883",
			wantScheme: "tcp",
			wantHost:   "mqtt.example:1883",
		},
		{
			name:    "unsupported scheme",
			in:      "ftp://mqtt.example:21",
			wantErr: true,
		},
		{
			name:    "missing host",
			in:      "tls://",
			wantErr: true,
		},
		{
			name:    "missing hostname with port",
			in:      "tls://:8883",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseMQTTServerURL(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseMQTTServerURL(%q) returned nil error", tc.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseMQTTServerURL(%q): %s", tc.in, err)
			}
			if got.Scheme != tc.wantScheme {
				t.Fatalf("scheme have: %s, want: %s", got.Scheme, tc.wantScheme)
			}
			if got.Host != tc.wantHost {
				t.Fatalf("host have: %s, want: %s", got.Host, tc.wantHost)
			}
		})
	}
}
