package cmd

import (
	"log"

	"github.com/dnstapir/edm/pkg/runner"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run edm in dnstap capture mode",
	Run: func(_ *cobra.Command, _ []string) {
		runner.Run(edmLogger, edmLoggerLevel)
	},
}

func init() {
	rootCmd.AddCommand(runCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// runCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// runCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	runCmd.Flags().Bool("disable-session-files", false, "do not write out session parquet files")
	runCmd.Flags().Bool("disable-histogram-sender", false, "do not check for histogram files to upload to core")
	runCmd.Flags().Bool("disable-mqtt", false, "disable MQTT message sending")
	runCmd.Flags().Bool("disable-mqtt-filequeue", false, "disable MQTT file based queue")

	runCmd.Flags().String("input-unix", "", "create unix socket for reading dnstap (e.g. /var/lib/unbound/dnstap.sock)")
	runCmd.Flags().String("input-tcp", "", "create TCP socket for reading dnstap (e.g. '127.0.0.1:53535')")
	runCmd.Flags().String("input-tls", "", "create TLS TCP socket for reading dnstap (e.g. '127.0.0.1:53535')")
	runCmd.MarkFlagsMutuallyExclusive("input-unix", "input-tcp", "input-tls")

	runCmd.Flags().String("input-tls-cert-file", "", "file containing cert used for TLS TCP socket")
	runCmd.Flags().String("input-tls-key-file", "", "file containing key used for TLS TCP socket")
	runCmd.MarkFlagsRequiredTogether("input-tls", "input-tls-cert-file", "input-tls-key-file")

	runCmd.Flags().String("input-tls-client-ca-file", "", "file containing CA used for client cert allowed to connect to TLS TCP socket")

	runCmd.Flags().String("cryptopan-key", "", "override the secret used for Crypto-PAn pseudonymization")

	runCmd.Flags().String("cryptopan-key-salt", "edm-kdf-salt-val", "the salt used for key derivation")
	runCmd.Flags().String("well-known-domains-file", "well-known-domains.dawg", "the DAWG file used for filtering well-known domains")
	runCmd.Flags().String("ignored-client-ips-file", "", "file containing a newline separated list of IPv4/IPv6 CIDRs of DNS clients that will be ignored")
	runCmd.Flags().String("ignored-question-names-file", "", "a DAWG file containing question section names that will be ignored")
	runCmd.Flags().String("data-dir", "/var/lib/edm", "directory where output data is written")
	runCmd.Flags().Int("minimiser-workers", 1, "how many minimiser workers to start (0 means same as GOMAXPROCS)")
	runCmd.Flags().String("mqtt-signing-key-file", "edm-mqtt-signer-key.pem", "ECSDSA key used for signing MQTT messages")
	runCmd.Flags().String("mqtt-client-key-file", "edm-mqtt-client-key.pem", "ECSDSA client key used for authenticating to MQTT bus")
	runCmd.Flags().String("mqtt-client-cert-file", "edm-mqtt-client.pem", "ECSDSA client cert used for authenticating to MQTT bus")
	runCmd.Flags().String("mqtt-server", "127.0.0.1:8883", "MQTT server we will publish events to")
	runCmd.Flags().String("mqtt-ca-file", "", "CA cert used for validating MQTT TLS connection, defaults to using OS CA certs")

	runCmd.Flags().Int("mqtt-keepalive", 30, "Keepalive interval for MQTT connection")
	runCmd.Flags().Int("qname-seen-entries", 10000000, "Number of 'seen' qnames stored in LRU cache, need to be changed based on RAM")
	runCmd.Flags().Int("cryptopan-address-entries", 10000000, "Number of cryptopan pseudonymised addresses stored in LRU cache, 0 disables the cache, need to be changed based on RAM")
	runCmd.Flags().Int("newqname-buffer", 1000, "Number of slots in new_qname publisher channel, if this is filled up we skip new_qname events")
	runCmd.Flags().Int("histogram-hll-explicit-threshold", 20, "When the number of unique IP addresses is beyond this threshold we will include HLL data for a domain in the histogram parquet file")
	runCmd.Flags().String("http-ca-file", "", "CA cert used for validating aggregate-receiver connection, defaults to using OS CA certs")
	runCmd.Flags().String("http-signing-key-file", "edm-http-signer-key.pem", "ECSDSA key used for signing HTTP messages to aggregate-receiver")
	runCmd.Flags().String("http-client-key-file", "edm-http-client-key.pem", "ECSDSA client key used for authenticating to aggregate-receiver")
	runCmd.Flags().String("http-client-cert-file", "edm-http-client.pem", "ECSDSA client cert used for authenticating to aggregate-receiver")
	runCmd.Flags().String("http-url", "https://127.0.0.1:8443", "Service we will POST aggregates to")

	// Debug options
	runCmd.Flags().Bool("debug", false, "print debug logging during operation")
	runCmd.Flags().String("debug-dnstap-filename", "", "File for dumping unmodified (sensitive) JSON-formatted dnstap packets we are about to process, for debugging")
	runCmd.Flags().Bool("debug-enable-blockprofiling", false, "Enable profiling of goroutine blocking events")
	runCmd.Flags().Bool("debug-enable-mutexprofiling", false, "Enable profiling of mutex contention events")

	err := viper.BindPFlags(runCmd.Flags())
	if err != nil {
		log.Fatal(err)
	}
}
