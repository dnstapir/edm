package cmd

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
)

// envPrefix namespaces the environment variables read by [Execute]: every
// flag of the "run" command can be set via envPrefix + "_" + the flag name
// upper-cased with "-" replaced by "_".
const envPrefix = "DNSTAPIR_EDM"

var (
	edmLogger      *slog.Logger
	edmLoggerLevel *slog.LevelVar

	// Seams for tests.
	exitProcess = os.Exit
	userHomeDir = os.UserHomeDir
	osArgs      = func() []string { return os.Args }
)

// errUnknownCommand is returned by dispatch for an unrecognized subcommand.
var errUnknownCommand = errors.New("unknown command")

// Execute parses the command line and dispatches to the matching subcommand.
//
// It is called by main.main(). On any error it terminates the process with
// exit code 1; errors are reported on stderr (or the logger) before exiting.
func Execute(logger *slog.Logger, loggerLevel *slog.LevelVar) {
	edmLogger = logger
	edmLoggerLevel = loggerLevel
	if err := dispatch(osArgs()[1:], os.Stdout, os.Stderr); err != nil {
		exitProcess(1)
	}
}

// dispatch routes args (os.Args[1:]) to a subcommand.
//
// Root-level flags (--config-file) may appear before the subcommand,
// matching the systemd unit's "dnstapir-edm --config-file X run" invocation;
// the "run" command also accepts --config-file after the subcommand, which
// takes precedence. A bare invocation or "help" prints usage and succeeds;
// an unrecognized subcommand prints usage on errW and returns an error
// wrapping [errUnknownCommand].
func dispatch(args []string, outW, errW io.Writer) (err error) {
	rootFS := flag.NewFlagSet("dnstapir-edm", flag.ContinueOnError)
	rootFS.SetOutput(errW)
	var rootCfgFile string
	rootFS.StringVar(&rootCfgFile, "config-file", "", "config file for sensitive information (default is $HOME/.dnstapir-edm.toml)")
	rootFS.Usage = func() { printUsage(errW, rootFS) }

	// Stdlib flag parsing stops at the first non-flag argument, leaving the
	// subcommand and its flags in rootFS.Args().
	err = rootFS.Parse(args)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}

	rest := rootFS.Args()
	if len(rest) == 0 || rest[0] == "help" {
		printUsage(outW, rootFS)
		return nil
	}

	switch rest[0] {
	case "run":
		err = runRun(rest[1:], rootCfgFile, errW)
	default:
		fmt.Fprintf(errW, "unknown command %q\n\n", rest[0])
		printUsage(errW, rootFS)
		err = fmt.Errorf("%w: %q", errUnknownCommand, rest[0])
	}
	return
}

// printUsage writes the top-level help text: the tool description, the
// available commands and the root flags.
func printUsage(w io.Writer, rootFS *flag.FlagSet) {
	fmt.Fprintln(w, `dnstapir-edm is a tool for reading dnstap data, pseudonymizing IP addresses and
outputting minimised output data.

Usage:
  dnstapir-edm [flags] <command> [command flags]

Commands:
  run     Run dnstapir-edm in dnstap capture mode
  help    Show this help text

Flags:`)
	fs := rootFS
	fs.SetOutput(w)
	fs.PrintDefaults()
}

// resolveConfigPath returns the config file to use: explicit when non-empty,
// otherwise $HOME/.dnstapir-edm.toml.
func resolveConfigPath(explicit string) (path string, err error) {
	if explicit != "" {
		return explicit, nil
	}
	var home string
	home, err = userHomeDir()
	if err == nil {
		path = filepath.Join(home, ".dnstapir-edm.toml")
	}
	return
}
