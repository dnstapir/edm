package cmd

import (
	"log/slog"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile        string
	edmLogger      *slog.Logger
	edmLoggerLevel *slog.LevelVar
	exitProcess    = os.Exit
	userHomeDir    = os.UserHomeDir

	viperSetConfigFile  = viper.SetConfigFile
	viperAddConfigPath  = viper.AddConfigPath
	viperSetConfigType  = viper.SetConfigType
	viperSetConfigName  = viper.SetConfigName
	viperAutomaticEnv   = viper.AutomaticEnv
	viperReadInConfig   = viper.ReadInConfig
	viperConfigFileUsed = viper.ConfigFileUsed
	viperWatchConfig    = viper.WatchConfig
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "dnstapir-edm",
	Short: "dnstap(ir) minimiser",
	Long: `dnstapir-edm is a tool for reading dnstap data, pseudonymizing IP addresses and
outputting minimised output data.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	// Run: func(cmd *cobra.Command, args []string) { },
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute(logger *slog.Logger, loggerLevel *slog.LevelVar) {
	// Set global variables so it can be used from run.go
	edmLogger = logger
	edmLoggerLevel = loggerLevel
	err := rootCmd.Execute()
	if err != nil {
		exitProcess(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config-file", "", "config file for sensitive information (default is $HOME/.dnstapir-edm.toml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	// rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viperSetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := userHomeDir()
		cobra.CheckErr(err)

		// Search config in home directory with name ".edm" (without extension).
		viperAddConfigPath(home)
		viperSetConfigType("toml")
		viperSetConfigName(".dnstapir-edm")
	}

	viperAutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viperReadInConfig(); err == nil {
		edmLogger.Info("using config file", "filename", viperConfigFileUsed())
	}

	// Make it so we can detect changes to the cryptopan secret in the config
	viperWatchConfig()
}
