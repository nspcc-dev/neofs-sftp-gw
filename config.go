package main

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/nspcc-dev/neofs-sdk-go/pool"
	"github.com/nspcc-dev/neofs-sftp-gw/handlers"
	"github.com/nspcc-dev/neofs-sftp-gw/internal/version"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

type devConfig struct {
	Enabled    bool
	SSHKeyPath string
	Passphrase string
	Address    string
}

const (
	defaultRebalanceTimer = 15 * time.Second
	defaultRequestTimeout = 15 * time.Second
	defaultConnectTimeout = 30 * time.Second
)

const (
	// Wallet.
	cfgWallet           = "wallet.path"
	cfgAddress          = "wallet.address"
	cfgWalletPassphrase = "wallet.passphrase"

	// Timeouts.
	cfgConnectTimeout = "connection.connect_timeout"
	cfgRequestTimeout = "connection.request_timeout"
	cfgRebalanceTimer = "connection.rebalance_timer"

	// Peers.
	cfgPeers = "peers"

	// User enabling.
	cfgUserEnabled = "user.enabled"
	cfgUserPath    = "user.path"

	// Dev variables.
	cfgDevEnabled       = "dev.enabled"
	cfgDevListenAddress = "dev.address"
	cfgDevSSHKey        = "dev.sshkey"
	cfgDevSSHPassphrase = "dev.passphrase"

	// Command line args.
	cfgConfigPath = "config"

	// envPrefix is environment variables prefix used for configuration.
	envPrefix = "SFTP_GW"

	configType = "yaml"

	cfgNeoFSContainerPolicy = "neofs.container.policy"
)

func fetchPeers(l *zap.Logger, v *viper.Viper) []pool.NodeParam {
	var peers []pool.NodeParam

	for i := 0; ; i++ {
		key := cfgPeers + "." + strconv.Itoa(i) + "."
		address := v.GetString(key + "address")
		weight := v.GetFloat64(key + "weight")
		priority := v.GetInt(key + "priority")

		if address == "" {
			l.Warn("skip, empty address")
			break
		}
		if weight <= 0 { // unspecified or wrong
			l.Warn("invalid weight, default 1 will be used",
				zap.Float64("weight", weight),
				zap.String("address", address))
			weight = 1
		}
		if priority <= 0 { // unspecified or wrong
			l.Warn("invalid priority, default 1 will be used",
				zap.Int("priority", priority),
				zap.String("address", address))
			priority = 1
		}
		peers = append(peers, pool.NewNodeParam(priority, address, weight))

		l.Info("added connection peer",
			zap.String("address", address),
			zap.Float64("weight", weight))
	}

	return peers
}

func newSettings() (*viper.Viper, *handlers.SftpServerConfig, devConfig) {
	v := viper.New()

	v.AutomaticEnv()
	v.SetEnvPrefix(envPrefix)
	v.SetConfigType(configType)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AllowEmptyEnv(true)

	// flags setup:
	flags := pflag.NewFlagSet("commandline", pflag.ExitOnError)
	flags.SetOutput(os.Stderr)
	flags.SortFlags = false

	sftpConfig := &handlers.SftpServerConfig{}
	flags.BoolVarP(&sftpConfig.ReadOnly, "read-only", "R", false, "read-only server")
	flags.BoolVarP(&sftpConfig.DebugStderr, "debug-stderr", "e", false, "debug to stderr")
	flags.StringVarP(&sftpConfig.DebugLevel, "debug-level", "l", "ERROR", "debug level")
	versionFlag := flags.BoolP("version", "v", false, "show version")

	config := flags.String(cfgConfigPath, "", "config path")

	// dev section
	v.SetDefault(cfgDevListenAddress, "0.0.0.0:2022")
	v.SetDefault(cfgDevEnabled, false)

	// user section
	v.SetDefault(cfgUserEnabled, false)

	// main section
	setDefaults(v)

	if err := v.BindPFlags(flags); err != nil {
		panic(err)
	}
	if err := flags.Parse(os.Args); err != nil {
		panic(err)
	}

	if versionFlag != nil && *versionFlag {
		fmt.Printf("NeoFS SFTP Gateway\nVersion: %s\nGoVersion: %s\n", version.Version, runtime.Version())
		os.Exit(0)
	}

	if !v.IsSet(cfgConfigPath) {
		panic("no config provided")
	}

	cfgBuff := bytes.NewBuffer(nil)
	file, err := os.ReadFile(*config)
	if err != nil {
		panic(err)
	}

	expanded := os.ExpandEnv(string(file))
	cfgBuff.WriteString(expanded)

	if err := v.ReadConfig(cfgBuff); err != nil {
		panic(err)
	}

	devConf := devConfig{
		Enabled:    v.GetBool(cfgDevEnabled),
		SSHKeyPath: v.GetString(cfgDevSSHKey),
		Passphrase: v.GetString(cfgDevSSHPassphrase),
		Address:    v.GetString(cfgDevListenAddress),
	}
	userV := viper.New()
	userV.SetConfigType(configType)
	setDefaults(userV)
	if !v.GetBool(cfgUserEnabled) || !v.IsSet(cfgUserPath) {
		userV = v
	} else {
		userConfigPath := v.GetString(cfgUserPath)
		if cfgFile, err := os.Open(userConfigPath); err != nil {
			fmt.Fprintln(os.Stderr, err)
			userV = v
		} else if err := userV.ReadConfig(cfgFile); err != nil {
			panic(err)
		}
	}

	return userV, sftpConfig, devConf
}

func setDefaults(v *viper.Viper) {
	v.SetDefault(cfgRequestTimeout, defaultRequestTimeout)
	v.SetDefault(cfgConnectTimeout, defaultConnectTimeout)
	v.SetDefault(cfgRebalanceTimer, defaultRebalanceTimer)
}

func newLogger(_ *viper.Viper, sftpConfig *handlers.SftpServerConfig) *zap.Logger {
	config := zap.NewProductionConfig()

	debugStream := "/dev/null"
	if sftpConfig.DebugStderr {
		debugStream = "stderr"
	}

	config.OutputPaths = []string{debugStream}
	config.ErrorOutputPaths = []string{debugStream}

	if err := config.Level.UnmarshalText([]byte(sftpConfig.DebugLevel)); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}

	l, err := config.Build()
	if err != nil {
		panic(err)
	}

	return l
}
