package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-sdk-go/pool"
	"github.com/nspcc-dev/neofs-sftp-gw/handlers"
	"github.com/nspcc-dev/neofs-sftp-gw/internal/wallet"
	"github.com/pkg/sftp"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"golang.org/x/crypto/ssh"
)

func main() {
	v, sftpConfig, devConf := newSettings()
	l := newLogger(v, sftpConfig)
	g, _ := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	app := newHandler(g, l, v, sftpConfig)

	if devConf.Enabled {
		devServer(app, devConf)
	} else {
		server(app)
	}
}

func newHandler(ctx context.Context, l *zap.Logger, v *viper.Viper, sftpConfig *handlers.SftpServerConfig) *handlers.App {
	var (
		conns pool.Pool
		key   *keys.PrivateKey
		err   error

		reBalance  = defaultRebalanceTimer
		conTimeout = defaultConnectTimeout
		reqTimeout = defaultRequestTimeout
		poolPeers  = fetchPeers(l, v)
	)

	if v := v.GetDuration(cfgConnectTimeout); v > 0 {
		conTimeout = v
	} else {
		l.Warn("invalid connection_timeout, default one will be used", zap.Duration("default", defaultConnectTimeout))
	}
	if v := v.GetDuration(cfgRequestTimeout); v > 0 {
		reqTimeout = v
	} else {
		l.Warn("invalid request_timeout, default one will be used", zap.Duration("default", defaultRequestTimeout))
	}
	if v := v.GetDuration(cfgRebalanceTimer); v > 0 {
		reBalance = v
	} else {
		l.Warn("invalid rebalance_timeout, default one will be used", zap.Duration("default", defaultRebalanceTimer))
	}

	password := wallet.GetPassword(v, cfgWalletPassphrase)
	if key, err = wallet.GetKeyFromPath(v.GetString(cfgWallet), v.GetString(cfgAddress), password); err != nil {
		l.Fatal("could not load NeoFS private key", zap.Error(err))
	}

	l.Info("using credentials", zap.String("NeoFS", hex.EncodeToString(key.PublicKey().Bytes())))

	opts := &pool.BuilderOptions{
		Key:                     &key.PrivateKey,
		NodeConnectionTimeout:   conTimeout,
		NodeRequestTimeout:      reqTimeout,
		ClientRebalanceInterval: reBalance,
	}
	conns, err = poolPeers.Build(ctx, opts)
	if err != nil {
		l.Fatal("failed to create connection pool", zap.Error(err))
	}

	return handlers.NewApp(conns, l, sftpConfig)
}

func server(app *handlers.App) {
	svr := sftp.NewRequestServer(
		struct {
			io.Reader
			io.WriteCloser
		}{
			os.Stdin,
			os.Stdout,
		},
		sftp.Handlers{
			FileGet:  app,
			FilePut:  app,
			FileCmd:  app,
			FileList: app,
		},
	)

	if err := svr.Serve(); err == io.EOF {
		if err2 := svr.Close(); err2 != nil {
			app.Log.Fatal("sftp server completed with error:", zap.Error(err2))
		}
		app.Log.Info("sftp client exited session.")
	} else if err != nil {
		app.Log.Fatal("sftp server completed with error:", zap.Error(err))
	}
}

func devServer(app *handlers.App, devConf devConfig) {
	config := &ssh.ServerConfig{
		PasswordCallback: func(c ssh.ConnMetadata, pass []byte) (*ssh.Permissions, error) {
			app.Log.Debug("Login", zap.String("user", c.User()))
			if c.User() == "test" && string(pass) == "test" {
				return nil, nil
			}
			return nil, fmt.Errorf("password rejected for %q", c.User())
		},
	}

	privateBytes, err := ioutil.ReadFile(devConf.SSHKeyPath)
	if err != nil {
		app.Log.Fatal("Failed to load private key", zap.Error(err))
	}

	private, err := ssh.ParsePrivateKeyWithPassphrase(privateBytes, []byte(devConf.Passphrase))
	if err != nil {
		app.Log.Fatal("Failed to parse private key", zap.Error(err))
	}
	config.AddHostKey(private)

	listener, err := net.Listen("tcp", devConf.Address)
	if err != nil {
		app.Log.Fatal("failed to listen for connection", zap.Error(err))
	}
	app.Log.Info("Listening", zap.String("address", listener.Addr().String()))

	nConn, err := listener.Accept()
	if err != nil {
		app.Log.Fatal("failed to accept incoming connection", zap.Error(err))
	}

	_, chans, reqs, err := ssh.NewServerConn(nConn, config)
	if err != nil {
		app.Log.Fatal("failed to handshake", zap.Error(err))
	}

	// The incoming Request channel must be serviced.
	go ssh.DiscardRequests(reqs)

	for newChannel := range chans {
		app.Log.Debug("Incoming channel", zap.String("channel type", newChannel.ChannelType()))
		if newChannel.ChannelType() != "session" {
			if err := newChannel.Reject(ssh.UnknownChannelType, "unknown channel type"); err != nil {
				app.Log.Error("reject error", zap.Error(err))
			}
			app.Log.Warn("Unknown channel type", zap.String("type", newChannel.ChannelType()))
			continue
		}
		channel, requests, err := newChannel.Accept()
		if err != nil {
			app.Log.Fatal("could not accept channel.", zap.Error(err))
		}
		app.Log.Debug("Channel accepted")

		go func(in <-chan *ssh.Request) {
			for req := range in {
				ok := false
				switch req.Type {
				case "subsystem":
					if string(req.Payload[4:]) == "sftp" {
						ok = true
					}
				}
				if err := req.Reply(ok, nil); err != nil {
					app.Log.Error("reply error", zap.Error(err))
				}
			}
		}(requests)

		server := sftp.NewRequestServer(channel, sftp.Handlers{
			FileGet:  app,
			FilePut:  app,
			FileCmd:  app,
			FileList: app,
		})

		if err := server.Serve(); err == io.EOF {
			if err2 := server.Close(); err2 != nil {
				app.Log.Fatal("sftp server close error", zap.Error(err2))
			}
			app.Log.Info("sftp client exited session.")
		} else if err != nil {
			app.Log.Fatal("sftp server completed with error:", zap.Error(err))
		}
	}
}
