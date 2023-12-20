package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/pool"
	"github.com/nspcc-dev/neofs-sdk-go/user"
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

	zap.ReplaceGlobals(l)

	if devConf.Enabled {
		devServer(app, devConf)
	} else {
		server(app)
	}
}

func newHandler(ctx context.Context, l *zap.Logger, v *viper.Viper, sftpConfig *handlers.SftpServerConfig) *handlers.App {
	var (
		reBalance  = defaultRebalanceTimer
		conTimeout = defaultConnectTimeout
		reqTimeout = defaultRequestTimeout
		poolPeers  = fetchPeers(l, v)
	)

	if val := v.GetDuration(cfgConnectTimeout); val > 0 {
		conTimeout = val
	} else {
		l.Warn("invalid connection_timeout, default one will be used", zap.Duration("default", defaultConnectTimeout))
	}
	if val := v.GetDuration(cfgRequestTimeout); val > 0 {
		reqTimeout = val
	} else {
		l.Warn("invalid request_timeout, default one will be used", zap.Duration("default", defaultRequestTimeout))
	}
	if val := v.GetDuration(cfgRebalanceTimer); val > 0 {
		reBalance = val
	} else {
		l.Warn("invalid rebalance_timeout, default one will be used", zap.Duration("default", defaultRebalanceTimer))
	}

	password := wallet.GetPassword(v, cfgWalletPassphrase)
	key, err := wallet.GetKeyFromPath(v.GetString(cfgWallet), v.GetString(cfgAddress), password)
	if err != nil {
		l.Fatal("could not load NeoFS private key", zap.Error(err))
	}

	l.Info("using credentials", zap.String("NeoFS", hex.EncodeToString(key.PublicKey().Bytes())))

	signer := user.NewAutoIDSignerRFC6979(key.PrivateKey)
	ownerID := signer.UserID()

	var prm pool.InitParameters
	prm.SetSigner(signer)
	prm.SetNodeDialTimeout(conTimeout)
	prm.SetHealthcheckTimeout(reqTimeout)
	prm.SetClientRebalanceInterval(reBalance)

	for _, peer := range poolPeers {
		prm.AddNode(peer)
	}

	conns, err := pool.NewPool(prm)
	if err != nil {
		l.Fatal("failed to create connection pool", zap.Error(err))
	}

	if err = conns.Dial(ctx); err != nil {
		l.Fatal("failed to dial connection pool", zap.Error(err))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ni, err := conns.NetworkInfo(ctx, client.PrmNetworkInfo{})
	if err != nil {
		l.Fatal("failed to get network info", zap.Error(err))
	}

	return handlers.NewApp(conns, signer, &ownerID, l, sftpConfig, ni.MaxObjectSize(), v.GetString(cfgNeoFSContainerPolicy))
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

	privateBytes, err := os.ReadFile(devConf.SSHKeyPath)
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
