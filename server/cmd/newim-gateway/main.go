package main

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/m-ice/NewIM/server/auth/session"
	"github.com/m-ice/NewIM/server/gateway"
	"github.com/m-ice/NewIM/server/storage/authsession"
	"github.com/m-ice/NewIM/server/storage/messagesync"
	"github.com/m-ice/NewIM/server/storage/postgresidentity"
	userstore "github.com/m-ice/NewIM/server/storage/user"
	syncmessage "github.com/m-ice/NewIM/server/sync/message"
	"github.com/m-ice/NewIM/server/user"
	"github.com/m-ice/NewIM/server/userhttp"
)

func main() {
	logger := log.New(os.Stderr, "", 0)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg, err := readConfig(os.Getenv)
	if err == nil {
		err = run(ctx, cfg, logger)
	}
	if err != nil {
		logger.Print(err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config, logger *log.Logger) error {
	cert, err := tls.LoadX509KeyPair(cfg.certFile, cfg.keyFile)
	if err != nil {
		return errors.New("newim-gateway TLS configuration unavailable")
	}
	terminal := make(chan struct{})
	guard := postgresidentity.NewGuard(func(error) { close(terminal) })
	startup, cancelStartup := context.WithTimeout(ctx, 10*time.Second)
	defer cancelStartup()
	authRepo, err := authsession.Open(startup, authsession.Config{
		DSN: cfg.dsn, AllowLocalSocket: cfg.allowLocalSocket, ApplicationName: "newim-gateway-auth",
		AdmissionRequired: true, Guard: guard,
	})
	if err != nil {
		return errors.New("newim-gateway session storage unavailable")
	}
	defer authRepo.Close()
	if err = authRepo.CheckGatewaySchema(startup); err != nil {
		return errors.New("newim-gateway handshake storage unavailable")
	}
	messageRepo, err := messagesync.Open(startup, messagesync.Config{
		DSN: cfg.dsn, AllowLocalSocket: cfg.allowLocalSocket, ApplicationName: "newim-gateway-messages", Guard: guard,
	})
	if err != nil {
		return errors.New("newim-gateway message storage unavailable")
	}
	defer messageRepo.Close()

	auth, err := session.NewService(authRepo, session.Config{Clock: session.ClockFunc(time.Now), Observer: authObserver{}})
	if err != nil {
		return errConfig
	}
	messages, err := syncmessage.NewService(messageRepo, syncmessage.Config{Observer: messageObserver{}})
	if err != nil {
		return errConfig
	}
	handler, err := gateway.New(auth, messages, authRepo, cfg.gateway)
	if err != nil {
		return errConfig
	}

	var routes http.Handler = handler
	if limits := cfg.profiles; limits != nil {
		profileRepo, err := userstore.Open(startup, userstore.Config{
			DSN: cfg.dsn, AllowLocalSocket: cfg.allowLocalSocket,
			ApplicationName: "newim-gateway-profiles", Guard: guard,
		})
		if err != nil {
			return errors.New("newim-gateway profile storage unavailable")
		}
		defer profileRepo.Close()
		service, err := user.NewService(profileRepo)
		if err != nil {
			return errConfig
		}
		profiles, err := userhttp.New(service, auth, gateway.NewProfileBudget(limits.ip, limits.account))
		if err != nil {
			return errConfig
		}
		routes = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if userhttp.ContainsRoute(r.URL.EscapedPath()) {
				profiles.ServeHTTP(w, r)
				return
			}
			handler.ServeHTTP(w, r)
		})
	}
	cancelStartup()

	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.addr)
	if err != nil {
		return errors.New("newim-gateway listener unavailable")
	}
	defer listener.Close()
	tlsListener := tls.NewListener(limitListener(listener, cfg.gateway.MaxConnections+32), &tls.Config{
		MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert},
		NextProtos: []string{"http/1.1"},
	})
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	server := &http.Server{
		Handler: routes, ConnContext: gateway.ConnContext, Protocols: protocols,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second,
		WriteTimeout: 10 * time.Second, IdleTimeout: 15 * time.Second,
		MaxHeaderBytes: 8192,
		ErrorLog:       log.New(discardHTTPError{}, "", 0),
	}

	maintenance, cancelMaintenance := context.WithCancel(ctx)
	pruned := make(chan struct{})
	go func() {
		defer close(pruned)
		prune(maintenance, authRepo, logger)
	}()
	served := make(chan error, 1)
	go func() { served <- server.Serve(tlsListener) }()
	logger.Printf("newim-gateway listening on %s", listener.Addr())

	var result error
	select {
	case <-ctx.Done():
	case <-terminal:
		result = errors.New("newim-gateway database identity changed")
	case err := <-served:
		if !errors.Is(err, http.ErrServerClosed) {
			result = errors.New("newim-gateway listener stopped")
		}
	}
	cancelMaintenance()
	handler.BeginDrain()
	drain, cancelDrain := context.WithTimeout(context.Background(), cfg.drainTimeout)
	defer cancelDrain()
	httpDrained := make(chan error, 1)
	go func() { httpDrained <- server.Shutdown(drain) }()
	if err := handler.Drain(drain); err != nil {
		logger.Print("newim-gateway connection drain deadline reached")
	}
	if err := <-httpDrained; err != nil {
		_ = server.Close()
	}
	<-pruned
	return result
}

func prune(ctx context.Context, repo *authsession.Repository, logger *log.Logger) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		removed, err := repo.PruneGatewayHandshakes(ctx, time.Now(), authsession.MaxGatewayHandshakePrune)
		delay := time.Minute
		if err != nil && ctx.Err() == nil {
			logger.Print("newim-gateway handshake cleanup unavailable")
		}
		if removed == authsession.MaxGatewayHandshakePrune {
			delay = 100 * time.Millisecond
		}
		timer.Reset(delay)
	}
}

type authObserver struct{}

func (authObserver) Observe(session.Observation) {}

type messageObserver struct{}

func (messageObserver) Observe(syncmessage.Observation) {}

// net/http errors can embed untrusted request or driver data. The gateway logs
// its own startup, database identity and drain outcomes.
type discardHTTPError struct{}

func (discardHTTPError) Write(p []byte) (int, error) { return len(p), nil }
