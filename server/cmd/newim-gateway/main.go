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
	"github.com/m-ice/NewIM/server/friend"
	"github.com/m-ice/NewIM/server/friendhttp"
	"github.com/m-ice/NewIM/server/gateway"
	"github.com/m-ice/NewIM/server/group"
	"github.com/m-ice/NewIM/server/grouphttp"
	"github.com/m-ice/NewIM/server/media"
	"github.com/m-ice/NewIM/server/media/localfs"
	"github.com/m-ice/NewIM/server/media/s3store"
	"github.com/m-ice/NewIM/server/mediahttp"
	messageservice "github.com/m-ice/NewIM/server/message"
	"github.com/m-ice/NewIM/server/push"
	"github.com/m-ice/NewIM/server/read"
	"github.com/m-ice/NewIM/server/readhttp"
	"github.com/m-ice/NewIM/server/storage/authsession"
	"github.com/m-ice/NewIM/server/storage/conversationsync"
	friendstore "github.com/m-ice/NewIM/server/storage/friend"
	groupstore "github.com/m-ice/NewIM/server/storage/group"
	mediastore "github.com/m-ice/NewIM/server/storage/media"
	messagestore "github.com/m-ice/NewIM/server/storage/message"
	"github.com/m-ice/NewIM/server/storage/messagesync"
	"github.com/m-ice/NewIM/server/storage/postgresidentity"
	pushstore "github.com/m-ice/NewIM/server/storage/push"
	readstore "github.com/m-ice/NewIM/server/storage/read"
	userstore "github.com/m-ice/NewIM/server/storage/user"
	syncconversation "github.com/m-ice/NewIM/server/sync/conversation"
	"github.com/m-ice/NewIM/server/sync/conversationhttp"
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
	if cfg.gateway.TrackSubscriptions {
		if err = authRepo.CheckGatewaySubscriptionSchema(startup); err != nil {
			return errors.New("newim-gateway subscription storage unavailable")
		}
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

	var pushRegistration *push.RegistrationHandler
	if limits := cfg.pushRegistration; limits != nil {
		vault, err := push.LoadVault(cfg.pushKeysFile)
		if err != nil {
			return errConfig
		}
		repo, err := pushstore.Open(startup, pushstore.Config{
			DSN: cfg.dsn, AllowLocalSocket: cfg.allowLocalSocket,
			ApplicationName: "newim-gateway-push", Guard: guard,
		}, vault)
		if err != nil {
			return errors.New("newim-gateway push registration storage unavailable")
		}
		defer repo.Close()
		pushRegistration, err = push.NewRegistrationHandler(auth, repo, gateway.NewProfileBudget(limits.ip, limits.account))
		if err != nil {
			return errConfig
		}
	}

	var profiles *userhttp.Handler
	var friends *friendhttp.Handler
	var groups *grouphttp.Handler
	var messageRoutes *gateway.MessageRoutes
	var conversationRoutes *conversationhttp.Handler
	var readRoutes *readhttp.Handler
	var mediaRoutes *mediahttp.Handler
	var mediaValidator messageservice.MediaValidator
	if limits := cfg.profiles; limits != nil {
		profileRepo, err := userstore.Open(startup, userstore.Config{
			DSN: cfg.dsn, AllowLocalSocket: cfg.allowLocalSocket,
			ApplicationName: "newim-gateway-profiles", Guard: guard,
			ProjectConversations: cfg.projectConversations,
			ProjectReadState:     cfg.projectReadState,
		})
		if err != nil {
			return errors.New("newim-gateway profile storage unavailable")
		}
		defer profileRepo.Close()
		service, err := user.NewService(profileRepo)
		if err != nil {
			return errConfig
		}
		profiles, err = userhttp.New(service, auth, gateway.NewProfileBudget(limits.ip, limits.account))
		if err != nil {
			return errConfig
		}
	}
	if limits := cfg.friends; limits != nil {
		friendRepo, err := friendstore.Open(startup, friendstore.Config{
			DSN: cfg.dsn, AllowLocalSocket: cfg.allowLocalSocket,
			ApplicationName: "newim-gateway-friends", Guard: guard,
		})
		if err != nil {
			return errors.New("newim-gateway friend storage unavailable")
		}
		defer friendRepo.Close()
		service, err := friend.NewService(friendRepo)
		if err != nil {
			return errConfig
		}
		friends, err = friendhttp.New(service, auth, gateway.NewProfileBudget(limits.ip, limits.account))
		if err != nil {
			return errConfig
		}
	}
	if limits := cfg.media; limits != nil {
		objects, err := openMediaObjects(startup, cfg)
		if err != nil {
			return errors.New("newim-gateway media object storage unavailable")
		}
		defer objects.Close()
		repo, err := mediastore.Open(startup, mediastore.Config{
			DSN: cfg.dsn, AllowLocalSocket: cfg.allowLocalSocket,
			ApplicationName: "newim-gateway-media", Guard: guard,
		})
		if err != nil {
			return errors.New("newim-gateway media metadata storage unavailable")
		}
		defer repo.Close()
		if err = repo.CheckSchema(startup); err != nil {
			return errors.New("newim-gateway media metadata schema unavailable")
		}
		service, err := media.NewService(repo, media.Config{
			Objects: objects, ObjectReader: objects, ObjectTimeout: mediahttp.ObjectTimeout, Clock: media.ClockFunc(time.Now),
		})
		if err != nil {
			return errConfig
		}
		mediaRoutes, err = mediahttp.New(service, auth, gateway.NewProfileBudget(limits.ip, limits.account))
		if err != nil {
			return errConfig
		}
		mediaValidator = service
	}
	if limits := cfg.privateMessages; limits != nil {
		sendRepo, err := messagestore.Open(startup, messagestore.Config{
			DSN: cfg.dsn, AllowLocalSocket: cfg.allowLocalSocket,
			ApplicationName: "newim-gateway-send", Guard: guard,
			PrivatePolicy: true, DirectOnly: cfg.groups == nil, ManagedGroups: cfg.groups != nil,
			StrangerPolicy:       cfg.strangerPolicy,
			ProjectConversations: cfg.projectConversations,
			ProjectReadState:     cfg.projectReadState,
		})
		if err != nil {
			return errors.New("newim-gateway send storage unavailable")
		}
		defer sendRepo.Close()
		sender, err := messageservice.NewService(sendRepo, messageservice.Config{
			IDs:   messageservice.NewCSPRNGIDGenerator(),
			Clock: messageservice.ClockFunc(time.Now), Observer: sendObserver{},
			MediaValidator: mediaValidator,
		})
		if err != nil {
			return errConfig
		}
		newMessageRoutes := gateway.NewMessageRoutes
		if mediaRoutes != nil {
			newMessageRoutes = gateway.NewMessageRoutesWithMedia
		}
		messageRoutes, err = newMessageRoutes(auth, sender, sendRepo, gateway.NewMessageBudget(limits.ip, limits.account, cfg.messageDeviceLimit))
		if err != nil {
			return errConfig
		}
	}
	if limits := cfg.groups; limits != nil {
		groupRepo, err := groupstore.Open(startup, groupstore.Config{
			DSN: cfg.dsn, AllowLocalSocket: cfg.allowLocalSocket,
			ApplicationName: "newim-gateway-groups", Guard: guard,
			ProjectConversations: cfg.projectConversations,
			ProjectReadState:     cfg.projectReadState,
		})
		if err != nil {
			return errors.New("newim-gateway group storage unavailable")
		}
		defer groupRepo.Close()
		service, err := group.NewService(groupRepo)
		if err != nil {
			return errConfig
		}
		groups, err = grouphttp.New(service, auth, gateway.NewProfileBudget(limits.ip, limits.account))
		if err != nil {
			return errConfig
		}
	}
	if limits := cfg.conversationSync; limits != nil {
		repository, err := conversationsync.Open(startup, conversationsync.Config{
			DSN: cfg.dsn, AllowLocalSocket: cfg.allowLocalSocket,
			ApplicationName: "newim-gateway-sync", Guard: guard, RequireSourceReady: true,
			IncludeReadState: cfg.projectReadState, ExposeAccountSettings: cfg.accountSettings,
		})
		if err != nil {
			return errors.New("newim-gateway conversation sync storage unavailable")
		}
		defer repository.Close()
		activeKeyID, keys, err := conversationhttp.LoadKeyring(cfg.syncKeysFile)
		if err != nil {
			return errConfig
		}
		service, err := syncconversation.NewService(repository, syncconversation.Config{
			ActiveKeyID: activeKeyID, Keys: keys,
		})
		for _, key := range keys {
			clear(key)
		}
		if err != nil {
			return errConfig
		}
		budget := gateway.NewProfileBudget(limits.ip, limits.account)
		if cfg.accountSettings {
			conversationRoutes, err = conversationhttp.NewWithAccountSettings(service, messages, auth, budget, repository)
		} else {
			conversationRoutes, err = conversationhttp.NewWithMessages(service, messages, auth, budget)
		}
		if err != nil {
			return errConfig
		}
	}
	if limits := cfg.readWatermark; limits != nil {
		repository, err := readstore.Open(startup, readstore.Config{
			DSN: cfg.dsn, AllowLocalSocket: cfg.allowLocalSocket,
			ApplicationName: "newim-gateway-read", Guard: guard,
			ProjectReadState: cfg.projectReadState,
		})
		if err != nil {
			return errors.New("newim-gateway read watermark storage unavailable")
		}
		defer repository.Close()
		service, err := read.NewService(repository)
		if err != nil {
			return errConfig
		}
		readRoutes, err = readhttp.New(service, auth, gateway.NewProfileBudget(limits.ip, limits.account))
		if err != nil {
			return errConfig
		}
	}
	routes := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.EscapedPath()
		switch {
		case pushRegistration != nil && path == push.RegistrationRoute:
			pushRegistration.ServeHTTP(w, r)
		case profiles != nil && userhttp.ContainsRoute(path):
			profiles.ServeHTTP(w, r)
		case friends != nil && friendhttp.ContainsRoute(path):
			friends.ServeHTTP(w, r)
		case groups != nil && grouphttp.ContainsRoute(path):
			groups.ServeHTTP(w, r)
		case mediaRoutes != nil && mediahttp.ContainsRoute(path):
			mediaRoutes.ServeHTTP(w, r)
		case conversationRoutes != nil && conversationhttp.ContainsRoute(path):
			conversationRoutes.ServeHTTP(w, r)
		case readRoutes != nil && readhttp.ContainsRoute(path):
			readRoutes.ServeHTTP(w, r)
		case messageRoutes != nil && gateway.ContainsMessageRoute(path):
			messageRoutes.ServeHTTP(w, r)
		default:
			handler.ServeHTTP(w, r)
		}
	})
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

type mediaObjects interface {
	media.ObjectStore
	media.ObjectReader
	Close() error
}

func openMediaObjects(ctx context.Context, cfg config) (mediaObjects, error) {
	if cfg.mediaS3 != nil {
		return s3store.Open(ctx, *cfg.mediaS3)
	}
	return localfs.Open(cfg.mediaRoot)
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

type sendObserver struct{}

func (sendObserver) Observe(messageservice.Observation) {}

type messageObserver struct{}

func (messageObserver) Observe(syncmessage.Observation) {}

// net/http errors can embed untrusted request or driver data. The gateway logs
// its own startup, database identity and drain outcomes.
type discardHTTPError struct{}

func (discardHTTPError) Write(p []byte) (int, error) { return len(p), nil }
