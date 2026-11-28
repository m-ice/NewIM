package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/m-ice/NewIM/server/api"
	"github.com/m-ice/NewIM/server/buildinfo"
)

// startMetrics starts the optional loopback-only scrape listener.
func startMetrics(ctx context.Context, addr string, registry *api.MetricsRegistry, logger *log.Logger, sources ...api.MetricsSource) (func(), error) {
	if addr == "" {
		return func() {}, nil
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, errors.New("newim-gateway metrics listener unavailable")
	}
	version := "unknown"
	if info, err := buildinfo.Read(); err == nil && info.ServerVersion != "" {
		version = info.ServerVersion
	}
	server := &http.Server{
		Handler:           api.NewOperationalMetricsHandler(version, registry, sources...),
		ReadHeaderTimeout: 3 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 10 * time.Second,
		MaxHeaderBytes: 4096, ErrorLog: log.New(discardHTTPError{}, "", 0),
	}
	logger.Printf("newim-gateway metrics listening on %s", listener.Addr())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Print("newim-gateway metrics listener stopped")
		}
	}()
	return func() {
		_ = server.Close()
		<-done
	}, nil
}
