//go:build integration

// Command loss-wrapper forwards one message request to a real server, waits
// until the upstream response proves commit, then drops only that response.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

type wrapper struct {
	api     string
	ops     string
	client  *http.Client
	dropped atomic.Bool
}

func (w *wrapper) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	target := w.api
	if request.URL.Path == "/ready" || request.URL.Path == "/metrics" {
		target = w.ops
	}
	body, err := io.ReadAll(request.Body)
	if err != nil {
		http.Error(response, `{"error":{"code":"HTTP_INTERNAL_ERROR"}}`, http.StatusInternalServerError)
		return
	}
	upstream, err := http.NewRequestWithContext(request.Context(), request.Method, target+request.URL.RequestURI(), bytes.NewReader(body))
	if err != nil {
		http.Error(response, `{"error":{"code":"HTTP_INTERNAL_ERROR"}}`, http.StatusInternalServerError)
		return
	}
	upstream.Header = request.Header.Clone()
	upstream.ContentLength = int64(len(body))
	upstreamResult, err := w.client.Do(upstream)
	if err != nil {
		http.Error(response, `{"error":{"code":"SERVER_TEMPORARY_UNAVAILABLE"}}`, http.StatusServiceUnavailable)
		return
	}
	defer upstreamResult.Body.Close()
	upstreamBody, err := io.ReadAll(upstreamResult.Body)
	if err != nil {
		http.Error(response, `{"error":{"code":"HTTP_INTERNAL_ERROR"}}`, http.StatusInternalServerError)
		return
	}
	if request.Method == http.MethodPost && request.URL.Path == "/api/v1/messages" && w.dropped.CompareAndSwap(false, true) {
		hijacker, ok := response.(http.Hijacker)
		if !ok {
			http.Error(response, `{"error":{"code":"HTTP_INTERNAL_ERROR"}}`, http.StatusInternalServerError)
			return
		}
		connection, _, err := hijacker.Hijack()
		if err != nil {
			return
		}
		_ = connection.Close()
		return
	}
	copyHeaders(response.Header(), upstreamResult.Header)
	response.WriteHeader(upstreamResult.StatusCode)
	_, _ = response.Write(upstreamBody)
}

func copyHeaders(destination, source http.Header) {
	for key, values := range source {
		for _, value := range values {
			destination.Add(key, value)
		}
	}
}

func main() {
	apiAddr := flag.String("api-addr", "", "public API listener")
	opsAddr := flag.String("ops-addr", "", "public Ops listener")
	flag.Parse()
	apiUpstream := os.Getenv("NEWIM_LOSS_UPSTREAM_API")
	opsUpstream := os.Getenv("NEWIM_LOSS_UPSTREAM_OPS")
	if *apiAddr == "" || *opsAddr == "" || apiUpstream == "" || opsUpstream == "" {
		fmt.Fprintln(os.Stderr, "SERVER_INVALID_ARGUMENT")
		os.Exit(2)
	}
	handler := &wrapper{api: apiUpstream, ops: opsUpstream, client: &http.Client{Timeout: 8 * time.Second}}
	apiServer := &http.Server{Addr: *apiAddr, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	opsServer := &http.Server{Addr: *opsAddr, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	results := make(chan error, 2)
	go func() { results <- apiServer.ListenAndServe() }()
	go func() { results <- opsServer.ListenAndServe() }()
	select {
	case <-ctx.Done():
	case err := <-results:
		if err != nil && err != http.ErrServerClosed {
			log.Print("listener failed")
		}
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = apiServer.Shutdown(shutdownCtx)
	_ = opsServer.Shutdown(shutdownCtx)
}
