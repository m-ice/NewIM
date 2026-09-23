package api

import (
	"context"
	"errors"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultHeaderTimeout   = 5 * time.Second
	defaultReadTimeout     = 10 * time.Second
	defaultWriteTimeout    = 10 * time.Second
	defaultIdleTimeout     = 60 * time.Second
	defaultMaxHeaderBytes  = 16 * 1024
	defaultShutdownTimeout = 10 * time.Second
	defaultStartupTimeout  = 5 * time.Second
)

var (
	// ErrAlreadyStarted indicates a Server was run more than once.
	// ErrAlreadyStarted 表示 Server 被重复运行。
	ErrAlreadyStarted = fail(CodeAlreadyStarted)
)

type serverResult struct {
	err error
}

// Server owns two HTTP listeners and their bounded lifecycle.
// Server 管理两个 HTTP listener 及其有界生命周期。
type Server struct {
	cfg     Config
	logger  *slog.Logger
	metrics *metrics

	started atomic.Bool
	ready   atomic.Bool

	shutdownOnce sync.Once
	shutdownErr  error

	listenerMu sync.RWMutex

	apiListener net.Listener
	opsListener net.Listener
	apiServer   *http.Server
	opsServer   *http.Server
	apiHandler  http.Handler
	opsHandler  http.Handler
}

// New constructs a single-use HTTP server from a validated configuration.
// New 根据已校验配置创建一个只运行一次的 HTTP 服务。
func New(cfg Config, logger *slog.Logger) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	s := &Server{
		cfg:     cfg,
		logger:  logger,
		metrics: newMetrics(cfg.ServerVersion),
	}
	s.apiHandler = s.makeHandler("api")
	s.opsHandler = s.makeHandler("ops")
	s.apiServer = newHTTPServer(s.apiHandler)
	s.opsServer = newHTTPServer(s.opsHandler)
	return s, nil
}

func newHTTPServer(handler http.Handler) *http.Server {
	return &http.Server{
		Handler:                      handler,
		DisableGeneralOptionsHandler: true,
		ErrorLog:                     log.New(io.Discard, "", 0),
		ReadHeaderTimeout:            defaultHeaderTimeout,
		ReadTimeout:                  defaultReadTimeout,
		WriteTimeout:                 defaultWriteTimeout,
		IdleTimeout:                  defaultIdleTimeout,
		MaxHeaderBytes:               defaultMaxHeaderBytes,
	}
}

// Run binds both listeners, serves until cancellation/failure, then drains.
// Run 绑定两个 listener，在取消或故障后服务并有界排空。
func (s *Server) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if !s.started.CompareAndSwap(false, true) {
		return ErrAlreadyStarted
	}
	if err := s.bind(); err != nil {
		return err
	}

	results := make(chan serverResult, 2)
	started := make(chan struct{}, 2)
	go serve(s.apiServer, s.apiListener, started, results)
	go serve(s.opsServer, s.opsListener, started, results)

	var runErr error
	remaining := 2
	startedCount := 0
	startupTimer := time.NewTimer(defaultStartupTimeout)
	for startedCount < 2 && runErr == nil && ctx.Err() == nil {
		select {
		case <-started:
			startedCount++
		case result := <-results:
			remaining--
			runErr = result.err
			if runErr == nil || errors.Is(runErr, http.ErrServerClosed) {
				runErr = fail(CodeRuntimeFailed)
			}
		case <-ctx.Done():
		case <-startupTimer.C:
			runErr = fail(CodeRuntimeFailed)
		}
	}
	if !startupTimer.Stop() {
		select {
		case <-startupTimer.C:
		default:
		}
	}
	if runErr == nil && startedCount == 2 && ctx.Err() == nil {
		s.ready.Store(true)
		select {
		case <-ctx.Done():
		case result := <-results:
			remaining--
			runErr = result.err
			if runErr == nil || errors.Is(runErr, http.ErrServerClosed) {
				runErr = fail(CodeRuntimeFailed)
			}
		}
	}

	s.ready.Store(false)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
	if err := s.shutdown(shutdownCtx); err != nil && runErr == nil {
		runErr = err
	}
	cancel()

	deadline := time.NewTimer(defaultShutdownTimeout)
	defer deadline.Stop()
	for remaining > 0 {
		select {
		case result := <-results:
			remaining--
			if runErr == nil && result.err != nil && !errors.Is(result.err, http.ErrServerClosed) {
				runErr = result.err
			}
		case <-deadline.C:
			return fail(CodeShutdownFailed)
		}
	}
	return runErr
}

func (s *Server) bind() error {
	apiListener, err := net.Listen("tcp", s.cfg.APIAddr)
	if err != nil {
		return fail(CodeBindFailed)
	}
	opsListener, err := net.Listen("tcp", s.cfg.OpsAddr)
	if err != nil {
		_ = apiListener.Close()
		return fail(CodeBindFailed)
	}
	s.listenerMu.Lock()
	s.apiListener = apiListener
	s.opsListener = opsListener
	s.listenerMu.Unlock()
	return nil
}

func serve(server *http.Server, listener net.Listener, started chan<- struct{}, results chan<- serverResult) {
	results <- serverResult{err: server.Serve(&startListener{Listener: listener, started: started})}
}

type startListener struct {
	net.Listener
	once    sync.Once
	started chan<- struct{}
}

func (l *startListener) Accept() (net.Conn, error) {
	l.once.Do(func() { l.started <- struct{}{} })
	return l.Listener.Accept()
}

// Shutdown makes readiness false before draining both listeners.
// Shutdown 先将就绪状态置为 false，再排空两个 listener。
func (s *Server) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.shutdownOnce.Do(func() {
		s.shutdownErr = s.doShutdown(ctx)
	})
	return s.shutdownErr
}

func (s *Server) shutdown(ctx context.Context) error {
	return s.Shutdown(ctx)
}

func (s *Server) doShutdown(ctx context.Context) error {
	s.ready.Store(false)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, server := range []*http.Server{s.apiServer, s.opsServer} {
		if server == nil {
			continue
		}
		wg.Add(1)
		go func(server *http.Server) {
			defer wg.Done()
			errs <- server.Shutdown(ctx)
		}(server)
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		s.closeServers()
		<-done
		return fail(CodeShutdownFailed)
	}
	close(errs)
	var shutdownErr error
	for err := range errs {
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			shutdownErr = err
		}
	}
	if shutdownErr != nil {
		s.closeServers()
		return fail(CodeShutdownFailed)
	}
	return nil
}

func (s *Server) closeServers() {
	if s.apiServer != nil {
		_ = s.apiServer.Close()
	}
	if s.opsServer != nil {
		_ = s.opsServer.Close()
	}
}

// Ready reports whether both listeners have bound and serving has started.
// Ready 报告两个 listener 是否已绑定并开始服务。
func (s *Server) Ready() bool { return s.ready.Load() }

// APIAddr returns the actual API listener address after binding.
// APIAddr 在绑定后返回实际 API listener 地址。
func (s *Server) APIAddr() string {
	s.listenerMu.RLock()
	defer s.listenerMu.RUnlock()
	if s.apiListener == nil {
		return ""
	}
	return s.apiListener.Addr().String()
}

// OpsAddr returns the actual Ops listener address after binding.
// OpsAddr 在绑定后返回实际 Ops listener 地址。
func (s *Server) OpsAddr() string {
	s.listenerMu.RLock()
	defer s.listenerMu.RUnlock()
	if s.opsListener == nil {
		return ""
	}
	return s.opsListener.Addr().String()
}

func (s *Server) makeHandler(listener string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		route, ok := resolveRoute(listener, r.URL.EscapedPath())
		method := normalizeMethod(r.Method)
		started := time.Now()
		recorder := &responseRecorder{ResponseWriter: w}
		recorder.Header().Set("Cache-Control", "no-store")
		recorder.Header().Set("X-Content-Type-Options", "nosniff")

		if !ok {
			s.metrics.begin(listener, "unmatched", method)
			defer func() { s.finish(recorder, listener, "unmatched", method, started) }()
			writeError(recorder, http.StatusNotFound, HTTPRouteNotFound)
			return
		}
		if r.Method != http.MethodGet {
			s.metrics.begin(listener, route, method)
			defer func() { s.finish(recorder, listener, route, method, started) }()
			recorder.Header().Set("Allow", http.MethodGet)
			writeError(recorder, http.StatusMethodNotAllowed, HTTPMethodNotAllowed)
			return
		}
		if requestHasBody(r) {
			s.metrics.begin(listener, route, method)
			defer func() { s.finish(recorder, listener, route, method, started) }()
			writeError(recorder, http.StatusBadRequest, HTTPBodyNotAllowed)
			return
		}

		s.metrics.begin(listener, route, method)
		defer func() { s.finish(recorder, listener, route, method, started) }()
		defer s.recoverRequest(listener, route, method, recorder)

		switch route {
		case "health":
			writeJSON(recorder, http.StatusOK, `{"status":"ok"}`+"\n")
		case "ready":
			if !s.ready.Load() {
				writeError(recorder, http.StatusServiceUnavailable, ServerNotReady)
				return
			}
			writeJSON(recorder, http.StatusOK, `{"status":"ready"}`+"\n")
		case "metrics":
			recorder.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
			recorder.WriteHeader(http.StatusOK)
			_ = s.metrics.writeTo(recorder)
		}
	})
}

func (s *Server) recoverRequest(listener, route, method string, recorder *responseRecorder) {
	if recovered := recover(); recovered != nil {
		if !recorder.wroteHeader {
			writeError(recorder, http.StatusInternalServerError, HTTPInternalError)
		}
		s.logRequest(listener, route, method, http.StatusInternalServerError)
	}
}

func (s *Server) finish(recorder *responseRecorder, listener, route, method string, started time.Time) {
	status := recorder.statusCode()
	s.metrics.end(listener, route, method, status, time.Since(started))
	s.logRequest(listener, route, method, status)
}

func (s *Server) logRequest(listener, route, method string, status int) {
	s.logger.Info("http_request",
		slog.String("listener", listener),
		slog.String("route", route),
		slog.String("method", method),
		slog.Int("status", status),
	)
}

func resolveRoute(listener, path string) (string, bool) {
	switch listener {
	case "api":
		if path == "/api/v1/health" {
			return "health", true
		}
	case "ops":
		switch path {
		case "/ready":
			return "ready", true
		case "/metrics":
			return "metrics", true
		}
	}
	return "", false
}

func normalizeMethod(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodOptions:
		return method
	default:
		return "OTHER"
	}
}

func requestHasBody(r *http.Request) bool {
	return r.ContentLength > 0 || r.ContentLength < 0 || len(r.TransferEncoding) != 0
}

type responseRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *responseRecorder) WriteHeader(status int) {
	if r.wroteHeader {
		return
	}
	r.status = status
	r.wroteHeader = true
	r.ResponseWriter.WriteHeader(status)
}

func (r *responseRecorder) Write(data []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	return r.ResponseWriter.Write(data)
}

func (r *responseRecorder) statusCode() int {
	if r.status == 0 {
		return http.StatusOK
	}
	return r.status
}

func writeJSON(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}

func writeError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, `{"error":{"code":"`+code+`"}}`+"\n")
}
